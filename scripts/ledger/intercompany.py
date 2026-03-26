"""
Intercompany Transfer Detection — v7

Rule:
  When Account B sends money to Account A:
  1. Account B API shows an outbound money_transfer (negative net)
  2. Account A release report shows a matching deposit on the SAME DAY
  3. Match on: |gross_amount_B| ≈ net_amount_A  (within $1.00 tolerance)
  4. If a transfer appears in BOTH B API and A release → use API data (more detail)
     and mark the release report row as deduped.

Output:
  - source_release_reports.intercompany = 1
  - source_release_reports.counterpart_account = 'B'
  - ledger_final.intercompany = 1  (on both sides)

Usage:
    python scripts/ledger/intercompany.py

The rule will be validated with real data before being applied.
This script DRY-RUN by default — pass --apply to actually write to DB.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import (
    get_connection, init_db,
    mark_intercompany_release, mark_intercompany_ledger,
)

from datetime import datetime, timedelta

TOLERANCE = 1.00   # $1.00 matching tolerance for net amount
DRY_RUN   = "--apply" not in sys.argv


def normalize_date(dt_str):
    """Return just the date portion (YYYY-MM-DD) or None."""
    if not dt_str:
        return None
    return dt_str[:10]


def run_detection():
    init_db()
    conn = get_connection()
    cur  = conn.cursor()

    print("\n" + "=" * 60)
    print("🔍  Intercompany Detection" + (" (DRY RUN)" if DRY_RUN else " (APPLYING)"))
    print("=" * 60)

    # ── 1. Pull all B→A money transfers from Account B API ─────
    cur.execute("""
        SELECT internal_id, date_created, description,
               transaction_amount, net_received_amount, fee_amount,
               collector_id
        FROM source_api_payments_B
        WHERE operation_type = 'money_transfer'
          AND (net_received_amount < 0 OR transaction_amount < 0)
    """)
    b_outbound = cur.fetchall()
    print(f"\n📤  Account B outbound transfers (B→?): {len(b_outbound)} candidates")

    # ── 2. Pull all positive release report entries from Account A
    cur.execute("""
        SELECT source_id, date, description, gross_amount,
               net_credit_amount, intercompany
        FROM source_release_reports_A
        WHERE gross_amount > 0
    """)
    a_inbound = cur.fetchall()
    print(f"📥  Account A release deposits (A←?): {len(a_inbound)} candidates\n")

    # ── 3. Match: B outbound ↔ A release by date + amount ──────
    matched_b    = []   # (b_id, b_date, a_source_id) — matched pairs
    matched_a    = set() # a_source_ids already matched
    unmatched_b  = []

    a_by_date = {}
    for row in a_inbound:
        d = normalize_date(row[1])
        a_by_date.setdefault(d, []).append(row)

    for b in b_outbound:
        b_id, b_date_raw, b_desc, b_gross, b_net, b_fee, b_coll = b
        b_date = normalize_date(b_date_raw)
        b_abs  = abs(b_net) if b_net else abs(b_gross)

        candidates = a_by_date.get(b_date, [])
        best_match = None
        best_diff  = float("inf")

        for a in candidates:
            a_sid, a_date, a_desc, a_gross, a_net, a_flag = a
            if a_sid in matched_a:
                continue
            diff = abs(a_net - b_abs) if a_net else abs(a_gross - b_abs)
            if diff <= TOLERANCE and diff < best_diff:
                best_diff  = diff
                best_match = (a_sid, a_gross, a_net)

        if best_match:
            a_sid, a_gross, a_net = best_match
            matched_b.append((b_id, b_date, a_sid, b_gross, b_net, b_fee, b_desc))
            matched_a.add(a_sid)
        else:
            unmatched_b.append((b_id, b_date, b_gross, b_net, b_desc))

    # ── 4. Report ───────────────────────────────────────────────
    print(f"✅  Matches found: {len(matched_b)}")
    print(f"⚠️   Unmatched (B→? transfers): {len(unmatched_b)}\n")

    if matched_b:
        print(f"{'B API ID':<30} {'B Date':<12} {'A Release ID':<30} {'B Net':>12} {'A Net':>12} {'Diff':>8}")
        print("-" * 100)
        for b_id, b_date, a_sid, b_gross, b_net, b_fee, b_desc in matched_b:
            b_abs = abs(b_net)
            a_row = next(r for r in a_inbound if r[0] == a_sid)
            a_net = a_row[4] or a_row[3]
            diff  = abs(a_net - b_abs)
            print(f"{b_id:<30} {b_date:<12} {a_sid:<30} {b_net:>12.2f} {a_net:>12.2f} {diff:>8.2f}")

    if unmatched_b:
        print(f"\n{'='*60}")
        print(f"Unmatched B→? transfers (manual review):")
        print(f"{'B API ID':<30} {'Date':<12} {'Net':>12}  Description")
        print("-" * 80)
        for b_id, b_date, b_gross, b_net, b_desc in unmatched_b:
            print(f"{b_id:<30} {b_date:<12} {b_net:>12.2f}  {b_desc[:40]}")

    # ── 5. Apply ────────────────────────────────────────────────
    if not DRY_RUN:
        print("\n🔨  Applying changes...")
        for b_id, b_date, a_sid, b_gross, b_net, b_fee, b_desc in matched_b:
            mark_intercompany_release(a_sid, "A", "B")   # mark A release row
            mark_intercompany_ledger(b_id, "B")         # mark B ledger entry
            mark_intercompany_ledger(a_sid, "A")        # mark A ledger entry
            print(f"   ✅ {a_sid} ← marked intercompany (B counterpart: {b_id})")
        print("\n✅  Intercompany detection applied.")
    else:
        print("\n💡  Pass --apply to write these changes to the database.")

    conn.close()
    print()


if __name__ == "__main__":
    run_detection()
