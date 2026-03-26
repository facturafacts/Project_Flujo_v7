"""
Merge all sources into ledger_final — v7 per-account version.

Combines:
  - source_api_payments_{A|B}   → ledger entries
  - source_release_reports_{A|B} → ledger entries

Sign handling:
  Sale / Deposit / Account Funding  → positive (+)
  Purchase / Withdrawal             → negative (-)

Usage:
    python scripts/ledger/merge.py          # incremental (new only)
    python scripts/ledger/merge.py --full   # rebuild ledger from scratch
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import (
    get_connection, init_db, upsert_ledger,
    get_all_api_rows, get_all_release_rows,
)


# ── Sign helpers ───────────────────────────────────────────────

def api_sign(row):
    """
    Returns (gross_amount, fee, net_amount, category) for an API payment row.
    """
    op   = row.get("operation_type") or ""
    coll = row.get("collector_id")            # NULL = you paid (purchase)
    gross = float(row.get("transaction_amount") or 0)
    fee   = float(row.get("fee_amount") or 0)
    net   = float(row.get("net_received_amount") or 0)

    if op == "pos_payment" or op == "regular_payment":
        if coll:
            return  gross,  fee,  net,  "POS Sale"
        else:
            return -gross,  fee, -net,  "Purchase/Expense"
    elif op == "account_fund":
        return gross, fee, net, "Account Funding (Deposit)"
    elif op == "money_transfer":
        if net >= 0:
            return  gross, fee,  net,  "Money Transfer"
        else:
            return  gross, fee,  net,  "Money Transfer Out"
    return gross, fee, net, op or "Unknown"


def release_sign(row):
    """
    Returns (gross_amount, category) for a release report row.
    Positive gross = money in, negative = money out.
    """
    gross = float(row.get("gross_amount") or 0)
    if gross > 0:
        cat = "Bank Deposit"
    elif gross < 0:
        cat = "Bank Withdrawal"
    else:
        cat = "Zero-Movement"
    return gross, cat


# ── Core merge ─────────────────────────────────────────────────

def merge_api(account, full=False):
    conn = get_connection()
    cur  = conn.cursor()

    if full:
        cur.execute(
            "DELETE FROM ledger_final WHERE source_account = ? AND source = 'api'",
            (account.upper(),)
        )
        print(f"  🗑  Cleared existing api rows for Account {account}.")
        conn.commit()

    # Build set of internal_ids already in ledger for this account+source (for dedup)
    cur.execute("""
        SELECT internal_id FROM ledger_final
        WHERE source_account = ? AND source = 'api'
    """, (account.upper(),))
    already = {r[0] for r in cur.fetchall()}
    conn.close()

    rows = get_all_api_rows(account)
    new_rows = [r for r in rows if r[0] not in already]

    for r in new_rows:
        (iid, date_c, op, desc, gross, fee, net, coll) = r
        sign_gross, sign_fee, sign_net, cat = api_sign({
            "operation_type":       op,
            "collector_id":         coll,
            "transaction_amount":   gross,
            "fee_amount":           fee,
            "net_received_amount":  net,
            "description":         desc,
        })
        upsert_ledger((
            iid, account.upper(), date_c, cat, None, None,
            desc, sign_gross, sign_fee, sign_net, "api", 0
        ))

    print(f"  ✅  Merged {len(new_rows)}/{len(rows)} API rows for Account {account}.")
    return len(new_rows)


def merge_release(account, full=False):
    conn = get_connection()
    cur  = conn.cursor()

    if full:
        cur.execute(
            "DELETE FROM ledger_final WHERE source_account = ? AND source = 'release'",
            (account.upper(),)
        )
        print(f"  🗑  Cleared existing release rows for Account {account}.")
        conn.commit()

    # Dedup against already-merged release rows
    cur.execute("""
        SELECT internal_id FROM ledger_final
        WHERE source_account = ? AND source = 'release'
    """, (account.upper(),))
    already = {r[0] for r in cur.fetchall()}
    conn.close()

    rows = get_all_release_rows(account)
    new_rows = [r for r in rows if r[0] not in already]

    for r in new_rows:
        sid, date, desc, gross, credit, debit = r
        sign_gross, cat = release_sign({"gross_amount": gross, "description": desc})
        upsert_ledger((
            sid, account.upper(), date, cat, None, None,
            desc, sign_gross, 0.0, sign_gross, "release", 0
        ))

    print(f"  ✅  Merged {len(new_rows)}/{len(rows)} release rows for Account {account}.")
    return len(new_rows)


def run_merge(full=False):
    init_db()
    print(f"\n🔀  Merge {'(FULL rebuild)' if full else '(incremental)'}\n")

    total = 0
    for acc in ("A", "B"):
        total += merge_api(acc, full)
        total += merge_release(acc, full)

    print(f"\n✅  Merge complete — {total} new rows inserted.\n")


if __name__ == "__main__":
    run_merge(full="--full" in sys.argv)
