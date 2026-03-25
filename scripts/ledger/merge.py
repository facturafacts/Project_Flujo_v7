"""
Merge all sources into ledger_final — v7 dual-account version.

Combines:
  - source_api_payments   → ledger entries (tagged by source_account)
  - source_release_reports → ledger entries (tagged by source_account)

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
    insert_api_payments_batch, insert_release_reports_batch,
)

import csv, io
from datetime import datetime

# ── Sign helpers ────────────────────────────────────────────────

def api_sign(row):
    """
    Returns (gross_amount, fee, net_amount, category) for an API payment.
    """
    op    = row.get("operation_type") or ""
    coll  = row.get("collector_id")          # NULL = you paid (purchase)
    gross = float(row.get("transaction_amount") or 0)
    fee   = float(row.get("fee_amount") or 0)
    net   = float(row.get("net_received_amount") or 0)
    desc  = row.get("description") or ""

    # Sale (you received money)
    if op == "pos_payment" or op == "regular_payment":
        if coll:          # collector_id set = someone paid you
            return gross, fee, net, "POS Sale"
        else:             # collector_id NULL = you paid someone
            return -gross, fee, -net, "Purchase/Expense"
    elif op == "account_fund":
        return gross, fee, net, "Account Funding (Deposit)"
    elif op == "money_transfer":
        # Inbound transfer vs outbound — net positive means money came in
        if net >= 0:
            return gross, fee, net, "Money Transfer"
        else:
            return gross, fee, net, "Money Transfer Out"
    return gross, fee, net, op or "Unknown"


def release_sign(row):
    """
    Returns (gross_amount, category) for a release report row.
    Positive gross = money in, negative = money out.
    """
    gross = float(row.get("gross_amount") or 0)
    desc  = row.get("description") or ""

    if gross > 0:
        cat = "Bank Deposit"
    elif gross < 0:
        cat = "Bank Withdrawal"
    else:
        cat = "Zero-Movement"
    return gross, cat


# ── Core merge ──────────────────────────────────────────────────

def merge_api(account, full=False):
    conn = get_connection()
    cur  = conn.cursor()

    if full:
        cur.execute("DELETE FROM ledger_final WHERE source_account = ? AND source = 'api'", (account,))
        print(f"  🗑  Cleared existing api rows for Account {account}.")
    conn.commit()

    cur.execute("""
        SELECT internal_id, date_created, operation_type, description,
               transaction_amount, fee_amount, net_received_amount,
               collector_id, source_account
        FROM source_api_payments
        WHERE source_account = ?
        AND internal_id NOT IN (
            SELECT internal_id FROM ledger_final
            WHERE source_account = ? AND source = 'api'
        )
    """, (account, account))
    # Note: NOT IN subquery above won't work with empty ledger — handle separately

    rows = cur.fetchall()
    conn.close()

    for r in rows:
        (iid, date_c, op, desc, gross, fee, net, coll, sa) = r
        sign_gross, sign_fee, sign_net, cat = api_sign({
            "operation_type": op,
            "collector_id":   coll,
            "transaction_amount": gross,
            "fee_amount":     fee,
            "net_received_amount": net,
            "description":    desc,
        })
        upsert_ledger((
            iid, sa, date_c, cat, None, None,
            desc, sign_gross, sign_fee, sign_net,
            "api", 0
        ))

    print(f"  ✅  Merged {len(rows)} API rows for Account {account}.")
    return len(rows)


def merge_release(account, full=False):
    conn = get_connection()
    cur  = conn.cursor()

    if full:
        cur.execute("DELETE FROM ledger_final WHERE source_account = ? AND source = 'release'", (account,))
        print(f"  🗑  Cleared existing release rows for Account {account}.")
    conn.commit()

    cur.execute("""
        SELECT source_id, date, description, gross_amount,
               net_credit_amount, net_debit_amount, source_account
        FROM source_release_reports
        WHERE source_account = ?
    """, (account,))
    rows = cur.fetchall()
    conn.close()

    for r in rows:
        sid, date, desc, gross, credit, debit, sa = r
        sign_gross, cat = release_sign({"gross_amount": gross, "description": desc})
        fee = 0.0
        upsert_ledger((
            sid, sa, date, cat, None, None,
            desc, sign_gross, fee, sign_gross,
            "release", 0
        ))

    print(f"  ✅  Merged {len(rows)} release rows for Account {account}.")
    return len(rows)


def run_merge(full=False):
    init_db()
    print(f"\n🔀  Merge {'(FULL rebuild)' if full else '(incremental)'}\n")

    total = 0
    for acc in ("A", "B"):
        total += merge_api(acc, full)
        total += merge_release(acc, full)

    print(f"\n✅  Merge complete — {total} rows processed.\n")


if __name__ == "__main__":
    run_merge(full="--full" in sys.argv)
