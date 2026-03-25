"""
Quick Summary — prints ledger stats to console.
Usage: python scripts/reports/summary.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import get_connection, get_ledger_stats

def run():
    conn = __import__("data.db_manager", fromlist=["get_connection"]).get_connection()
    cur  = conn.cursor()

    cur.execute("""
        SELECT source_account,
               COUNT(*) as total,
               SUM(CASE WHEN classification IS NULL THEN 1 ELSE 0 END) as unlabeled,
               SUM(CASE WHEN classification = 'Work' THEN 1 ELSE 0 END) as work,
               SUM(CASE WHEN classification = 'Personal' THEN 1 ELSE 0 END) as personal,
               SUM(CASE WHEN gross_amount > 0 THEN gross_amount ELSE 0 END) as inflow,
               SUM(CASE WHEN gross_amount < 0 THEN gross_amount ELSE 0 END) as outflow
        FROM ledger_final
        GROUP BY source_account
    """)
    rows = cur.fetchall()
    conn.close()

    print("\n📊  Ledger Summary — Project_Flujo_v7\n")
    print(f"{'Acct':<6} {'Total':>8} {'Unlabeled':>10} {'Work':>8} {'Personal':>10} {'Inflow':>14} {'Outflow':>14}")
    print("-" * 75)
    for r in rows:
        sa, total, unl, w, p, infl, outfl = r
        label = "A (Expense)" if sa == "A" else "B (Concentrator)"
        print(f"{label:<18} {total:>8} {unl:>10} {w:>8} {p:>10} {infl:>14.2f} {outfl:>14.2f}")

    print()

if __name__ == "__main__":
    run()
