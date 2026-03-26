"""
Migration: flat source tables → per-account source tables (v7)

From:
  source_api_payments       (has source_account column)
  source_release_reports    (has source_account column)

To:
  source_api_payments_A
  source_api_payments_B
  source_release_reports_A
  source_release_reports_B

ledger_final stays as-is (already has source_account).
This script is ONE-TIME only. Run it, verify, then delete.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.db_manager import get_connection

print("=" * 60)
print("📦  Migration: flat → per-account tables")
print("=" * 60)

conn = get_connection()
cur  = conn.cursor()

# ── Count before ────────────────────────────────────────────────
cur.execute("SELECT COUNT(*), source_account FROM source_api_payments GROUP BY source_account")
api_counts = dict(cur.fetchall())
cur.execute("SELECT COUNT(*), source_account FROM source_release_reports GROUP BY source_account")
rel_counts = dict(cur.fetchall())
print(f"\n  Before:")
print(f"    source_api_payments:      {api_counts}")
print(f"    source_release_reports:   {rel_counts}")
print(f"    ledger_final:             {cur.execute('SELECT COUNT(*) FROM ledger_final').fetchone()[0]} rows")

# ── Create new per-account tables ─────────────────────────────
print("\n  🏗️  Creating per-account tables...")

cur.execute("DROP TABLE IF EXISTS source_api_payments_A")
cur.execute("DROP TABLE IF EXISTS source_api_payments_B")
cur.execute("DROP TABLE IF EXISTS source_release_reports_A")
cur.execute("DROP TABLE IF EXISTS source_release_reports_B")

cur.execute("""
    CREATE TABLE source_api_payments_A AS
    SELECT * FROM source_api_payments WHERE source_account = 'A'
""")
cur.execute("""
    CREATE TABLE source_api_payments_B AS
    SELECT * FROM source_api_payments WHERE source_account = 'B'
""")
cur.execute("""
    CREATE TABLE source_release_reports_A AS
    SELECT * FROM source_release_reports WHERE source_account = 'A'
""")
cur.execute("""
    CREATE TABLE source_release_reports_B AS
    SELECT * FROM source_release_reports WHERE source_account = 'B'
""")

# ── Verify row counts ──────────────────────────────────────────
print("  ✅ Rows distributed:")
for tbl, col in [("source_api_payments_A", None), ("source_api_payments_B", None),
                  ("source_release_reports_A", None), ("source_release_reports_B", None)]:
    n = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"       {tbl}: {n} rows")

# ── Drop old flat tables ───────────────────────────────────────
print("\n  🗑️  Dropping old flat tables...")
cur.execute("DROP TABLE source_api_payments")
cur.execute("DROP TABLE source_release_reports")

conn.commit()

# ── Final verify ───────────────────────────────────────────────
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print(f"\n  Final tables: {tables}")
conn.close()

print("\n✅  Migration complete. Old flat tables are gone.")
print("   Run `python scripts/ledger/merge.py --full` to rebuild ledger_final.")
