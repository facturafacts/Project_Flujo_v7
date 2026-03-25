"""
Import Labels from Excel — v7

Reads a v7_labeling_*.xlsx and writes Classification + Subcategory
back to the ledger_final table AND updates "To Label (All)" in-place
so that P&L SUMIFS formulas pick up the new labels immediately.

Only updates classification and subcategory — never deletes.

Usage:
    python scripts/excel/import.py
    python scripts/excel/import.py output/v7_labeling_20260325_0800.xlsx
"""
import sys, os, glob
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import update_classification, init_db
import openpyxl


def run_import(xlsx_path=None):
    init_db()

    if xlsx_path is None:
        out_dir = Path(__file__).parent.parent.parent / "output"
        files   = sorted(out_dir.glob("v7_labeling_*.xlsx"))
        if not files:
            print("❌  No v7_labeling_*.xlsx found in output/. Run export.py first.")
            sys.exit(1)
        xlsx_path = files[-1]

    xlsx_path = Path(xlsx_path)
    print(f"\n📥  Importing from: {xlsx_path.name}\n")

    wb = openpyxl.load_workbook(xlsx_path)

    # ── 1. Update ledger_final from Account A / B sheets ──────────
    updated = 0; skipped = 0; errors = 0

    for ws in wb.worksheets:
        if ws.title not in ("🔵 Account A — To Label", "🟢 Account B — To Label"):
            continue

        sa = "A" if "Account A" in ws.title else "B"
        print(f"  Processing: {ws.title}")

        for row in ws.iter_rows(min_row=4, values_only=False):
            iid_cell  = row[0]   # col A: internal_id
            cls_cell  = row[3]   # col D: Classification
            subc_cell = row[4]   # col E: Subcategory
            interco   = row[8].value if len(row) > 8 else ""

            iid  = iid_cell.value
            cls  = cls_cell.value
            subc = subc_cell.value

            # Intercompany = no labeling needed
            if interco and "⚠️" in str(interco):
                skipped += 1
                continue
            if not iid:
                skipped += 1
                continue

            if cls and str(cls).strip():
                cls  = str(cls).strip()
                subc = str(subc).strip() if subc else ""
                update_classification(iid, sa, cls, subc)
                print(f"    ✅  {iid}  [{sa}]  {cls} / {subc}")
                updated += 1
            else:
                skipped += 1

    # ── 2. Update "To Label (All)" in-place ────────────────────
    ws_all = wb.get_sheet_by_name("To Label (All)")
    if ws_all:
        print(f"\n  Updating 'To Label (All)' in-place...")
        updated_all = 0
        for row in ws_all.iter_rows(min_row=3, values_only=False):
            iid_cell = row[0]   # col A: internal_id
            sa_cell  = row[1]   # col B: source_account
            cls_cell = row[4]   # col E: Classification
            subc_cell = row[5]  # col F: Subcategory

            iid = iid_cell.value
            if not iid:
                continue

            sa = str(sa_cell.value).strip() if sa_cell.value else "A"

            # Find corresponding data in labeled sheets
            # Since we just updated the DB, re-fetch from ledger_final via a
            # quick lookup approach — query the DB directly for this iid+sa
            conn = __import__("data.db_manager", fromlist=["get_connection"]).get_connection()
            cur  = conn.cursor()
            cur.execute("""
                SELECT classification, subcategory
                FROM ledger_final
                WHERE internal_id = ? AND source_account = ?
            """, (iid, sa))
            row_db = cur.fetchone()
            conn.close()

            if row_db and row_db[0]:
                cls, subc = row_db
                cls_cell.value  = cls
                subc_cell.value = subc or ""
                updated_all += 1

        print(f"    ✅  Updated {updated_all} rows in 'To Label (All)'.")
    else:
        print(f"\n  ⚠️  'To Label (All)' sheet not found — P&L will refresh on next export.")

    # ── Save in-place ───────────────────────────────────────────
    wb.save(xlsx_path)

    print(f"\n✅  Import complete.")
    print(f"    Ledger updated:  {updated}")
    print(f"    Skipped:         {skipped}")
    print(f"    'To Label (All)' updated: {updated_all}")
    print(f"    File saved:      {xlsx_path.name}")
    print(f"\n    💡 Open Excel → press F9 or save to refresh P&L formulas.\n")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    run_import(path)
