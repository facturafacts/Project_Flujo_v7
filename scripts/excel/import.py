"""
Import Labels from Excel — v7

Reads Account A and Account B sheets from a v7_labeling_*.xlsx,
writes Classification + Subcategory back to ledger_final.

Only updates classification and subcategory — never deletes.

Usage:
    python scripts/excel/import.py
    python scripts/excel/import.py output/v7_labeling_20260325_0800.xlsx
"""
import sys, os
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
            print("❌  No v7_labeling_*.xlsx found. Run export.py first.")
            sys.exit(1)
        xlsx_path = files[-1]

    xlsx_path = Path(xlsx_path)
    print(f"\n📥  Importing from: {xlsx_path.name}\n")

    wb = openpyxl.load_workbook(xlsx_path)

    updated = 0; skipped = 0

    # Sheets to import from (Account A and Account B)
    SHEETS = {
        "🔵 Account A": "A",
        "🟢 Account B": "B",
    }

    for sheet_name, sa in SHEETS.items():
        ws = wb.get_sheet_by_name(sheet_name)
        if not ws:
            print(f"  ⚠️  Sheet '{sheet_name}' not found — skipping.")
            continue

        print(f"  Processing: {sheet_name}")

        for row in ws.iter_rows(min_row=4, values_only=False):
            # Col A (0): internal_id | Col C (2): Category | Col D (3): Classification | Col E (4): Subcategory | Col I (8): Interco
            iid_cell  = row[0]
            cls_cell  = row[3]   # col D = Classification
            subc_cell = row[4]   # col E = Subcategory
            interco   = row[8].value if len(row) > 8 else ""

            iid = iid_cell.value
            cls = cls_cell.value
            subc = subc_cell.value

            # Skip intercompany rows
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

    print(f"\n✅  Import complete.")
    print(f"    Updated:  {updated}")
    print(f"    Skipped:  {skipped}  (blank labels or intercompany)")
    print(f"\n    💡 Press F9 in Excel or save to refresh P&L formulas.\n")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    run_import(path)
