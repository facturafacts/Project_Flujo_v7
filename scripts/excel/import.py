"""
Import Labels from Excel — v7

Reads a v7_labeling_*.xlsx file and writes Classification + Subcategory
back to the ledger_final table.

Only updates classification and subcategory — never deletes or overwrites
any other field.

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
        # Find most recent export
        out_dir = Path(__file__).parent.parent.parent / "output"
        files   = sorted(out_dir.glob("v7_labeling_*.xlsx"))
        if not files:
            print("❌  No v7_labeling_*.xlsx found in output/. Run export.py first.")
            sys.exit(1)
        xlsx_path = files[-1]

    xlsx_path = Path(xlsx_path)
    print(f"\n📥  Importing from: {xlsx_path.name}\n")

    wb = openpyxl.load_workbook(xlsx_path)
    updated = 0
    skipped = 0
    errors  = 0

    for ws in wb.worksheets:
        # Only process the labeled sheets (rows start at row 4 in To-Label sheets)
        if ws.title not in ("🔵 Account A — To Label", "🟢 Account B — To Label"):
            continue

        for row in ws.iter_rows(min_row=4, values_only=False):
            iid_cell   = row[0]   # internal_id (col A)
            cls_cell   = row[3]   # Classification (col D)
            subc_cell  = row[4]   # Subcategory  (col E)
            interco    = row[8].value if len(row) > 8 else ""  # col I

            iid  = iid_cell.value
            cls  = cls_cell.value
            subc = subc_cell.value

            # Skip intercompany (no labeling needed)
            if interco and "⚠️" in str(interco):
                skipped += 1
                continue

            if not iid:
                continue

            # Only update if classification was actually set
            if cls and str(cls).strip():
                cls  = str(cls).strip()
                subc = str(subc).strip() if subc else ""
                # Derive source_account from sheet name
                sa   = "A" if "Account A" in ws.title else "B"
                update_classification(iid, sa, cls, subc)
                print(f"  ✅  {iid}  [{sa}]  {cls} / {subc}")
                updated += 1
            else:
                skipped += 1

    print(f"\n✅  Import complete.")
    print(f"    Updated:  {updated}")
    print(f"    Skipped:  {skipped}")
    if errors:
        print(f"    Errors:   {errors}")
    print()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    run_import(path)
