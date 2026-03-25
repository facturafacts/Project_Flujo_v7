"""
Excel Export — v7 Multi-Account Labeling UX

Creates output/v7_labeling_YYYYMMDD_HHMM.xlsx with 6 sheets:
  1. Summary     — stats by account
  2. Account A  — unlabeled from A
  3. Account B  — unlabeled from B  (intercompany highlighted)
  4. All Labeled — review / un-label
  5. Catalog    — chart of accounts (editable)
  6. P&L        — live Work vs Personal summary

Usage:
    python scripts/excel/export.py
"""
import sys, os, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import (
    get_connection, get_unlabeled_for_export, get_labeled_for_review,
    get_pnl_data, get_ledger_stats, init_db,
)
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Styles ──────────────────────────────────────────────────────

YELLOW  = PatternFill("solid", fgColor="FFF3CD")   # intercompany
BLUE_A  = PatternFill("solid", fgColor="CCE5FF")   # Account A rows
GREEN_B = PatternFill("solid", fgColor="D4EDDA")   # Account B rows
HEADER  = PatternFill("solid", fgColor="343A40")
RED_ALT = PatternFill("solid", fgColor="FFF8F8")
WHITE   = PatternFill("solid", fgColor="FFFFFF")

def hdr_cell(cell, text=""):
    cell.value     = text
    cell.fill      = HEADER
    cell.font      = Font(bold=True, color="FFFFFF")
    cell.alignment = Alignment(horizontal="center")

def border_all(cell):
    s = Side(style="thin", color="CCCCCC")
    cell.border = Border(left=s, right=s, top=s, bottom=s)

def set_cols(ws, widths):
    for col, w in widths.items():
        ws.column_dimensions[col].width = w


# ── Sheet 1: Summary ────────────────────────────────────────────

def write_summary(ws, stats, by_acc):
    ws.sheet_view.showGridLines = False

    hdr_cell(ws["A1"], "Mercado Pago — Labeling Status")
    ws["A1"].font = Font(bold=True, size=14, color="FFFFFF")
    ws.merge_cells("A1:D1")
    ws.row_dimensions[1].height = 30

    hdrs = ["", "Account A (Expense)", "Account B (Concentrator)", "Total"]
    for c, h in enumerate(hdrs, 1):
        hdr_cell(ws.cell(3, c), h)

    total, unlabeled, work, personal = stats
    a_total = by_acc.get("A", 0)
    b_total = by_acc.get("B", 0)

    rows = [
        ("Total entries",     a_total,            b_total,            total),
        ("Unlabeled",         "—",                "—",                unlabeled),
        ("Labeled Work",      "—",                "—",                work),
        ("Labeled Personal",  "—",                "—",                personal),
    ]
    for r, (label, a, b, tot) in enumerate(rows, 4):
        ws.cell(r, 1, label).font = Font(bold=True)
        ws.cell(r, 2, a)
        ws.cell(r, 3, b)
        ws.cell(r, 4, tot)
        for c in range(1, 5):
            border_all(ws.cell(r, c))
            ws.cell(r, c).alignment = Alignment(horizontal="center")

    ws.cell(8, 1, "Instructions").font = Font(bold=True, size=12)
    instructions = [
        "1. Go to the Account A or Account B sheet to label transactions.",
        "2. Select Classification (Work / Personal) and Subcategory from dropdowns.",
        "3. Intercompany rows (yellow) are auto-detected — no labeling needed.",
        "4. When done, save this file and run: python scripts/excel/import.py",
        "5. Run this script again to get a fresh export.",
    ]
    for i, line in enumerate(instructions, 9):
        ws.cell(i, 1, line)
        ws.merge_cells(f"A{i}:D{i}")

    set_cols(ws, {"A": 28, "B": 22, "C": 26, "D": 14})


# ── Sheet 2 & 3: To-Label (A and B) ─────────────────────────────

def write_label_sheet(ws, account, rows, categories):
    ws.sheet_view.showGridLines = False
    color = BLUE_A if account == "A" else GREEN_B
    label = "Account A — To Label" if account == "A" else "Account B — To Label"
    account_tag = "🔵 Account A"  if account == "A" else "🟢 Account B"

    hdr_cell(ws["A1"], f"{account_tag}  |  {label}")
    ws["A1"].font = Font(bold=True, size=13, color="FFFFFF")
    ws.merge_cells("A1:I1")
    ws.row_dimensions[1].height = 28

    col_headers = [
        "internal_id", "Date", "Category", "Classification",
        "Subcategory", "Description", "Gross", "Fee", "Intercompany"
    ]
    for c, h in enumerate(col_headers, 1):
        hdr_cell(ws.cell(3, c), h)

    conn_fill   = PatternFill("solid", fgColor="D4EDDA")
    work_fill   = PatternFill("solid", fgColor="D1ECF1")
    pers_fill   = PatternFill("solid", fgColor="F8D7DA")

    class_cols = {"": WHITE, "Work": work_fill, "Personal": pers_fill}

    for r_idx, row in enumerate(rows, 4):
        (iid, sa, date, cat, desc, gross, fee, net, interco) = row
        fill = YELLOW if interco else color

        vals = [iid, date, cat, "", "", desc, gross, fee, "⚠️ YES" if interco else ""]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r_idx, c, v)
            cell.fill = fill
            border_all(cell)
            if c in (7, 8):
                cell.number_format = '"$"#,##0.00'
                cell.alignment = Alignment(horizontal="right")
            elif c == 9:
                cell.alignment = Alignment(horizontal="center")

        # Dropdown for Classification
        dv = ws.cell(r_idx, 4)
        dv.fill = class_cols.get("", WHITE)
        dv.value = ""
        border_all(dv)

        # Dropdown for Subcategory
        ws.cell(r_idx, 5).fill = WHITE
        border_all(ws.cell(r_idx, 5))

    widths = {"A": 36, "B": 12, "C": 22, "D": 16, "E": 24, "F": 38, "G": 14, "H": 12, "I": 14}
    set_cols(ws, widths)


# ── Sheet 4: All Labeled (Review) ───────────────────────────────

def write_review_sheet(ws, rows):
    ws.sheet_view.showGridLines = False

    hdr_cell(ws["A1"], "✅  All Labeled Transactions (Read-Only)")
    ws["A1"].font = Font(bold=True, size=13, color="FFFFFF")
    ws.merge_cells("A1:K1")
    ws.row_dimensions[1].height = 28

    col_headers = [
        "internal_id", "Acct", "Date", "Category", "Classification",
        "Subcategory", "Description", "Gross", "Fee", "Net", "Interco"
    ]
    for c, h in enumerate(col_headers, 1):
        hdr_cell(ws.cell(3, c), h)

    for r_idx, row in enumerate(rows, 4):
        (iid, sa, date, cat, subcat, cls, desc, gross, fee, net, interco) = row
        vals = [iid, sa, date, cat, cls, subcat, desc, gross, fee, net,
                "⚠️" if interco else ""]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r_idx, c, v)
            cell.fill = YELLOW if interco else (BLUE_A if sa == "A" else GREEN_B)
            border_all(cell)
            if c in (8, 9, 10):
                cell.number_format = '"$"#,##0.00'
                cell.alignment = Alignment(horizontal="right")

    set_cols(ws, {"A": 36, "B": 6, "C": 12, "D": 22, "E": 14, "F": 24,
                  "G": 38, "H": 14, "I": 12, "J": 14, "K": 8})


# ── Sheet 5: Catalog ─────────────────────────────────────────────

def write_catalog(ws, categories_csv_path):
    ws.sheet_view.showGridLines = False
    hdr_cell(ws["A1"], "📂  Chart of Accounts — Editable")
    ws["A1"].font = Font(bold=True, size=13, color="FFFFFF")
    ws.merge_cells("A1:C1")

    for c, h in enumerate(["Context", "Direction", "Subcategory"], 1):
        hdr_cell(ws.cell(3, c), h)

    with open(categories_csv_path) as f:
        for r_idx, line in enumerate(f, 4):
            parts = line.strip().split(",")
            if len(parts) >= 3:
                for c, v in enumerate(parts[:3], 1):
                    cell = ws.cell(r_idx, c, v.strip())
                    border_all(cell)
                    if parts[0].strip() == "Context":
                        cell.fill = PatternFill("solid", fgColor="E9ECEF")

    set_cols(ws, {"A": 16, "B": 12, "C": 34})


# ── Sheet 6: P&L ─────────────────────────────────────────────────

def calc_pnl(rows):
    totals = {"Work": {"inflow": 0, "outflow": 0}, "Personal": {"inflow": 0, "outflow": 0}}
    by_acc = {"A": 0.0, "B": 0.0}
    for row in rows:
        cls, subcat, cat, gross, fee, sa = row
        if cls not in ("Work", "Personal") or gross is None:
            continue
        direction = "inflow" if gross > 0 else "outflow"
        totals[cls][direction] += abs(gross)
        if cls == "Work":
            by_acc[sa] = by_acc.get(sa, 0) + abs(gross)

    return totals, by_acc


def write_pnl(ws, rows):
    ws.sheet_view.showGridLines = False
    hdr_cell(ws["A1"], "💰  Profit & Loss Summary")
    ws["A1"].font = Font(bold=True, size=14, color="FFFFFF")
    ws.merge_cells("A1:C1")
    ws.row_dimensions[1].height = 30

    totals, by_acc = calc_pnl(rows)

    r = 3
    ws.cell(r, 1, "Work").font = Font(bold=True, size=12)
    ws.merge_cells(f"A{r}:C{r}")
    r += 1

    for label, key in [("Work Inflow", "Work"), ("Work Outflow", "Work")]:
        val = totals[key]["inflow"] if "Inflow" in label else totals[key]["outflow"]
        label_text = "Work Inflow" if "Inflow" in label else "Work Outflow"
        ws.cell(r, 1, label_text).fill = PatternFill("solid", fgColor="D1ECF1")
        ws.cell(r, 3, val).number_format = '"$"#,##0.00'
        ws.cell(r, 3).alignment = Alignment(horizontal="right")
        for c in range(1, 4):
            border_all(ws.cell(r, c))
        r += 1

    net_work = totals["Work"]["inflow"] - totals["Work"]["outflow"]
    ws.cell(r, 1, "Net Work").font = Font(bold=True)
    ws.cell(r, 3, net_work).number_format = '"$"#,##0.00'
    ws.cell(r, 3).font = Font(bold=True, color="28A745" if net_work >= 0 else "DC3545")
    ws.cell(r, 3).alignment = Alignment(horizontal="right")
    for c in range(1, 4):
        border_all(ws.cell(r, c))
    r += 2

    ws.cell(r, 1, "Personal").font = Font(bold=True, size=12)
    ws.merge_cells(f"A{r}:C{r}")
    r += 1

    for label_text, key in [("Personal Inflow", "Personal"), ("Personal Outflow", "Personal")]:
        val = totals[key]["inflow"] if "Inflow" in label_text else totals[key]["outflow"]
        ws.cell(r, 1, label_text).fill = PatternFill("solid", fgColor="F8D7DA")
        ws.cell(r, 3, val).number_format = '"$"#,##0.00'
        ws.cell(r, 3).alignment = Alignment(horizontal="right")
        for c in range(1, 4):
            border_all(ws.cell(r, c))
        r += 1

    net_pers = totals["Personal"]["inflow"] - totals["Personal"]["outflow"]
    ws.cell(r, 1, "Net Personal").font = Font(bold=True)
    ws.cell(r, 3, net_pers).number_format = '"$"#,##0.00'
    ws.cell(r, 3).font = Font(bold=True, color="28A745" if net_pers >= 0 else "DC3545")
    ws.cell(r, 3).alignment = Alignment(horizontal="right")
    for c in range(1, 4):
        border_all(ws.cell(r, c))
    r += 2

    ws.cell(r, 1, "By Account (Work Revenue)").font = Font(bold=True)
    ws.merge_cells(f"A{r}:C{r}")
    r += 1
    ws.cell(r, 1, "Account A (Expense)").fill = BLUE_A
    ws.cell(r, 2, "Account B (Concentrator)").fill = GREEN_B
    for c in range(1, 4):
        border_all(ws.cell(r, c))
    r += 1
    ws.cell(r, 1, by_acc.get("A", 0)); ws.cell(r, 1).number_format = '"$"#,##0.00'
    ws.cell(r, 2, by_acc.get("B", 0)); ws.cell(r, 2).number_format = '"$"#,##0.00'
    for c in range(1, 4):
        border_all(ws.cell(r, c))
        ws.cell(r, c).alignment = Alignment(horizontal="right")

    set_cols(ws, {"A": 28, "B": 26, "C": 18})


# ── Main ────────────────────────────────────────────────────────

def run_export():
    init_db()

    stats   = get_ledger_stats()
    unlabeled_a = get_unlabeled_for_export("A", limit=500)
    unlabeled_b = get_unlabeled_for_export("B", limit=500)
    labeled     = get_labeled_for_review()
    pnl_rows    = get_pnl_data()

    categories_path = Path(__file__).parent.parent.parent / "data" / "categories.csv"

    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    out_path = Path(__file__).parent.parent.parent / "output" / f"v7_labeling_{ts}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    wb.remove(wb.active)   # remove default sheet

    ws1 = wb.create_sheet("📊 Summary")
    ws2 = wb.create_sheet("🔵 Account A — To Label")
    ws3 = wb.create_sheet("🟢 Account B — To Label")
    ws4 = wb.create_sheet("✅ All Labeled")
    ws5 = wb.create_sheet("📂 Catalog")
    ws6 = wb.create_sheet("💰 P&L")

    write_summary(ws1, stats[:4], stats[4])
    write_label_sheet(ws2, "A", unlabeled_a, categories_path)
    write_label_sheet(ws3, "B", unlabeled_b, categories_path)
    write_review_sheet(ws4, labeled)
    write_catalog(ws5, categories_path)
    write_pnl(ws6, pnl_rows)

    wb.save(out_path)
    print(f"\n✅  Exported: {out_path}")
    print(f"    Unlabeled A: {len(unlabeled_a)}  |  Unlabeled B: {len(unlabeled_b)}")
    print(f"    Labeled: {len(labeled)}")


if __name__ == "__main__":
    run_export()
