"""
Excel Export — v7 Multi-Account Labeling with Weekly P&L

6 sheets:
  1. 📊 Summary     — stats by account
  2. 🔵 Account A   — unlabeled from Account A
  3. 🟢 Account B   — unlabeled from Account B
  4. ✅ All Labeled — review / un-label
  5. 📂 Catalog     — Chart of Accounts (editable, source of truth)
  6. 💰 P&L         — weekly columns, SUMIFS from Catalog cell refs

P&L Design:
  - Category names live ONLY in Catalog (col C, rows 2+)
  - P&L section header rows list subcategory ROW numbers (e.g., "→ row 4")
  - SUMIFS formulas reference Catalog!$C4, Catalog!$C5, etc.
  - To rename a category: change ONE cell in Catalog col C → all formulas update
  - Weekly columns: last 13 weeks of data, live from "To Label (All)"

Usage:
    python scripts/excel/export.py
"""
import sys, os, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import (
    get_connection, get_unlabeled_for_export, get_labeled_for_review,
    get_pnl_data, init_db,
)
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Styles ──────────────────────────────────────────────────────

THIN   = Side(style="thin",   color="CCCCCC")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

def fill(hex_):  return PatternFill("solid", fgColor=hex_)
def font(bold=False, color="000000", size=10): return Font(bold=bold, color=color, size=size)
def align(h="left", v="center", wrap=False):    return Alignment(horizontal=h, vertical=v, wrap_text=wrap)

HDR_FILL   = fill("343A40")
HDR_FONT   = font(bold=True, color="FFFFFF")
BLUE_A_FILL = fill("CCE5FF")
GREEN_B_FILL = fill("D4EDDA")
YELLOW_FILL  = fill("FFF3CD")
INCOME_FILL  = fill("D1ECF1")
EXPENSE_FILL = fill("FCE4D6")
TOTAL_FILL   = fill("DEEAF1")
NET_FILL     = fill("1F3864")
WHITE_FILL   = fill("FFFFFF")

def hdr_cell(cell, text=""):
    cell.value = text; cell.fill = HDR_FILL
    cell.font  = HDR_FONT
    cell.alignment = align("center")
    cell.border = BORDER

def styled(cell, value=None, fill_=None, bold=False, color="000000",
           h="left", size=10, border=True, fmt=None, wrap=False):
    if value is not None: cell.value = value
    if fill_:            cell.fill = fill_
    cell.font      = font(bold=bold, color=color, size=size)
    cell.alignment = align(h=h, wrap=wrap)
    if border: cell.border = BORDER
    if fmt:    cell.number_format = fmt

def set_cols(ws, widths):
    for col, w in widths.items():
        ws.column_dimensions[col].width = w


# ════════════════════════════════════════════════════════════════
# SHEET 1 — SUMMARY
# ════════════════════════════════════════════════════════════════

def write_summary(ws, total, unlabeled, work, personal, by_acc):
    ws.sheet_view.showGridLines = False
    hdr_cell(ws["A1"], "Mercado Pago — Labeling Status  (v7)")
    ws["A1"].font = Font(bold=True, size=14, color="FFFFFF")
    ws.merge_cells("A1:D1"); ws.row_dimensions[1].height = 30

    for c, h in enumerate(["", "Account A (Expense)", "Account B (Concentrator)", "Total"], 1):
        hdr_cell(ws.cell(3, c), h)

    rows_ = [
        ("Total entries",     by_acc.get("A", 0), by_acc.get("B", 0), total),
        ("Unlabeled",         "—",               "—",                unlabeled),
        ("Labeled Work",      "—",               "—",                work),
        ("Labeled Personal",  "—",               "—",                personal),
    ]
    for r, (lbl, a, b, tot) in enumerate(rows_, 4):
        styled(ws.cell(r, 1), lbl, bold=True)
        for c, v in enumerate([a, b, tot], 2):
            styled(ws.cell(r, c), v, h="center")

    for r in range(4, 8):
        for c in range(1, 5): ws.cell(r, c).border = BORDER

    styled(ws.cell(8, 1), "Instructions", bold=True, size=12)
    lines = [
        "1. Open Account A or Account B sheet → label transactions.",
        "2. Col E = Classification (Work/Personal), Col F = Subcategory.",
        "3. Yellow rows = intercompany (auto-detected, no labeling needed).",
        "4. When done: save file → run: python scripts/excel/import.py",
        "5. Run this script again for a fresh export.",
        "6. Weekly P&L is on the 💰 P&L sheet — live from To Label (All).",
    ]
    for i, line in enumerate(lines, 9):
        ws.merge_cells(f"A{i}:D{i}")
        styled(ws.cell(i, 1), line, color="555555", size=9)

    set_cols(ws, {"A": 28, "B": 22, "C": 26, "D": 14})


# ════════════════════════════════════════════════════════════════
# SHEET 2 & 3 — TO LABEL (Account A and B)
# ════════════════════════════════════════════════════════════════

LABEL_HDRS = [
    "internal_id", "Date", "Category", "Classification",
    "Subcategory", "Description", "Gross", "Fee", "Intercompany"
]

def write_label_sheet(ws, account, rows):
    ws.sheet_view.showGridLines = False
    acct_fill  = BLUE_A_FILL if account == "A" else GREEN_B_FILL
    tag        = "🔵 Account A — To Label" if account == "A" else "🟢 Account B — To Label"

    hdr_cell(ws["A1"], tag)
    ws["A1"].font = Font(bold=True, size=13, color="FFFFFF")
    ws.merge_cells("A1:I1"); ws.row_dimensions[1].height = 26

    for c, h in enumerate(LABEL_HDRS, 1):
        hdr_cell(ws.cell(3, c), h)

    for r_idx, row in enumerate(rows, 4):
        iid, sa, date, cat, desc, gross, fee, net, interco = row
        row_fill = YELLOW_FILL if interco else acct_fill

        vals = [iid, (str(date)[:10] if date else ""), cat, "", "", desc, gross, fee,
                "⚠️ INTERCO" if interco else ""]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(r_idx, c, v)
            cell.fill = row_fill; cell.border = BORDER
            if c in (7, 8): cell.number_format = '"$"#,##0.00'
            if c == 9 and interco: cell.alignment = align("center")

    set_cols(ws, {"A": 36, "B": 12, "C": 22, "D": 16, "E": 24,
                  "F": 38, "G": 14, "H": 12, "I": 14})


# ════════════════════════════════════════════════════════════════
# SHEET 4 — ALL LABELED (Review)
# ════════════════════════════════════════════════════════════════

def write_review_sheet(ws, rows):
    ws.sheet_view.showGridLines = False
    hdr_cell(ws["A1"], "✅  All Labeled Transactions (Read-Only)")
    ws["A1"].font = Font(bold=True, size=13, color="FFFFFF")
    ws.merge_cells("A1:K1"); ws.row_dimensions[1].height = 26

    for c, h in enumerate(["internal_id","Acct","Date","Category","Classification",
                             "Subcategory","Description","Gross","Fee","Net","Interco"], 1):
        hdr_cell(ws.cell(3, c), h)

    for r_idx, row in enumerate(rows, 4):
        iid, sa, date, cat, subcat, cls, desc, gross, fee, net, interco = row
        rfill = YELLOW_FILL if interco else (BLUE_A_FILL if sa == "A" else GREEN_B_FILL)
        for c, v in enumerate([iid, sa, (str(date)[:10] if date else ""), cat,
                                 cls, subcat, desc, gross, fee, net,
                                 "⚠️" if interco else ""], 1):
            cell = ws.cell(r_idx, c, v)
            cell.fill = rfill; cell.border = BORDER
            if c in (8, 9, 10): cell.number_format = '"$"#,##0.00'

    set_cols(ws, {"A": 36, "B": 6, "C": 12, "D": 22, "E": 14, "F": 24,
                  "G": 38, "H": 14, "I": 12, "J": 14, "K": 8})


# ════════════════════════════════════════════════════════════════
# SHEET 5 — CATALOG  (Source of Truth for P&L cell refs)
# ════════════════════════════════════════════════════════════════

CATALOG = [
    # (Context,    Direction,  Subcategory)
    ("Context",    "Direction", "Subcategory"),          # row 2 — header
    ("Work",       "Inflow",   "Client Payment"),
    ("Work",       "Inflow",   "Retainer"),
    ("Work",       "Inflow",   "Capital Injection"),
    ("Work",       "Inflow",   "Reimbursement"),
    ("Work",       "Inflow",   "Interest / Yield"),
    ("Work",       "Outflow",  "Liquor"),
    ("Work",       "Outflow",  "Wine Supplier"),
    ("Work",       "Outflow",  "Craft Beer"),
    ("Work",       "Outflow",  "Butcher"),
    ("Work",       "Outflow",  "Groceries"),
    ("Work",       "Outflow",  "Ginger Beer"),
    ("Work",       "Outflow",  "Septic Maint"),
    ("Work",       "Outflow",  "Payroll"),
    ("Work",       "Outflow",  "Kitchen / Cutlery"),
    ("Work",       "Outflow",  "Software / Subscriptions"),
    ("Work",       "Outflow",  "Contractors / Freelancers"),
    ("Work",       "Outflow",  "Equipment"),
    ("Work",       "Outflow",  "Marketing / Ads"),
    ("Work",       "Outflow",  "Banking Fees"),
    ("Work",       "Outflow",  "Utilities"),
    ("Work",       "Outflow",  "Rent"),
    ("Work",       "Outflow",  "Professional Services"),
    ("Work",       "Outflow",  "Taxes & Licenses"),
    ("Work",       "Outflow",  "Travel"),
    ("Work",       "Outflow",  "Fuel"),
    ("Work",       "Outflow",  "Maintenance"),
    ("Work",       "Outflow",  "Insurance"),
    ("Work",       "Outflow",  "Other"),
    ("Personal",   "Inflow",   "Wallet Top-Up"),
    ("Personal",   "Inflow",   "Owner Draw"),
    ("Personal",   "Inflow",   "Refund / Reimbursement"),
    ("Personal",   "Inflow",   "Gift / Repayment"),
    ("Personal",   "Outflow",  "Groceries"),
    ("Personal",   "Outflow",  "Restaurants"),
    ("Personal",   "Outflow",  "Personal Shopping"),
    ("Personal",   "Outflow",  "Health / Wellness"),
    ("Personal",   "Outflow",  "Travel"),
    ("Personal",   "Outflow",  "Entertainment"),
    ("Personal",   "Outflow",  "Education"),
    ("Personal",   "Outflow",  "Other"),
]

# Build a lookup: subcategory name → catalog ROW index (1-based)
CATALOG_ROW = {name: idx + 2 for idx, (_, _, name) in enumerate(CATALOG) if name != "Subcategory"}


def write_catalog_sheet(ws):
    ws.sheet_view.showGridLines = False
    hdr_cell(ws["A1"], "📂  Chart of Accounts — Source of Truth  (edit column C to rename → P&L updates auto)")
    ws["A1"].font = Font(bold=True, size=12, color="FFFFFF")
    ws.merge_cells("A1:C1"); ws.row_dimensions[1].height = 28

    for c, h in enumerate(["Context", "Direction", "Subcategory  ← edit here to rename"], 1):
        hdr_cell(ws.cell(2, c), h)

    ctx_fills = {"Work": fill("D1ECF1"), "Personal": fill("F8D7DA"),
                 "Context": fill("E9ECEF")}

    for r_idx, (ctx, dr, name) in enumerate(CATALOG, 3):
        f = ctx_fills.get(ctx, WHITE_FILL)
        for c, v in enumerate([ctx, dr, name], 1):
            cell = ws.cell(r_idx, c, v)
            cell.fill = f; cell.border = BORDER
            cell.alignment = align()
        if r_idx == 3:
            ws.cell(r_idx, 1).font = Font(italic=True, color="888888")

    # Note at bottom
    note_row = len(CATALOG) + 4
    ws.merge_cells(f"A{note_row}:C{note_row}")
    styled(ws.cell(note_row, 1),
           "💡 To rename a category: change the name in col C above. "
           "All P&L SUMIFS formulas reference these cells — no formula editing needed.",
           color="555555", size=9, wrap=True)
    ws.row_dimensions[note_row].height = 30

    set_cols(ws, {"A": 14, "B": 12, "C": 38})


# ════════════════════════════════════════════════════════════════
# SHEET 6 — P&L WEEKLY  (SUMIFS from Catalog cell refs)
# ════════════════════════════════════════════════════════════════

MONEY = '"$"#,##0.00'

def to_week_key(dt) -> str:
    """Convert date value to ISO week string 'YYYY-Wnn'."""
    if not dt: return ""
    s = str(dt).strip()[:10]
    try:
        from pandas import Timestamp
        ts = Timestamp(s)
        if str(ts) == "NaT": return ""
        y, wk, _ = ts.isocalendar()
        return f"{y}-W{wk:02d}"
    except Exception:
        return ""


def build_pnl_sheet(ws, df_all):
    """
    df_all: DataFrame with columns [internal_id, source_account, date, category,
            subcategory, description, gross_amount, mp_fee, net_amount,
            classification, source]
    """
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "B4"

    # ── Compute week keys ─────────────────────────────────────
    df_dated = df_all[df_all["date"].apply(to_week_key) != ""].copy()
    df_dated["week_key"] = df_dated["date"].apply(to_week_key)
    all_weeks = sorted(df_dated["week_key"].unique())
    recent_weeks = all_weeks[-13:] if len(all_weeks) > 13 else all_weeks

    DATA_COL_START = 3          # col C = first week
    LAST_WEEK_COL  = DATA_COL_START + len(recent_weeks) - 1
    TOTAL_COL      = LAST_WEEK_COL + 1
    TOTAL_COL_L    = get_column_letter(TOTAL_COL)

    # ── Row 1: title ───────────────────────────────────────────
    ws.merge_cells(f"A1:{TOTAL_COL_L}1")
    ws["A1"].value     = "P62  WEEKLY PROFIT & LOSS"
    ws["A1"].font      = Font(bold=True, size=14, color="FFFFFF")
    ws["A1"].fill      = fill("1F3864")
    ws["A1"].alignment = align("center")
    ws.row_dimensions[1].height = 28

    # ── Row 2: column headers ─────────────────────────────────
    hdr_cell(ws.cell(2, 1), "LINE ITEM")
    for wi, wk in enumerate(recent_weeks, DATA_COL_START):
        hdr_cell(ws.cell(2, wi), wk)
    hdr_cell(ws.cell(2, TOTAL_COL), "ALL TIME")

    for ci in range(1, TOTAL_COL + 1):
        ws.column_dimensions[get_column_letter(ci)].width = 11
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 2   # spacer

    # ── Catalog row lookup helper ──────────────────────────────
    def cat_row(name) -> int:
        """Return the Catalog sheet row number for a subcategory name."""
        return CATALOG_ROW.get(name, None)

    # ── Formula builder ───────────────────────────────────────
    def sumifs_income(subcategory_name, week_col_letter):
        """Work income: classification=Work, subcategory matches, net>0."""
        cr = cat_row(subcategory_name)
        if cr is None: return "0"
        cat_ref = f"Catalog!$C{cr}"
        if week_col_letter:
            return (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                    f"'To Label (All)'!$E:$E,\"Work\","
                    f"'To Label (All)'!$F:$F,{cat_ref},"
                    f"'To Label (All)'!$G:$G,{week_col_letter}$2,"
                    f"'To Label (All)'!$K:$K,\">0\"),0)")
        else:   # ALL TIME — no week filter
            return (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                    f"'To Label (All)'!$E:$E,\"Work\","
                    f"'To Label (All)'!$F:$F,{cat_ref},"
                    f"'To Label (All)'!$K:$K,\">0\"),0)")

    def sumifs_expense(subcategory_name, week_col_letter):
        """Work expense: classification=Work, subcategory matches, net<0."""
        cr = cat_row(subcategory_name)
        if cr is None: return "0"
        cat_ref = f"Catalog!$C{cr}"
        if week_col_letter:
            return (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                    f"'To Label (All)'!$E:$E,\"Work\","
                    f"'To Label (All)'!$F:$F,{cat_ref},"
                    f"'To Label (All)'!$G:$G,{week_col_letter}$2,"
                    f"'To Label (All)'!$K:$K,\"<0\"),0)")
        else:
            return (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                    f"'To Label (All)'!$E:$E,\"Work\","
                    f"'To Label (All)'!$F:$F,{cat_ref},"
                    f"'To Label (All)'!$K:$K,\"<0\"),0)")

    def section_bar(row, label):
        ws.cell(row, 1, f"  {label}")
        for ci in range(1, TOTAL_COL + 1):
            c = ws.cell(row, ci)
            c.fill   = fill("D9E1F2")
            c.font   = Font(bold=True, color="1F3864", size=10)
            c.border = BORDER
            c.alignment = align("left")

    def data_row(row, label, formula_fn, font_color="000000", bg=None):
        c = ws.cell(row, 1, f"  {label}")
        c.font = Font(size=10, color=font_color)
        c.border = BORDER; c.alignment = align("left")
        if bg:
            c.fill = fill(bg)

        income_rows_refs = []
        for wi in range(DATA_COL_START, TOTAL_COL + 1):
            cl = get_column_letter(wi)
            wk_letter = cl if wi < TOTAL_COL else None
            formula = formula_fn(wk_letter)
            cell = ws.cell(row, wi, formula if wi < TOTAL_COL else formula)
            cell.number_format = MONEY
            cell.font = Font(size=10, color=font_color)
            cell.border = BORDER
            cell.alignment = align(h="right")
            if bg: cell.fill = fill(bg)

    def total_row(row, label, component_rows, font_color, bg_hex):
        c = ws.cell(row, 1, f"  {label}")
        c.font = Font(bold=True, size=10, color=font_color)
        c.fill = fill(bg_hex); c.border = BORDER; c.alignment = align("left")
        for ci in range(DATA_COL_START, TOTAL_COL + 1):
            cl = get_column_letter(ci)
            refs = "+".join([f"{cl}{r}" for r in component_rows])
            cell = ws.cell(row, ci, f"={refs}")
            cell.number_format = MONEY
            cell.font = Font(bold=True, size=10, color=font_color)
            cell.fill = fill(bg_hex); cell.border = BORDER
            cell.alignment = align(h="right")
        return row

    row = 3

    # ── SECTION: WORK INCOME ──────────────────────────────────
    section_bar(row, "WORK  INCOME"); row += 1
    income_rows = []

    income_items = ["Client Payment", "Retainer", "Interest / Yield"]
    for name in income_items:
        cr = cat_row(name)
        if not cr: continue
        formulas_w = {}; formulas_t = {}
        for wi in range(DATA_COL_START, TOTAL_COL):
            cl = get_column_letter(wi)
            formulas_w[wi] = sumifs_income(name, cl)
        formulas_t[TOTAL_COL] = sumifs_income(name, None)
        data_row(row, name, lambda wl: formulas_w.get(wi, "0") if False else None,
                 font_color="0070C0", bg="DEEAF1")
        # Manual per-column write
        for wi in range(DATA_COL_START, TOTAL_COL):
            wl = get_column_letter(wi)
            c = ws.cell(row, wi)
            c.value = sumifs_income(name, wl)
            c.number_format = MONEY; c.font = Font(size=10, color="0070C0")
            c.border = BORDER; c.alignment = align(h="right"); c.fill = fill("DEEAF1")
        ws.cell(row, TOTAL_COL).value = sumifs_income(name, None)
        ws.cell(row, TOTAL_COL).number_format = MONEY
        ws.cell(row, TOTAL_COL).font = Font(size=10, color="0070C0")
        ws.cell(row, TOTAL_COL).border = BORDER
        ws.cell(row, TOTAL_COL).alignment = align(h="right")
        ws.cell(row, TOTAL_COL).fill = fill("DEEAF1")
        income_rows.append(row); row += 1

    # POS Sales income row — category = "POS Sale" in col D
    formulas_ps = {}; formulas_ps_all = {}
    for wi in range(DATA_COL_START, TOTAL_COL):
        cl = get_column_letter(wi)
        formulas_ps[wi] = (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                            f"'To Label (All)'!$E:$E,\"Work\","
                            f"'To Label (All)'!$D:$D,\"POS Sale\","
                            f"'To Label (All)'!$G:$G,{cl}$2,"
                            f"'To Label (All)'!$K:$K,\">0\"),0)")
    formulas_ps_all[TOTAL_COL] = (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                                    f"'To Label (All)'!$E:$E,\"Work\","
                                    f"'To Label (All)'!$D:$D,\"POS Sale\","
                                    f"'To Label (All)'!$K:$K,\">0\"),0)")
    for wi in range(DATA_COL_START, TOTAL_COL):
        c = ws.cell(row, wi); c.value = formulas_ps[wi]
        c.number_format = MONEY; c.font = Font(size=10, color="0070C0")
        c.border = BORDER; c.alignment = align(h="right"); c.fill = fill("DEEAF1")
    ws.cell(row, 1, "  POS Sales"); ws.cell(row, 1).font = Font(size=10, color="0070C0")
    ws.cell(row, 1).border = BORDER; ws.cell(row, 1).fill = fill("DEEAF1")
    ws.cell(row, TOTAL_COL).value = formulas_ps_all[TOTAL_COL]
    ws.cell(row, TOTAL_COL).number_format = MONEY
    ws.cell(row, TOTAL_COL).font = Font(size=10, color="0070C0")
    ws.cell(row, TOTAL_COL).border = BORDER
    ws.cell(row, TOTAL_COL).alignment = align(h="right")
    ws.cell(row, TOTAL_COL).fill = fill("DEEAF1")
    income_rows.append(row); row += 1

    row = total_row(row, "Work Income Total", income_rows, "0070C0", "BDD7EE"); row += 2

    # ── SECTION: WORK EXPENSES ────────────────────────────────
    expense_rows = []
    section_bar(row, "WORK  EXPENSES"); row += 1

    expense_items = [
        "Liquor", "Wine Supplier", "Craft Beer", "Butcher", "Groceries",
        "Ginger Beer", "Septic Maint", "Payroll", "Kitchen / Cutlery",
        "Software / Subscriptions", "Contractors / Freelancers", "Equipment",
        "Marketing / Ads", "Banking Fees", "Utilities", "Rent",
        "Professional Services", "Taxes & Licenses", "Travel", "Fuel",
        "Maintenance", "Insurance", "Other",
    ]

    for name in expense_items:
        cr = cat_row(name)
        if not cr: continue
        for wi in range(DATA_COL_START, TOTAL_COL):
            cl = get_column_letter(wi)
            c = ws.cell(row, wi)
            c.value = sumifs_expense(name, cl)
            c.number_format = MONEY; c.font = Font(size=10, color="C00000")
            c.border = BORDER; c.alignment = align(h="right"); c.fill = fill("FCE4D6")
        ws.cell(row, 1, f"  {name}")
        ws.cell(row, 1).font = Font(size=10, color="C00000")
        ws.cell(row, 1).border = BORDER; ws.cell(row, 1).fill = fill("FCE4D6")
        ws.cell(row, TOTAL_COL).value = sumifs_expense(name, None)
        ws.cell(row, TOTAL_COL).number_format = MONEY
        ws.cell(row, TOTAL_COL).font = Font(size=10, color="C00000")
        ws.cell(row, TOTAL_COL).border = BORDER
        ws.cell(row, TOTAL_COL).alignment = align(h="right")
        ws.cell(row, TOTAL_COL).fill = fill("FCE4D6")
        expense_rows.append(row); row += 1

    # MP Fees row
    for wi in range(DATA_COL_START, TOTAL_COL):
        cl = get_column_letter(wi)
        c = ws.cell(row, wi)
        c.value = (f"=IFERROR(SUMIFS('To Label (All)'!$J:$J,"
                   f"'To Label (All)'!$E:$E,\"Work\","
                   f"'To Label (All)'!$G:$G,{cl}$2),0)")
        c.number_format = MONEY; c.font = Font(size=10, color="C00000")
        c.border = BORDER; c.alignment = align(h="right"); c.fill = fill("FCE4D6")
    ws.cell(row, 1, "  MP Fees (Work)"); ws.cell(row, 1).font = Font(size=10, color="C00000")
    ws.cell(row, 1).border = BORDER; ws.cell(row, 1).fill = fill("FCE4D6")
    ws.cell(row, TOTAL_COL).value = ("=IFERROR(SUMIFS('To Label (All)'!$J:$J,"
                                      "'To Label (All)'!$E:$E,\"Work\"),0)")
    ws.cell(row, TOTAL_COL).number_format = MONEY
    ws.cell(row, TOTAL_COL).font = Font(size=10, color="C00000")
    ws.cell(row, TOTAL_COL).border = BORDER
    ws.cell(row, TOTAL_COL).alignment = align(h="right")
    ws.cell(row, TOTAL_COL).fill = fill("FCE4D6")
    expense_rows.append(row); row += 1

    row = total_row(row, "Work Expenses Total", expense_rows, "C00000", "F4B8B8"); row += 2

    # ── NET PROFIT ─────────────────────────────────────────────
    income_total_r = income_rows[-1] if income_rows else row - 5
    expense_total_r = expense_rows[-1] if expense_rows else row - 4
    for wi in range(DATA_COL_START, TOTAL_COL + 1):
        cl = get_column_letter(wi)
        c = ws.cell(row, wi)
        c.value = f"={cl}{income_total_r}+{cl}{expense_total_r}"
        c.number_format = MONEY
        c.font = Font(bold=True, size=11, color="FFFFFF")
        c.fill = fill("1F3864"); c.border = BORDER; c.alignment = align(h="right")
    ws.cell(row, 1, "  NET PROFIT"); ws.cell(row, 1).font = Font(bold=True, size=11, color="FFFFFF")
    ws.cell(row, 1).fill = fill("1F3864"); ws.cell(row, 1).border = BORDER
    row += 2

    # ── PERSONAL SECTION ─────────────────────────────────────
    section_bar(row, "PERSONAL  (Cash Flow — not P&L)"); row += 1
    personal_rows = []

    personal_items = ["Wallet Top-Up", "Owner Draw"]
    for name in personal_items:
        cr = cat_row(name)
        if not cr: continue
        for wi in range(DATA_COL_START, TOTAL_COL):
            cl = get_column_letter(wi)
            c = ws.cell(row, wi)
            c.value = (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                       f"'To Label (All)'!$E:$E,\"Personal\","
                       f"'To Label (All)'!$F:$F,Catalog!$C{cr},"
                       f"'To Label (All)'!$G:$G,{cl}$2),0)")
            c.number_format = MONEY; c.font = Font(size=10, color="7B7B7B")
            c.border = BORDER; c.alignment = align(h="right"); c.fill = fill("F8F9FA")
        ws.cell(row, 1, f"  {name}"); ws.cell(row, 1).font = Font(size=10, color="7B7B7B")
        ws.cell(row, 1).border = BORDER; ws.cell(row, 1).fill = fill("F8F9FA")
        ws.cell(row, TOTAL_COL).value = (f"=IFERROR(SUMIFS('To Label (All)'!$K:$K,"
                                          f"'To Label (All)'!$E:$E,\"Personal\","
                                          f"'To Label (All)'!$F:$F,Catalog!$C{cr}),0)")
        ws.cell(row, TOTAL_COL).number_format = MONEY
        ws.cell(row, TOTAL_COL).font = Font(size=10, color="7B7B7B")
        ws.cell(row, TOTAL_COL).border = BORDER
        ws.cell(row, TOTAL_COL).alignment = align(h="right")
        ws.cell(row, TOTAL_COL).fill = fill("F8F9FA")
        personal_rows.append(row); row += 1

    if personal_rows:
        row = total_row(row, "Personal Cash Flow", personal_rows, "7B7B7B", "E9ECEF"); row += 2

    # ── FOOTNOTES ─────────────────────────────────────────────
    notes = [
        f"Notes:  • Last 13 weeks: {recent_weeks[0]} → {recent_weeks[-1]}",
        "• Income (blue): Work + net > 0  |  Expenses (red): Work + net < 0  |  Gray: Personal",
        "• To rename a category: change the name in Catalog sheet col C — all formulas update",
        "• MP Fees: fee column for all Work-classified transactions per week",
        "• Values are LIVE — label transactions in 'To Label (All)', Save, press F9 to refresh",
    ]
    for ni, note_text in enumerate(notes, row):
        ws.merge_cells(f"A{ni}:{TOTAL_COL_L}{ni}")
        c = ws.cell(ni, 1, note_text)
        c.font = Font(italic=True, size=9, color="888888")
        c.alignment = Alignment(wrap_text=True, horizontal="left")
        ws.row_dimensions[ni].height = 16

    return recent_weeks


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def run_export():
    init_db()
    conn = get_connection()

    # Build unlabeled per-account sheets
    unlabeled_a = get_unlabeled_for_export("A", limit=10000)
    unlabeled_b = get_unlabeled_for_export("B", limit=10000)

    # Build "To Label (All)" — combined, for SUMIFS P&L
    cur = conn.cursor()
    cur.execute("""
        SELECT internal_id, source_account, date, category,
               subcategory, description, gross_amount, mp_fee, net_amount,
               classification, source
        FROM ledger_final
        ORDER BY date DESC
    """)
    all_rows = cur.fetchall()
    conn.close()

    import pandas as pd
    cols = ["internal_id","source_account","date","category",
            "subcategory","description","gross_amount","mp_fee","net_amount",
            "classification","source"]
    df_all = pd.DataFrame(all_rows, columns=cols)

    # Stats
    total    = len(df_all)
    unlabeled = int((df_all["classification"].isna() | (df_all["classification"] == "")).sum())
    work      = int((df_all["classification"] == "Work").sum())
    personal  = int((df_all["classification"] == "Personal").sum())
    by_acc    = df_all.groupby("source_account").size().to_dict()

    # Labeled review
    labeled = get_labeled_for_review()

    # Output path
    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    out_path = Path(__file__).parent.parent.parent / "output" / f"v7_labeling_{ts}.xlsx"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    wb.remove(wb.active)

    ws1 = wb.create_sheet("📊 Summary")
    ws2 = wb.create_sheet("🔵 Account A — To Label")
    ws3 = wb.create_sheet("🟢 Account B — To Label")
    ws4 = wb.create_sheet("✅ All Labeled")
    ws5 = wb.create_sheet("📂 Catalog")
    ws6 = wb.create_sheet("💰 P&L")
    ws7 = wb.create_sheet("To Label (All)")

    write_summary(ws1, total, unlabeled, work, personal, by_acc)
    write_label_sheet(ws2, "A", unlabeled_a)
    write_label_sheet(ws3, "B", unlabeled_b)
    write_review_sheet(ws4, labeled)
    write_catalog_sheet(ws5)

    # Build P&L first to know column count, then build "To Label (All)"
    weeks = build_pnl_sheet(ws6, df_all)

    # Build "To Label (All)" sheet — columns for SUMIFS compatibility
    build_to_label_all_sheet(ws7, df_all)

    wb.save(out_path)
    print(f"\n✅  Exported: {out_path.name}")
    print(f"    Account A unlabeled: {len(unlabeled_a)}")
    print(f"    Account B unlabeled: {len(unlabeled_b)}")
    print(f"    Total labeled: {len(labeled)}")
    print(f"    P&L weeks: {weeks[0]} → {weeks[-1]}  ({len(weeks)} columns)")
    print(f"    To Label (All) rows: {len(df_all)}")


def build_to_label_all_sheet(ws, df):
    """Build the 'To Label (All)' sheet that P&L SUMIFS formulas read from."""
    import pandas as pd
    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A3"

    hdr_cell(ws["A1"],
             "📋 To Label (All) — Combined view for P&L SUMIFS. "
             "Label using Account A / Account B sheets for better UX.")
    ws["A1"].font = Font(bold=True, size=9, color="FFFFFF")
    ws["A1"].fill = fill("495057")
    ws["A1"].alignment = Alignment(wrap_text=True)
    ws.row_dimensions[1].height = 30
    ws.merge_cells("A1:L1")

    hdrs = ["internal_id","Acct","Date","Category","Classification",
            "Subcategory","Week","Description","Gross","Fee","Net","Interco"]
    for c, h in enumerate(hdrs, 1):
        hdr_cell(ws.cell(2, c), h)

    for ri, row_data in df.iterrows():
        xl_row = ri + 3
        date_str = str(row_data["date"])[:10] if pd.notna(row_data["date"]) else ""
        week_key = to_week_key(date_str)

        vals = [
            str(row_data["internal_id"]),
            row_data.get("source_account", "A") if pd.notna(row_data.get("source_account")) else "A",
            date_str,
            row_data["category"] if pd.notna(row_data["category"]) else "",
            row_data["classification"] if pd.notna(row_data["classification"]) else "",
            row_data["subcategory"] if pd.notna(row_data["subcategory"]) else "",
            week_key,
            row_data["description"] if pd.notna(row_data["description"]) else "",
            row_data["gross_amount"] if pd.notna(row_data["gross_amount"]) else 0,
            row_data["mp_fee"] if pd.notna(row_data["mp_fee"]) else 0,
            row_data["net_amount"] if pd.notna(row_data["net_amount"]) else 0,
            "⚠️" if row_data.get("intercompany") else "",
        ]
        for c, v in enumerate(vals, 1):
            cell = ws.cell(xl_row, c, v)
            cell.border = BORDER
            if c in (9, 10, 11) and isinstance(v, (int, float)):
                cell.number_format = '"$"#,##0.00'

    set_cols(ws, {"A": 36, "B": 6, "C": 12, "D": 22, "E": 14, "F": 24,
                  "G": 12, "H": 38, "I": 14, "J": 12, "K": 14, "L": 8})


if __name__ == "__main__":
    run_export()
