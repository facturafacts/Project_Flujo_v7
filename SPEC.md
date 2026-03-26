# SPEC.md — Project_Flujo_v7

**Purpose:** Mercado Pago dual-account reconciliation pipeline
**Started:** 2026-03-25
**Based on:** Project_Flujo_v6 (https://github.com/facturafacts/Project_Flujo_v6)

---

## Why V7 Exists

V6 works for a single Mercado Pago account. V7 adds:
1. Multi-account support (Account A + Account B)
2. Source-account tagging on every entry
3. Intercompany transfer detection (B→A)
4. Improved labeling UX
5. Cleaner, modular architecture

---

## Definitions

### Accounts

| Name | Role | Token Key | Description |
|---|---|---|---|
| **Account A** | Expense / Legacy | `MP_ACCESS_TOKEN_A` | Historical transactions, now expense side |
| **Account B** | Concentrator / New | `MP_ACCESS_TOKEN_B` | All new POS sales flow here |

### Source Identifiers

Every ledger entry gets tagged with `source_account`:
- `A` — from Account A API
- `B` — from Account B API
- `A_release` — from Account A release report (bank movements)
- `B_release` — from Account B release report (bank movements)

---

## Data Flow

```
Account A API  ─────┐
                    │
Account B API  ─────┼──► source_api_payments_A/B
                    │         │
Release A CSV   ─────┤         │
                    │         ▼
Release B CSV  ─────┘   source_release_reports_A/B
                              │
                              ▼
                         ledger_final
                         (with source_account tag)
```

### Intercompany Detection

When Account B sends money to Account A:
- Appears in Account B API as an outbound transfer
- Appears in Account A release report as a Money Transfer

**Detection rule:** Match by `description` + `gross_amount` + date across accounts.
- A release report entry that matches a B API outbound transfer = **Intercompany**
- Both sides get tagged: `intercompany=True`, `counterpart_account=B`

---

## Database Schema (v7 — per-account source tables)

### `source_api_payments_A` / `source_api_payments_B`
```sql
-- Primary key: internal_id (MP payment ID)
-- No source_account column — table name IS the account
internal_id           TEXT PRIMARY KEY,
date_created          TEXT,
date_approved         TEXT,
operation_type        TEXT,
payment_type_id       TEXT,
status                TEXT,
status_detail         TEXT,
description           TEXT,
transaction_amount    REAL,
net_received_amount   REAL,
fee_amount            REAL,
payer_email           TEXT,
payment_method_id     TEXT,
collector_id          INTEGER,   -- NULL = purchase, set = sale
payer_id              INTEGER,
raw_json              TEXT
```

### `source_release_reports_A` / `source_release_reports_B`
```sql
-- Primary key: source_id (release report row ID)
-- No source_account column — table name IS the account
source_id             TEXT PRIMARY KEY,
date                  TEXT,
description           TEXT,
gross_amount          REAL,       -- negative for outflows
net_credit_amount     REAL,
net_debit_amount      REAL,
intercompany          INTEGER DEFAULT 0,
counterpart_account   TEXT,       -- set when matched as intercompany
raw_csv_row           TEXT
```

### `ledger_final`
```sql
-- Keeps source_account column to identify origin
internal_id        TEXT PRIMARY KEY,
source_account     TEXT NOT NULL,   -- 'A' or 'B'
date               TEXT,
category           TEXT,
subcategory        TEXT,
classification     TEXT,            -- 'Work' or 'Personal'
description        TEXT,
gross_amount       REAL,            -- negative for outflows
mp_fee             REAL,
net_amount         REAL,            -- negative for outflows
source             TEXT,            -- 'api' or 'release'
intercompany       INTEGER DEFAULT 0
```

---

## Excel Labeling UX Improvements

### Problems with v6
- Single flat list of all transactions — no grouping
- No visibility into which account an entry came from
- Classification done in one pass, no review step
- No summary of what was labeled in the session

### v7 Excel Layout

**Sheet 1: Summary**
- Quick stats: total unlabeled, labeled Work, labeled Personal
- Breakdown by account (A vs B unlabeled count)
- Date range of unlabeled entries

**Sheet 2: Account A — Unlabeled**
- Only Account A entries still needing labels
- Same dropdown UX (classification + subcategory)

**Sheet 3: Account B — Unlabeled**
- Only Account B entries
- Intercompany transfers highlighted (already pre-tagged)

**Sheet 4: All Labeled (Review)**
- Read-only view of everything labeled
- Can un-label by clearing the row

**Sheet 5: Catalog**
- Chart of accounts (same as v6, editable)

**Sheet 6: P&L**
- Auto-calculated Work vs Personal summary
- Breakdown by account (A vs B revenue/expenses)

---

## Script Architecture (Refactor)

```
scripts/
├── sync/
│   ├── sync_account_a.py    # Pull from Account A API
│   ├── sync_account_b.py    # Pull from Account B API
│   └── ingest_releases.py   # Ingest release CSVs for both accounts
├── ledger/
│   ├── merge.py             # Merge all sources → ledger_final
│   └── intercompany.py      # Detect and tag intercompany transfers
├── excel/
│   ├── export.py            # Export labeled/unlabeled to Excel (multi-sheet)
│   └── import.py            # Import labels from Excel back to ledger
└── reports/
    ├── pnl.py               # P&L report
    └── summary.py           # Quick summary stats
```

**Core principle:** Each script does one thing. No mega-scripts.

---

## Env / Config

`.env` structure:
```env
MP_ACCESS_TOKEN_A=<Account A token>
MP_ACCESS_TOKEN_B=<Account B token>
LEDGER_DB_PATH=../data/ledger.db
```

---

## UX Principles

1. **Never show everything at once** — filter by account first
2. **Intercompany should be visually distinct** — highlighted row, pre-checked
3. **Small batches** — export only unlabeled, max 500 rows per sheet
4. **Session summary** — show what was labeled before closing Excel
5. **No data loss** — import only updates classification/subcategory, never deletes

---

## Full Workflow (v7 — Dual Account)

```
# ── 1. Sync both account APIs ──────────────────────────────────
python scripts/sync/sync_account_a.py          # Account A — incremental
python scripts/sync/sync_account_b.py          # Account B — incremental
python scripts/sync/sync_account_b.py --full    # Account B — full backfill (first run)

# ── 2. Ingest release reports ─────────────────────────────────
python scripts/sync/ingest_releases.py          # Both accounts
python scripts/sync/ingest_releases.py A       # Account A only
python scripts/sync/ingest_releases.py B       # Account B only

# ── 3. Merge to ledger ─────────────────────────────────────────
python scripts/ledger/merge.py                  # Incremental (new only)
python scripts/ledger/merge.py --full          # Full rebuild

# ── 4. Detect intercompany transfers ─────────────────────────
python scripts/ledger/intercompany.py           # Dry run (review first!)
python scripts/ledger/intercompany.py --apply   # Apply matches to DB

# ── 5. Export to Excel ─────────────────────────────────────────
python scripts/excel/export.py                  # Creates output/v7_labeling_YYYYMMDD_HHMM.xlsx

# ── 6. Import labels ───────────────────────────────────────────
python scripts/excel/import.py                  # Import from most recent export
python scripts/excel/import.py output/v7_labeling_xxx.xlsx  # Specific file

# ── Quick status ───────────────────────────────────────────────
python scripts/reports/summary.py
```


---

## Environment Setup

`.env` (at root of Project_Flujo_v7/):
```env
MP_ACCESS_TOKEN_A=REPLACE_WITH_ACCOUNT_A_TOKEN   # Expense / Legacy account
MP_ACCESS_TOKEN_B=REPLACE_WITH_ACCOUNT_B_TOKEN   # Concentrator / New account
LEDGER_DB_PATH=./data/ledger.db
```

To add a new token: edit `.env` and replace the value. No code changes needed.

---

## Intercompany Rule

**Rule:** When Account B sends money to Account A:
1. Account **B API** shows an outbound `money_transfer` (net_received < 0)
2. Account **A release report** shows a matching deposit on the **same date**
3. Match on: `|net_B| ≈ net_credit_A` within **$1.00 tolerance**
4. If the same transfer appears in **both B API and A release report** → use API data (more detail), skip/dedupe the release row

**Validation:** The script runs in DRY RUN mode first — shows matches, you review, then pass `--apply`.

```
# Dry run (review first)
python scripts/ledger/intercompany.py

# Apply if matches look correct
python scripts/ledger/intercompany.py --apply
```

---

## Open Questions

1. ✅ Release reports for both accounts — confirmed yes
2. ✅ Both accounts use same bank account for releases — confirmed
3. ✅ Net amount matching with $1.00 tolerance — confirmed
4. ✅ Account B sync start date — today (no backfill needed for B)

---

## Build Status

- [x] SPEC.md written
- [x] `.env` with both token slots (Account A + Account B)
- [x] Architecture refactored — `sync/`, `ledger/`, `excel/`, `reports/`
- [x] `db_manager.py` — per-account schema (separate tables per account)
- [x] Migration: flat → per-account tables ✅ (2026-03-26)
- [x] Multi-account sync scripts (`sync_account_a.py`, `sync_account_b.py`, `ingest_releases.py`)
- [x] Merge script (`merge.py`) — reads from per-account tables
- [x] Intercompany detection (`intercompany.py`) — queries per-account tables
- [x] Excel export (`export.py`) — 6-sheet UX with account-split sheets
- [x] Excel import (`import.py`)
- [x] Tested end-to-end with real tokens ✅

---

_Last updated: 2026-03-26_
