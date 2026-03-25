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

## Database Schema Changes

### `source_api_payments`
```sql
ALTER TABLE source_api_payments ADD COLUMN source_account TEXT; -- 'A' or 'B'
```

### `source_release_reports`
```sql
ALTER TABLE source_release_reports ADD COLUMN source_account TEXT;  -- 'A' or 'B'
ALTER TABLE source_release_reports ADD COLUMN intercompany INTEGER DEFAULT 0;
ALTER TABLE source_release_reports ADD COLUMN counterpart_account TEXT;  -- 'A' or 'B'
```

### `ledger_final`
```sql
ALTER TABLE ledger_final ADD COLUMN source_account TEXT;
ALTER TABLE ledger_final ADD COLUMN intercompany INTEGER DEFAULT 0;
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

## Commits Before Code

- [x] SPEC.md written
- [ ] `.env` updated with `MP_ACCESS_TOKEN_B`
- [ ] Architecture refactored (new script structure)
- [ ] Database migration (add source_account columns)
- [ ] Multi-account sync scripts
- [ ] Intercompany detection
- [ ] Excel export/import (new UX)
- [ ] Tested end-to-end with real tokens

---

## Open Questions

1. **Release reports for Account B** — does Account B have a linked bank account that produces release reports too? Or only Account A?
2. **Account A release reports** — do they come from the same bank account as v6 ( BBVA / SPM )?
3. **Intercompany transfer amount** — is it the full gross amount, or net of fees?
4. **When do we start syncing Account B?** — today, or from a specific date?

---

_Last updated: 2026-03-25_
