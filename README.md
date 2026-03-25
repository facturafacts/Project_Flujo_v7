# Mercado Pago Pipeline

A fully customized, local, terminal-based ERP and reconciliation system for Mercado Pago that perfectly isolates Business tracking from Personal spending.

## Architecture (3-Table SQLite)

```
┌─────────────────────┐     ┌─────────────────────┐
│  source_api_payments │     │ source_release_reports│
│  (Payments API)      │     │ (Bank Movements)      │
└──────────┬──────────┘     └──────────┬──────────┘
           │                           │
           └───────────┬───────────────┘
                       ▼
              ┌─────────────────┐
              │   ledger_final  │
              │  (Reconciled)   │
              └─────────────────┘
```

| Table | Source | Description |
|-------|--------|-------------|
| `source_api_payments` | v1/payments/search | Real-time card payments, transfers, fees |
| `source_release_reports` | v1/account/release_report | Bank withdrawals, deposits, reserves |
| `ledger_final` | Merged | Reconciled ledger with classifications |

## Key Logic

### Sign Handling (Critical)

| Transaction Type | Direction | Sign in Ledger |
|------------------|-----------|----------------|
| **Sale** (POS, QR, Link) | Money IN | **Positive** (+) |
| **Purchase** (you paid) | Money OUT | **Negative** (-) |
| **Bank Withdrawal** | Money OUT | **Negative** (-) |
| **Bank Deposit** | Money IN | **Positive** (+) |
| **Account Funding** | Money IN | **Positive** (+) |

### Purchase Detection

Purchases are detected by `collector_id IS NULL` in the API response:
- **Sale**: `collector_id = 138759157` (you received money)
- **Purchase**: `collector_id IS NULL` (you paid money)

## Quick Start

### 1. Initialize Database (First Run)
```bash
cd /home/subsc/.openclaw/workspace/workspaces/mercado-pago-pipeline
python3 -c "from data.db_manager import init_db; init_db()"
```

### 2. Sync API Payments
```bash
# Quick sync (last 7 days)
python3 scripts/live_sync.py

# Full sync from specific date
python3 scripts/live_sync.py 2026-01-01
```

This fetches in 7-day windows to handle Mercado Pago's 1000 record offset limit.

### 3. Ingest Release Reports
```bash
python3 scripts/ingest_release_reports.py
```

Downloads bank movement reports and shreds them into SQLite.

### 4. Merge to Ledger
```bash
# Quick merge (add new only)
python3 scripts/merge_to_ledger.py

# Full rebuild (clear and rebuild)
python3 scripts/merge_to_ledger.py --full
```

Combines both sources into `ledger_final` with correct sign handling.

### 5. Triage (Classify Transactions)

#### Option A: Interactive CLI
```bash
python3 scripts/triage.py
```

Interactive CLI to classify Bank Transfers as **Work** or **Personal**.

#### Option B: Excel Workflow (Recommended for Bulk)

**Export to Excel:**
```bash
python3 scripts/export_to_excel.py
```

Creates `output/to_label.xlsx` with 3 sheets:
- **Catalog** - Your Chart of Accounts (editable - add your own categories!)
- **To Label** - Transactions needing classification with dropdowns
- **P&L** - Auto-calculated Profit & Loss (updates as you label!)

**In Excel:**
1. Open `output/to_label.xlsx`
2. **Optional:** Add custom categories to the **Catalog** sheet
3. In **To Label** sheet:
   - Select `classification` from dropdown (Work/Personal)
   - Select `subcategory` from dropdown (based on your Catalog)
4. The **P&L** sheet auto-updates as you label!
5. Save the file

**Import back:**
```bash
python3 scripts/import_labels.py output/to_label.xlsx
```

### 6. View P&L Report
```bash
python3 scripts/pnl_report.py
```

## Directory Structure

```
mercado-pago-pipeline/
├── .env                           # MP_ACCESS_TOKEN
├── README.md                      # This file
├── data/
│   ├── db_manager.py             # Database connection & queries
│   ├── ledger.db                 # SQLite database
│   ├── categories.csv            # Chart of Accounts (editable)
│   ├── reports/                  # Raw release report CSVs
│   └── legacy/                   # Old CSV files (archived)
└── scripts/
    ├── live_sync.py              # Sync payments from API
    ├── ingest_release_reports.py # Ingest bank movements
    ├── merge_to_ledger.py        # Combine sources
    ├── triage.py                 # Classify transactions (CLI)
    ├── export_to_excel.py        # Export to Excel for labeling
    ├── import_labels.py          # Import labels from Excel
    └── pnl_report.py             # Generate P&L
```

## Categories

Edit `data/categories.csv` to customize your Chart of Accounts:

```csv
Context,Direction,Subcategory
Work,Inflow,Client Payment / Invoice
Work,Inflow,Retainer / Consulting
Work,Outflow,Software / Subscriptions
Work,Outflow,Contractors / Freelancers
Personal,Inflow,Wallet Top-Up (My Money)
Personal,Outflow,Owner's Draw (Paying Myself)
Personal,Outflow,Groceries / Food
```

## Data Flow

### API Payments (`live_sync.py`)
1. Paginates through v1/payments/search in 7-day windows
2. Stores raw JSON for audit trail
3. Calculates fees from `fee_details` or `net_received_amount`
4. Uses `INSERT OR IGNORE` to avoid duplicates

### Release Reports (`ingest_release_reports.py`)
1. Lists available reports from v1/account/release_report/list
2. Downloads each report CSV
3. Shreds CSV rows into SQLite
4. Handles multiple CSV formats (semicolon, comma, tab delimiters)

### Merge (`merge_to_ledger.py`)
1. **API Payments**:
   - POS payments → `POS Sale` (positive)
   - Account funding → `Account Funding` (positive)
   - Regular payment + collector_id IS NULL → `Purchase/Expense` (**negative**)
   - Regular payment + collector_id = 138759157 → `Sale` (positive)
   
2. **Release Reports**:
   - Positive gross → `Bank Deposit` (positive)
   - Negative gross → `Bank Withdrawal` (**negative**)

## Known Limitations

1. **Authorization Holds**: Temporary charges (like the $0.89 Openrouter pre-auths) may not appear in the API - they're not real transactions.

2. **Offset Limit**: Mercado Pago limits API offset to 1000 records. We handle this by fetching in 7-day windows.

3. **Duplicate IDs**: Some transaction IDs exist in both API and Release Reports. API data takes priority (more detailed).

## Troubleshooting

### No data in ledger
```bash
# Check source tables
sqlite3 data/ledger.db "SELECT COUNT(*) FROM source_api_payments;"
sqlite3 data/ledger.db "SELECT COUNT(*) FROM source_release_reports;"

# Re-merge
python3 scripts/merge_to_ledger.py --full
```

### Missing purchases (showing as sales)
Check the merge logic - purchases are detected by `collector_id IS NULL`. If MP changes their API, this may need adjustment.

### Wrong sign on amounts
Run a full merge to rebuild the ledger:
```bash
python3 scripts/merge_to_ledger.py --full
```

## Database Schema

```sql
-- API Payments (raw data)
CREATE TABLE source_api_payments (
    internal_id TEXT PRIMARY KEY,
    date_created TEXT,
    operation_type TEXT,
    status TEXT,
    description TEXT,
    transaction_amount REAL,
    net_received_amount REAL,
    fee_amount REAL,
    collector_id INTEGER,  -- NULL = purchase, 138759157 = sale
    payer_id INTEGER,
    raw_json TEXT
);

-- Release Reports (bank movements)
CREATE TABLE source_release_reports (
    source_id TEXT PRIMARY KEY,
    date TEXT,
    description TEXT,
    gross_amount REAL,      -- Can be negative
    net_credit_amount REAL,
    net_debit_amount REAL,
    raw_csv_row TEXT
);

-- Final Ledger (reconciled)
CREATE TABLE ledger_final (
    internal_id TEXT PRIMARY KEY,
    date TEXT,
    category TEXT,
    subcategory TEXT,
    classification TEXT,    -- Work/Personal
    description TEXT,
    gross_amount REAL,      -- Negative for outflows
    mp_fee REAL,
    net_amount REAL,        -- Negative for outflows
    source TEXT             -- 'api' or 'release'
);
```

## Development

### Adding a new script
Place new scripts in `scripts/` and add the sys.path insert at the top:

```python
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.db_manager import get_connection
```

### Querying the database
```bash
sqlite3 data/ledger.db "SELECT * FROM ledger_final LIMIT 10;"
```

## License

Private use only.
