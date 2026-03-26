"""
Database Manager for Mercado Pago Dual-Account Pipeline (v7)
Manages SQLite database with PER-ACCOUNT source tables + final ledger.

Tables:
  source_api_payments_A       — Account A payments (from MP API)
  source_api_payments_B       — Account B payments (from MP API)
  source_release_reports_A   — Account A bank movements (from release CSV)
  source_release_reports_B    — Account B bank movements (from release CSV)
  ledger_final               — Reconciled ledger (tagged by source_account)
  sync_metadata             — Sync timestamps per account
"""
import sqlite3
import os
from pathlib import Path

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("LEDGER_DB_PATH") or str(SCRIPT_DIR.parent / "data" / "ledger.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


# ── Schema: per-account source tables ────────────────────────

API_PAYMENTS_SCHEMA_A = """
    internal_id           TEXT    PRIMARY KEY,
    date_created         TEXT,
    date_approved        TEXT,
    operation_type       TEXT,
    payment_type_id      TEXT,
    status               TEXT,
    status_detail        TEXT,
    description          TEXT,
    transaction_amount   REAL,
    net_received_amount  REAL,
    fee_amount           REAL,
    payer_email          TEXT,
    payment_method_id    TEXT,
    collector_id         INTEGER,
    payer_id             INTEGER,
    raw_json             TEXT
"""
# Same schema for _B table
API_PAYMENTS_SCHEMA_B = API_PAYMENTS_SCHEMA_A

RELEASE_SCHEMA_A = """
    source_id             TEXT    PRIMARY KEY,
    date                  TEXT,
    description           TEXT,
    gross_amount          REAL,
    net_credit_amount     REAL,
    net_debit_amount       REAL,
    intercompany          INTEGER DEFAULT 0,
    counterpart_account   TEXT,
    raw_csv_row           TEXT
"""
# Same schema for _B table
RELEASE_SCHEMA_B = RELEASE_SCHEMA_A


def _create_table(cur, name, schema):
    cur.execute(f"CREATE TABLE IF NOT EXISTS {name} ({schema})")


def init_db():
    """Create all tables with per-account schema (v7)."""
    conn = get_connection()
    cur = conn.cursor()

    _create_table(cur, "source_api_payments_A",      API_PAYMENTS_SCHEMA_A)
    _create_table(cur, "source_api_payments_B",      API_PAYMENTS_SCHEMA_B)
    _create_table(cur, "source_release_reports_A",  RELEASE_SCHEMA_A)
    _create_table(cur, "source_release_reports_B",  RELEASE_SCHEMA_B)

    # ── Final Ledger (unchanged — keeps source_account tag) ────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ledger_final (
            internal_id        TEXT    PRIMARY KEY,
            source_account     TEXT    NOT NULL,
            date               TEXT,
            category           TEXT,
            subcategory        TEXT,
            classification     TEXT,
            description        TEXT,
            gross_amount       REAL,
            mp_fee             REAL,
            net_amount         REAL,
            source             TEXT,
            intercompany       INTEGER DEFAULT 0
        )
    """)

    # ── Sync Metadata ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            source_name    TEXT PRIMARY KEY,
            source_account TEXT,
            last_sync_ts   TEXT,
            last_sync_note TEXT,
            updated_at     TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("✅  Database initialized — per-account schema (v7)")


# ── Table name helpers ─────────────────────────────────────────

def _api_table(account):
    return f"source_api_payments_{account.upper()}"


def _release_table(account):
    return f"source_release_reports_{account.upper()}"


# ── Payment Inserts ────────────────────────────────────────────

def insert_api_payments_batch(rows, source_account):
    """
    rows: list of (data_dict, raw_json)
    source_account: 'A' or 'B'
    Writes to source_api_payments_{A|B} using INSERT OR IGNORE on internal_id.
    """
    tbl = _api_table(source_account)
    conn = get_connection()
    cur = conn.cursor()
    for data, raw_json in rows:
        cur.execute(f"""
            INSERT OR IGNORE INTO {tbl}
            (internal_id, date_created, date_approved, operation_type,
             payment_type_id, status, status_detail, description,
             transaction_amount, net_received_amount, fee_amount,
             payer_email, payment_method_id, collector_id, payer_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("id"),
            data.get("date_created"), data.get("date_approved"),
            data.get("operation_type"), data.get("payment_type_id"),
            data.get("status"), data.get("status_detail"),
            data.get("description"),
            data.get("transaction_amount"), data.get("net_received_amount"),
            data.get("fee_amount"), data.get("payer_email"),
            data.get("payment_method_id"), data.get("collector_id"),
            data.get("payer_id"), raw_json
        ))
    conn.commit()
    conn.close()


def insert_release_reports_batch(rows, source_account):
    """
    rows: list of dicts with keys: source_id, date, description,
          gross_amount, net_credit_amount, net_debit_amount, raw_csv_row
    Writes to source_release_reports_{A|B}.
    """
    tbl = _release_table(source_account)
    conn = get_connection()
    cur = conn.cursor()
    for data in rows:
        cur.execute(f"""
            INSERT OR IGNORE INTO {tbl}
            (source_id, date, description, gross_amount,
             net_credit_amount, net_debit_amount, raw_csv_row)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("source_id"),
            data.get("date"), data.get("description"),
            data.get("gross_amount"), data.get("net_credit_amount"),
            data.get("net_debit_amount"), data.get("raw_csv_row")
        ))
    conn.commit()
    conn.close()


# ── Sync Metadata ──────────────────────────────────────────────

def get_last_sync(source_account):
    key = f"api_{source_account.upper()}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT last_sync_ts FROM sync_metadata WHERE source_name = ?",
        (key,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_last_sync(source_account, timestamp, note):
    key = f"api_{source_account.upper()}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sync_metadata
        (source_name, source_account, last_sync_ts, last_sync_note, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (key, source_account.upper(), timestamp, note))
    conn.commit()
    conn.close()


def get_last_release_sync(source_account):
    key = f"release_{source_account.upper()}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT last_sync_ts FROM sync_metadata WHERE source_name = ?",
        (key,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_last_release_sync(source_account, timestamp, note):
    key = f"release_{source_account.upper()}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sync_metadata
        (source_name, source_account, last_sync_ts, last_sync_note, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (key, source_account.upper(), timestamp, note))
    conn.commit()
    conn.close()


# ── Intercompany ───────────────────────────────────────────────

def mark_intercompany_release(source_id, source_account, counterpart_account):
    """Mark a release report row as intercompany."""
    tbl = _release_table(source_account)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE {tbl}
        SET intercompany = 1, counterpart_account = ?
        WHERE source_id = ?
    """, (counterpart_account, source_id))
    conn.commit()
    conn.close()


def mark_intercompany_ledger(internal_id, source_account):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE ledger_final
        SET intercompany = 1
        WHERE internal_id = ? AND source_account = ?
    """, (internal_id, source_account.upper()))
    conn.commit()
    conn.close()


# ── Ledger ─────────────────────────────────────────────────────

def upsert_ledger(row):
    """Insert or replace a row into ledger_final."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO ledger_final
        (internal_id, source_account, date, category, subcategory,
         classification, description, gross_amount, mp_fee, net_amount,
         source, intercompany)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)
    conn.commit()
    conn.close()


def update_classification(internal_id, source_account, classification, subcategory):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE ledger_final
        SET classification = ?, subcategory = ?
        WHERE internal_id = ? AND source_account = ?
    """, (classification, subcategory, internal_id, source_account.upper()))
    conn.commit()
    conn.close()


def get_ledger_stats():
    """Return (total, unlabeled, work, personal, by_account)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*), source_account FROM ledger_final GROUP BY source_account"
    )
    by_acc = dict(cur.fetchall())
    cur.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN classification IS NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN classification = 'Work' THEN 1 ELSE 0 END),
               SUM(CASE WHEN classification = 'Personal' THEN 1 ELSE 0 END)
        FROM ledger_final
    """)
    row = cur.fetchone()
    conn.close()
    return row[0], row[1], row[2], row[3], by_acc


def get_unlabeled_for_export(source_account=None, limit=500):
    """Return unlabeled rows, optionally filtered by account."""
    conn = get_connection()
    cur = conn.cursor()
    if source_account:
        cur.execute("""
            SELECT internal_id, source_account, date, category,
                   description, gross_amount, mp_fee, net_amount, intercompany
            FROM ledger_final
            WHERE classification IS NULL AND source_account = ?
            ORDER BY date DESC LIMIT ?
        """, (source_account.upper(), limit))
    else:
        cur.execute("""
            SELECT internal_id, source_account, date, category,
                   description, gross_amount, mp_fee, net_amount, intercompany
            FROM ledger_final
            WHERE classification IS NULL
            ORDER BY date DESC LIMIT ?
        """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_labeled_for_review():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT internal_id, source_account, date, category, subcategory,
               classification, description, gross_amount, mp_fee,
               net_amount, intercompany
        FROM ledger_final
        WHERE classification IS NOT NULL
        ORDER BY date DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def get_pnl_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT classification, subcategory, category,
               gross_amount, mp_fee, source_account
        FROM ledger_final
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


# ── Per-account query helpers (used by merge + intercompany) ──

def get_all_api_rows(source_account):
    """Return all API payment rows for an account."""
    tbl = _api_table(source_account)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT internal_id, date_created, operation_type, description,
               transaction_amount, fee_amount, net_received_amount,
               collector_id, source_account
        FROM {tbl}
    """, )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_all_release_rows(source_account):
    """Return all release report rows for an account."""
    tbl = _release_table(source_account)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT source_id, date, description, gross_amount,
               net_credit_amount, net_debit_amount, source_account
        FROM {tbl}
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def get_api_row_count(source_account):
    tbl = _api_table(source_account)
    conn = get_connection()
    n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    conn.close()
    return n


def get_release_row_count(source_account):
    tbl = _release_table(source_account)
    conn = get_connection()
    n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    conn.close()
    return n


if __name__ == "__main__":
    init_db()
