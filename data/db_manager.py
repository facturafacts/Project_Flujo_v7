"""
Database Manager for Mercado Pago Dual-Account Pipeline (v7)
Manages SQLite database with source tables + final ledger.
Each entry is tagged by source_account: 'A' or 'B'.
"""
import sqlite3
import os
from pathlib import Path

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.getenv("LEDGER_DB_PATH") or str(SCRIPT_DIR.parent / "data" / "ledger.db")


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    """Create all tables with dual-account schema."""
    conn = get_connection()
    cur = conn.cursor()

    # ── API Payments (from both accounts) ─────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_api_payments (
            internal_id       TEXT    PRIMARY KEY,   -- MP payment ID
            source_account    TEXT    NOT NULL,      -- 'A' or 'B'
            date_created      TEXT,
            date_approved     TEXT,
            operation_type    TEXT,
            payment_type_id   TEXT,
            status            TEXT,
            status_detail     TEXT,
            description       TEXT,
            transaction_amount REAL,
            net_received_amount REAL,
            fee_amount        REAL,
            payer_email       TEXT,
            payment_method_id TEXT,
            collector_id      INTEGER,
            payer_id          INTEGER,
            raw_json          TEXT
        )
    """)

    # ── Release Reports (bank movements, both accounts) ───────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_release_reports (
            source_id          TEXT    PRIMARY KEY,
            source_account     TEXT    NOT NULL,     -- 'A' or 'B'
            date               TEXT,
            description        TEXT,
            gross_amount       REAL,
            net_credit_amount  REAL,
            net_debit_amount   REAL,
            intercompany       INTEGER DEFAULT 0,     -- 1 = matched interco transfer
            counterpart_account TEXT,                 -- 'A' or 'B'
            raw_csv_row        TEXT
        )
    """)

    # ── Final Ledger ───────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ledger_final (
            internal_id        TEXT    PRIMARY KEY,   -- payment_id or source_id
            source_account     TEXT    NOT NULL,      -- 'A' or 'B'
            date               TEXT,
            category           TEXT,
            subcategory        TEXT,
            classification     TEXT,                  -- 'Work' or 'Personal'
            description        TEXT,
            gross_amount       REAL,                  -- negative for outflows
            mp_fee             REAL,
            net_amount         REAL,                  -- negative for outflows
            source             TEXT,                  -- 'api' or 'release'
            intercompany       INTEGER DEFAULT 0       -- 1 = intercompany transfer
        )
    """)

    # ── Sync Metadata ──────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            source_name   TEXT PRIMARY KEY,
            source_account TEXT,
            last_sync_ts  TEXT,
            last_sync_note TEXT,
            updated_at    TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("✅  Database initialized — dual-account schema (v7)")


# ── Payment Inserts ─────────────────────────────────────────────

def insert_api_payments_batch(rows, source_account):
    """
    rows: list of (data_dict, raw_json)
    source_account: 'A' or 'B'
    Uses INSERT OR IGNORE (composite key: internal_id + source_account).
    """
    conn = get_connection()
    cur = conn.cursor()
    for data, raw_json in rows:
        cur.execute("""
            INSERT OR IGNORE INTO source_api_payments
            (internal_id, source_account, date_created, date_approved, operation_type,
             payment_type_id, status, status_detail, description, transaction_amount,
             net_received_amount, fee_amount, payer_email, payment_method_id,
             collector_id, payer_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("id"), source_account,
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
    """
    conn = get_connection()
    cur = conn.cursor()
    for data in rows:
        cur.execute("""
            INSERT OR IGNORE INTO source_release_reports
            (source_id, source_account, date, description,
             gross_amount, net_credit_amount, net_debit_amount, raw_csv_row)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("source_id"), source_account,
            data.get("date"), data.get("description"),
            data.get("gross_amount"), data.get("net_credit_amount"),
            data.get("net_debit_amount"), data.get("raw_csv_row")
        ))
    conn.commit()
    conn.close()


# ── Sync Metadata ───────────────────────────────────────────────

def get_last_sync(source_account):
    key = f"api_{source_account}"
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
    key = f"api_{source_account}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sync_metadata
        (source_name, source_account, last_sync_ts, last_sync_note, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (key, source_account, timestamp, note))
    conn.commit()
    conn.close()


def get_last_release_sync(source_account):
    key = f"release_{source_account}"
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
    key = f"release_{source_account}"
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO sync_metadata
        (source_name, source_account, last_sync_ts, last_sync_note, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
    """, (key, source_account, timestamp, note))
    conn.commit()
    conn.close()


# ── Intercompany ─────────────────────────────────────────────────

def mark_intercompany_release(source_id, counterpart_account):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE source_release_reports
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
    """, (internal_id, source_account))
    conn.commit()
    conn.close()


# ── Ledger ───────────────────────────────────────────────────────

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
    """, (classification, subcategory, internal_id, source_account))
    conn.commit()
    conn.close()


def get_ledger_stats():
    """Return (total, unlabeled, work, personal, by_account)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*), source_account FROM ledger_final GROUP BY source_account")
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
        """, (source_account, limit))
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
               classification, description, gross_amount, mp_fee, net_amount, intercompany
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


if __name__ == "__main__":
    init_db()
