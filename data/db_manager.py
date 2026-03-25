"""
Database Manager for Mercado Pago Ledger
Manages SQLite database with shredded source tables + final ledger.
"""
import sqlite3
import os

DB_PATH = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62/data/ledger.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

def init_db():
    """Create the 3-table architecture."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_api_payments (
            internal_id TEXT PRIMARY KEY,
            date_created TEXT,
            date_approved TEXT,
            operation_type TEXT,
            payment_type_id TEXT,
            status TEXT,
            status_detail TEXT,
            description TEXT,
            transaction_amount REAL,
            net_received_amount REAL,
            fee_amount REAL,
            payer_email TEXT,
            payment_method_id TEXT,
            collector_id INTEGER,
            payer_id INTEGER,
            raw_json TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS source_release_reports (
            source_id TEXT PRIMARY KEY,
            date TEXT,
            description TEXT,
            gross_amount REAL,
            net_credit_amount REAL,
            net_debit_amount REAL,
            raw_csv_row TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ledger_final (
            internal_id TEXT PRIMARY KEY,
            date TEXT,
            category TEXT,
            subcategory TEXT,
            classification TEXT,
            description TEXT,
            gross_amount REAL,
            mp_fee REAL,
            net_amount REAL,
            source TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            source_name TEXT PRIMARY KEY,
            last_sync_ts TEXT,
            last_sync_note TEXT,
            updated_at TEXT
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ Database initialized with 3-table architecture.")

def insert_api_payments_batch(batch):
    """
    Insert a list of (data_dict, raw_json) tuples into source_api_payments.
    Uses INSERT OR IGNORE to avoid duplicates.
    """
    conn = get_connection()
    cursor = conn.cursor()
    for data, raw_json in batch:
        cursor.execute("""
            INSERT OR IGNORE INTO source_api_payments
            (internal_id, date_created, date_approved, operation_type, payment_type_id,
             status, status_detail, description, transaction_amount,
             net_received_amount, fee_amount, payer_email, payment_method_id,
             collector_id, payer_id, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('id'),
            data.get('date_created'),
            data.get('date_approved'),
            data.get('operation_type'),
            data.get('payment_type_id'),
            data.get('status'),
            data.get('status_detail'),
            data.get('description'),
            data.get('transaction_amount'),
            data.get('net_received_amount'),
            data.get('fee_amount'),
            data.get('payer_email'),
            data.get('payment_method_id'),
            data.get('collector_id'),
            data.get('payer_id'),
            raw_json
        ))
    conn.commit()
    conn.close()

def insert_release_report_batch(batch):
    """
    Insert a list of (source_id, date, description, gross, credit, debit, raw_json) tuples.
    """
    conn = get_connection()
    cursor = conn.cursor()
    for data in batch:
        cursor.execute("""
            INSERT OR IGNORE INTO source_release_reports
            (source_id, date, description, gross_amount, net_credit_amount, net_debit_amount, raw_csv_row)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get('source_id'),
            data.get('date'),
            data.get('description'),
            data.get('gross_amount'),
            data.get('net_credit_amount'),
            data.get('net_debit_amount'),
            data.get('raw_csv_row')
        ))
    conn.commit()
    conn.close()

def get_last_sync(source_name):
    """Return the last sync timestamp for a source, or None."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT last_sync_ts FROM sync_metadata WHERE source_name = ?", (source_name,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None

def set_last_sync(source_name, timestamp, note):
    """Upsert last sync timestamp for a source."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO sync_metadata (source_name, last_sync_ts, last_sync_note, updated_at)
        VALUES (?, ?, ?, datetime('now'))
    """, (source_name, timestamp, note))
    conn.commit()
    conn.close()

def get_ledger_for_triage():
    """Get unclassified bank transfers for triage."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT internal_id, date, category, description, gross_amount, mp_fee, net_amount, source
        FROM ledger_final
        WHERE (category LIKE '%Bank Withdrawal%' OR category LIKE '%Account Funding%' OR category LIKE '%Release Report%')
        AND (classification IS NULL OR classification = '')
        ORDER BY date DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

def update_triage(internal_id, classification, subcategory):
    """Update classification and subcategory for a ledger row."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE ledger_final
        SET classification = ?, subcategory = ?
        WHERE internal_id = ?
    """, (classification, subcategory, internal_id))
    conn.commit()
    conn.close()

def get_pnl_data():
    """Get all ledger data for P&L report."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT category, subcategory, classification, gross_amount, mp_fee, net_amount, source
        FROM ledger_final
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    init_db()
