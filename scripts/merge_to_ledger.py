"""
Merge source tables into final ledger.
Reconciles API Payments + Release Reports into ledger_final.
"""
import sqlite3
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.db_manager import get_connection

def merge():
    """
    Merge source_api_payments and source_release_reports into ledger_final.
    Each row tagged with source ('api' or 'release').
    """
    conn = get_connection()
    cur = conn.cursor()
    
    # Clear existing ledger_final for clean rebuild
    # (Can change to UPDATE logic if you want incremental)
    cur.execute("DELETE FROM ledger_final")
    print("🗑️ Cleared existing ledger_final")
    
    # --- MERGE: API Payments ---
    # Determine category and sign based on operation_type and collector_id
    # If collector_id IS NULL, it's a purchase (you paid, negative)
    # If collector_id = 138759157, it's a sale (you received, positive)
    cur.execute("""
        INSERT OR IGNORE INTO ledger_final (
            internal_id, date, category, subcategory, classification,
            description, gross_amount, mp_fee, net_amount, source
        )
        SELECT 
            internal_id,
            date_created as date,
            CASE
                WHEN operation_type = 'pos_payment' THEN 'POS Sale'
                WHEN operation_type = 'account_fund' THEN 'Account Funding (Deposit)'
                WHEN operation_type = 'money_transfer' THEN 'Money Transfer'
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN 'Purchase/Expense'
                WHEN operation_type = 'regular_payment' THEN 'Sale (Link/QR/Web)'
                ELSE 'Other'
            END as category,
            '' as subcategory,
            '' as classification,
            description,
            CASE
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN -ABS(transaction_amount)
                ELSE ABS(transaction_amount)
            END as gross_amount,
            CASE
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN ABS(fee_amount)
                ELSE ABS(fee_amount) * -1
            END as mp_fee,
            CASE
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN -ABS(transaction_amount)
                ELSE net_received_amount
            END as net_amount,
            'api' as source
        FROM source_api_payments
        WHERE status IN ('approved', 'authorized')
    """)
    api_count = cur.rowcount
    print(f"📥 Merged {api_count} API payment rows")
    
    # --- MERGE: Release Reports ---
    # Handle all patterns: positive gross, negative gross, credit, debit
    cur.execute("""
        INSERT OR IGNORE INTO ledger_final (
            internal_id, date, category, subcategory, classification,
            description, gross_amount, mp_fee, net_amount, source
        )
        SELECT 
            source_id as internal_id,
            date,
            CASE
                WHEN gross_amount > 0 AND (net_credit_amount > 0 OR net_debit_amount = 0) THEN 'Bank Deposit'
                WHEN gross_amount < 0 OR net_debit_amount > 0 THEN 'Bank Withdrawal'
                WHEN net_credit_amount > 0 THEN 'Bank Deposit'
                ELSE 'Bank Movement'
            END as category,
            '' as subcategory,
            '' as classification,
            description,
            ABS(COALESCE(gross_amount, 0)) as gross_amount,
            0.0 as mp_fee,
            CASE 
                WHEN gross_amount < 0 THEN gross_amount 
                ELSE COALESCE(net_credit_amount, 0) - COALESCE(net_debit_amount, 0)
            END as net_amount,
            'release' as source
        FROM source_release_reports
    """)
    release_count = cur.rowcount
    print(f"📥 Merged {release_count} release report rows")
    
    conn.commit()
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM ledger_final")
    total = cur.fetchone()[0]
    
    conn.close()
    
    print(f"\n✅ Merge complete! ledger_final has {total} rows total.")

def quick_merge():
    """
    Quick merge - only add new records without clearing.
    Uses INSERT OR IGNORE logic.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    # API Payments
    cur.execute("""
        INSERT OR IGNORE INTO ledger_final (
            internal_id, date, category, subcategory, classification,
            description, gross_amount, mp_fee, net_amount, source
        )
        SELECT 
            internal_id,
            date_created,
            CASE
                WHEN operation_type = 'pos_payment' THEN 'POS Sale'
                WHEN operation_type = 'account_fund' THEN 'Account Funding (Deposit)'
                WHEN operation_type = 'money_transfer' THEN 'Money Transfer'
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN 'Purchase/Expense'
                WHEN operation_type = 'regular_payment' THEN 'Sale (Link/QR/Web)'
                ELSE 'Other'
            END,
            '', '',
            description,
            CASE
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN -ABS(transaction_amount)
                ELSE ABS(transaction_amount)
            END,
            CASE
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN ABS(fee_amount)
                ELSE ABS(fee_amount) * -1
            END,
            CASE
                WHEN operation_type = 'regular_payment' AND (collector_id IS NULL OR collector_id = '') THEN -ABS(transaction_amount)
                ELSE net_received_amount
            END,
            'api'
        FROM source_api_payments
        WHERE status IN ('approved', 'authorized')
    """)
    api_added = cur.rowcount
    
    # Release Reports
    # Handle all patterns: positive gross, negative gross, credit, debit
    cur.execute("""
        INSERT OR IGNORE INTO ledger_final (
            internal_id, date, category, subcategory, classification,
            description, gross_amount, mp_fee, net_amount, source
        )
        SELECT 
            source_id,
            date,
            CASE
                WHEN gross_amount > 0 AND (net_credit_amount > 0 OR net_debit_amount = 0) THEN 'Bank Deposit'
                WHEN gross_amount < 0 OR net_debit_amount > 0 THEN 'Bank Withdrawal'
                WHEN net_credit_amount > 0 THEN 'Bank Deposit'
                ELSE 'Bank Movement'
            END,
            '', '',
            description,
            ABS(COALESCE(gross_amount, 0)),
            0.0,
            CASE 
                WHEN gross_amount < 0 THEN gross_amount 
                ELSE COALESCE(net_credit_amount, 0) - COALESCE(net_debit_amount, 0)
            END,
            'release'
        FROM source_release_reports
    """)
    release_added = cur.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"✅ Quick merge: +{api_added} API, +{release_added} Release")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--full':
        merge()
    else:
        quick_merge()
