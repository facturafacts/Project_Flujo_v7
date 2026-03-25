"""
Import labels from Excel back to SQLite.
Reads the labeled data and updates ledger_final classification.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sqlite3
import pandas as pd
from data.db_manager import get_connection

def import_from_excel(excel_path):
    """Import classifications from Excel to SQLite."""
    
    if not os.path.exists(excel_path):
        print(f"❌ File not found: {excel_path}")
        return
    
    print(f"📖 Reading: {excel_path}")
    
    # Read the To Label sheet
    try:
        df = pd.read_excel(excel_path, sheet_name='To Label')
    except Exception as e:
        print(f"❌ Error reading Excel: {e}")
        return
    
    # Filter rows that have been labeled
    labeled = df[(df['classification'].notna()) & (df['classification'] != '')]
    
    if len(labeled) == 0:
        print("⚠️ No labeled transactions found.")
        return
    
    print(f"📝 Found {len(labeled)} labeled transactions")
    
    # Connect to database
    conn = get_connection()
    cursor = conn.cursor()
    
    updated = 0
    skipped = 0
    
    for _, row in labeled.iterrows():
        internal_id = str(row['internal_id'])
        classification = str(row['classification']).strip()
        subcategory = str(row['subcategory']).strip() if pd.notna(row['subcategory']) else ''
        
        # Validate classification
        if classification not in ['Work', 'Personal']:
            print(f"   ⚠️ Skipping {internal_id}: invalid classification '{classification}'")
            skipped += 1
            continue
        
        # Update database
        cursor.execute("""
            UPDATE ledger_final
            SET classification = ?, subcategory = ?
            WHERE internal_id = ?
        """, (classification, subcategory, internal_id))
        
        if cursor.rowcount > 0:
            updated += 1
        else:
            print(f"   ⚠️ ID not found: {internal_id}")
            skipped += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Import complete!")
    print(f"   Updated: {updated}")
    print(f"   Skipped: {skipped}")
    
    # Show P&L summary
    print("\n📊 Updated P&L Preview:")
    show_pnl_preview()

def show_pnl_preview():
    """Show a quick P&L preview after import."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            classification,
            SUM(CASE WHEN net_amount > 0 THEN net_amount ELSE 0 END) as inflow,
            SUM(CASE WHEN net_amount < 0 THEN net_amount ELSE 0 END) as outflow
        FROM ledger_final
        WHERE classification != ''
        GROUP BY classification
    """)
    
    results = cursor.fetchall()
    conn.close()
    
    for classification, inflow, outflow in results:
        net = inflow + outflow
        print(f"   {classification}: ${net:,.2f}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        excel_path = sys.argv[1]
    else:
        excel_path = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62/output/to_label.xlsx"
    
    import_from_excel(excel_path)
