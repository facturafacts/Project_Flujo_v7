"""
Mercado Pago P&L Report (SQLite)
Generates Profit & Loss from ledger_final in database.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlite3

# Color Codes
C_GREEN = '\033[92m'
C_RED = '\033[91m'
C_BOLD = '\033[1m'
C_RESET = '\033[0m'
C_CYAN = '\033[96m'
C_YELLOW = '\033[93m'

DB_PATH = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62/data/ledger.db"

def generate_pnl():
    """Generate P&L from SQLite ledger_final."""
    
    if not __import__('os').path.exists(DB_PATH):
        print(f"{C_RED}❌ Database not found. Run live_sync.py and merge_to_ledger.py first.{C_RESET}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all ledger data
    cursor.execute("""
        SELECT category, subcategory, classification, gross_amount, mp_fee, net_amount, source
        FROM ledger_final
    """)
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print(f"{C_YELLOW}⚠️ No data in ledger. Run sync first.{C_RESET}")
        return

    # Initialize P&L buckets
    revenue = 0.0
    yield_income = 0.0
    expenses_by_sub = {}
    total_expenses = 0.0
    mp_fees = 0.0
    personal_draws = 0.0
    capital_injections = 0.0

    for row in rows:
        category, subcategory, classification, gross, fee, net, source = row
        
        # Handle None values
        if gross is None:
            gross = 0.0
        if fee is None:
            fee = 0.0
        if net is None:
            net = 0.0
            
        gross = float(gross)
        fee = float(fee)
        net = float(net)
        
        # Track fees
        mp_fees += abs(fee)
        
        # Skip if classified as Personal
        if classification == "Personal":
            if gross > 0:
                capital_injections += gross
            else:
                personal_draws += abs(gross)
            continue
        
        # Bank movements = Equity/Cash Flow (not P&L)
        if category and ("Bank" in category or "Funding" in category):
            if gross > 0:
                capital_injections += gross
            else:
                personal_draws += abs(gross)
            continue

        # --- P&L Items ---
        
        # Sales Revenue
        if "Sale" in str(category):
            revenue += gross
        
        # Interest/Yield
        elif "Yield" in str(category) or "Interest" in str(category) or "Ganancias" in str(category):
            yield_income += gross
        
        # Expenses
        elif "Purchase" in str(category) or "Expense" in str(category):
            sub = subcategory if subcategory else "Uncategorized"
            expenses_by_sub[sub] = expenses_by_sub.get(sub, 0.0) + abs(gross)
            total_expenses += abs(gross)

    # Calculate totals
    total_income = revenue + yield_income
    total_all_expenses = total_expenses + mp_fees
    net_profit = total_income - total_all_expenses

    # Print Report
    print(f"\n{C_CYAN}{C_BOLD}=================================================={C_RESET}")
    print(f"{C_CYAN}{C_BOLD}           📊 PROFIT & LOSS STATEMENT 📊          {C_RESET}")
    print(f"{C_CYAN}{C_BOLD}=================================================={C_RESET}\n")
    
    print(f"{C_BOLD}INCOME{C_RESET}")
    print(f"  Sales & Revenue:               ${revenue:,.2f}")
    if yield_income > 0:
        print(f"  Interest Yield (Ganancias):    ${yield_income:,.2f}")
    print(f"{C_GREEN}  Total Income:                  ${total_income:,.2f}{C_RESET}\n")
    
    print(f"{C_BOLD}EXPENSES{C_RESET}")
    if expenses_by_sub:
        for k, v in sorted(expenses_by_sub.items(), key=lambda x: x[1], reverse=True):
            print(f"  {k:<30} ${v:,.2f}")
    else:
        print(f"  (No expenses categorized)")
    
    if mp_fees > 0:
        print(f"  Mercado Pago Fees:             ${mp_fees:,.2f}")
        
    print(f"{C_RED}  Total Expenses:                ${total_all_expenses:,.2f}{C_RESET}\n")
    
    print(f"{C_CYAN}--------------------------------------------------{C_RESET}")
    
    if net_profit >= 0:
        print(f"{C_BOLD}NET PROFIT:                      {C_GREEN}+${net_profit:,.2f}{C_RESET}")
    else:
        print(f"{C_BOLD}NET LOSS:                        {C_RED}-${abs(net_profit):,.2f}{C_RESET}")
        
    print(f"{C_CYAN}=================================================={C_RESET}")
    
    # Cash Flow Note
    print(f"\n{C_YELLOW}💡 Cash Flow (Not P&L):{C_RESET}")
    print(f"  Owner Draws / Personal Use:    -${personal_draws:,.2f}")
    print(f"  Capital Injections:           +${capital_injections:,.2f}\n")

if __name__ == "__main__":
    generate_pnl()
