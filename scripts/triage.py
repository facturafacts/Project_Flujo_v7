"""
Mercado Pago Triage System (SQLite)
Interactive CLI to classify ambiguous transactions.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import csv

# Color Codes
C_GREEN = '\033[92m'
C_RED = '\033[91m'
C_YELLOW = '\033[93m'
C_CYAN = '\033[96m'
C_RESET = '\033[0m'
C_BOLD = '\033[1m'

# Paths
DB_PATH = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62/data/ledger.db"
CATEGORIES_FILE = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62/data/categories.csv"

def load_categories():
    """Load categories from CSV into dict."""
    cat_map = {
        'Work_Inflow': [],
        'Work_Outflow': [],
        'Personal_Inflow': [],
        'Personal_Outflow': []
    }
    
    if os.path.exists(CATEGORIES_FILE):
        with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row['Context']}_{row['Direction']}"
                if key in cat_map:
                    cat_map[key].append(row['Subcategory'])
    
    # Always add 'Other' option
    for k in cat_map.keys():
        cat_map[k].append("Other")
        
    return cat_map

def print_banner():
    print(f"\n{C_CYAN}{C_BOLD}===================================================={C_RESET}")
    print(f"{C_CYAN}{C_BOLD}        💵 MERCADO PAGO TRIAGE SYSTEM (DB) 💵       {C_RESET}")
    print(f"{C_CYAN}{C_BOLD}===================================================={C_RESET}\n")

def get_list_selection(options):
    """Print numbered options and get selection."""
    for i, opt in enumerate(options, 1):
        print(f"   [{C_CYAN}{i}{C_RESET}] {opt}")
        
    while True:
        choice = input(f"\n   👉 Select a number (1-{len(options)}) > ").strip()
        try:
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(options):
                if options[choice_idx] == "Other":
                    custom = input(f"   ✏️  Please specify 'Other': ").strip()
                    return custom if custom else "Other"
                return options[choice_idx]
            else:
                print(f"   {C_RED}Invalid number. Try again.{C_RESET}")
        except ValueError:
            print(f"   {C_RED}Please enter a number.{C_RESET}")

def run_triage():
    """Main triage loop - works on SQLite ledger_final."""
    import sqlite3
    
    if not os.path.exists(DB_PATH):
        print(f"{C_RED}❌ Database not found. Run live_sync.py first.{C_RESET}")
        return

    cat_map = load_categories()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get unclassified bank transfers
    cursor.execute("""
        SELECT internal_id, date, category, description, gross_amount, mp_fee, net_amount, source
        FROM ledger_final
        WHERE (category LIKE '%Bank%' OR category LIKE '%Account Funding%')
        AND (classification IS NULL OR classification = '')
        ORDER BY date DESC
    """)
    rows = cursor.fetchall()
    
    if not rows:
        print(f"{C_GREEN}✅ All caught up! No transactions to classify.{C_RESET}\n")
        conn.close()
        return

    print_banner()
    print(f"{C_YELLOW}⚠️ Found {len(rows)} unclassified transactions.{C_RESET}\n")

    updated = 0
    for row in rows:
        internal_id, date, category, description, gross, fee, net, source = row
        is_inflow = net > 0
        
        amt_str = f"{C_GREEN}+${net:,.2f}{C_RESET}" if is_inflow else f"{C_RED}-${abs(net):,.2f}{C_RESET}"
        
        print(f"{C_BOLD}📅 Date:{C_RESET} {date}")
        print(f"{C_BOLD}🏷️  Type:{C_RESET} {category}")
        print(f"{C_BOLD}📝 Desc:{C_RESET} {description[:40]}")
        print(f"{C_BOLD}💰 Amount:{C_RESET} {amt_str}")
        print(f"{C_BOLD}📡 Source:{C_RESET} {source}")
        print("-" * 50)
        
        while True:
            ans = input(f"   👉 [{C_CYAN}W{C_RESET}]ork, [{C_CYAN}P{C_RESET}]ersonal, or [{C_YELLOW}S{C_RESET}]kip? > ").strip().upper()
            if ans in ['W', 'P', 'S']:
                break
            print(f"   {C_RED}Invalid. Just W, P, or S.{C_RESET}")
        
        if ans == 'S':
            print(f"   {C_YELLOW}⏭️  Skipped.{C_RESET}\n")
            continue
        
        classification = "Work" if ans == 'W' else "Personal"
        
        # Determine direction for category list
        direction = "Inflow" if is_inflow else "Outflow"
        key = f"{classification}_{direction}"
        
        print(f"\n   {C_BOLD}Choose Subcategory:{C_RESET}")
        sub = get_list_selection(cat_map.get(key, ["Other"]))
        
        # Update DB
        cursor.execute("""
            UPDATE ledger_final
            SET classification = ?, subcategory = ?
            WHERE internal_id = ?
        """, (classification, sub, internal_id))
        conn.commit()
        
        print(f"   {C_GREEN}✅ Saved → [{classification}] {sub}{C_RESET}\n")
        updated += 1

    conn.close()
    
    print(f"{C_CYAN}===================================================={C_RESET}")
    print(f"{C_GREEN}🎉 Triage complete! Classified {updated} transactions.{C_RESET}")
    print(f"{C_CYAN}===================================================={C_RESET}\n")

if __name__ == "__main__":
    run_triage()
