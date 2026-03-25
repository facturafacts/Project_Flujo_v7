"""
Robust pos_payment fetcher with proper batch pagination.
Fetches all pos_payment from MP API, deduplicates, inserts into DB, merges.
"""
import requests, json, time, sqlite3, sys, os
from dotenv import load_dotenv

WORKDIR = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62"
load_dotenv(dotenv_path=os.path.join(WORKDIR, ".env"))
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")
DB = os.path.join(WORKDIR, "data", "ledger.db")

url = "https://api.mercadopago.com/v1/payments/search"

def fetch_with_retry(params, max_retries=5):
    """Fetch with exponential backoff on rate limits."""
    for attempt in range(max_retries):
        r = requests.get(url, params=params)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 60 * (attempt + 1)))
            print(f"    ⏳ Rate limited. Waiting {retry_after}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(retry_after)
        else:
            print(f"    HTTP {r.status_code}: {r.text[:80]}")
            time.sleep(10)
    return None

def fetch_pos_payment(begin_date, end_date):
    """Fetch all pos_payment records for a date range using offset pagination."""
    all_records = []
    offset = 0
    limit = 1000
    
    while offset < 10000:
        params = {
            "access_token": MP_ACCESS_TOKEN,
            "begin_date": begin_date,
            "end_date": end_date,
            "offset": offset,
            "limit": limit,
            "operation_type": "pos_payment"
        }
        
        print(f"  offset={offset}...", end=" ", flush=True)
        data = fetch_with_retry(params)
        
        if data is None:
            print("FAILED after retries")
            break
        
        ops = data.get("results", [])
        total = data.get("paging", {}).get("total", 0)
        
        if not ops:
            if offset >= total and total > 0:
                print("done (offset >= total)")
            else:
                print("empty response")
            break
        
        all_records.extend(ops)
        print(f"+{len(ops)} (total in API: {total})")
        
        offset += limit
        if len(ops) < limit:
            break
        time.sleep(5)
    
    # Deduplicate
    seen = set()
    unique = []
    for r in all_records:
        if r["id"] not in seen:
            seen.add(r["id"])
            unique.append(r)
    
    return unique, len(all_records)

def main():
    sys.path.insert(0, WORKDIR)
    from data.db_manager import insert_api_payments_batch
    
    # Get existing IDs
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT internal_id FROM source_api_payments WHERE operation_type='pos_payment'")
    existing = {r[0] for r in cur.fetchall()}
    conn.close()
    print(f"Existing pos_payment in DB: {len(existing)}")
    
    # Fetch Jan + Feb + Mar separately (each < 1000 records to avoid offset cap)
    months = [
        ("2026-01-01T00:00:00.000+00:00", "2026-01-31T23:59:00.000+00:00", "Jan 2026"),
        ("2026-02-01T00:00:00.000+00:00", "2026-02-28T23:59:00.000+00:00", "Feb 2026"),
        ("2026-03-01T00:00:00.000+00:00", "2026-03-23T23:59:00.000+00:00", "Mar 2026"),
    ]
    
    grand_total_fetched = 0
    grand_total_new = 0
    
    for begin, end, label in months:
        print(f"\n[{label}] Fetching pos_payment...")
        records, fetched = fetch_pos_payment(begin, end)
        grand_total_fetched += fetched
        
        new_records = [(r, json.dumps(r)) for r in records if str(r["id"]) not in existing]
        grand_total_new += len(new_records)
        
        print(f"  → {len(records)} unique records, {len(new_records)} NEW")
        
        if new_records:
            insert_api_payments_batch(new_records)
            print(f"  ✅ Inserted {len(new_records)} new records")
        
        time.sleep(10)
    
    print(f"\n{'='*50}")
    print(f"Total fetched: {grand_total_fetched}")
    print(f"Total NEW inserted: {grand_total_new}")
    
    if grand_total_new > 0:
        # Merge to ledger
        import subprocess
        result = subprocess.run(
            ["python3", "scripts/merge_to_ledger.py"],
            cwd=WORKDIR,
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": WORKDIR}
        )
        print(result.stdout.strip() or result.stderr.strip())
        
        # Quick summary
        conn = sqlite3.connect(DB)
        cur = conn.cursor()
        print("\nPOS Sale by day (last 14 days):")
        cur.execute("""
            SELECT DATE(date) as day, COUNT(*) as txns, SUM(gross_amount) as total
            FROM ledger_final
            WHERE category='POS Sale'
            GROUP BY DATE(date)
            ORDER BY day DESC
            LIMIT 14
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]} ventas, ${row[2]:,.2f}")
        conn.close()
    else:
        print("No new records to insert.")

if __name__ == "__main__":
    main()
