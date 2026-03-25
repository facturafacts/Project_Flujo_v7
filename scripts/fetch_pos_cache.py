"""
Step 1: Fetch ALL pos_payment from MP API and save to disk.
Batches: 1000 records per call (API hard limit), sleep 5s between calls.
"""
import requests, json, time, os, sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

WORKDIR = "/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62"
CACHE_FILE = os.path.join(WORKDIR, "data", "mp_pos_cache.json")

load_dotenv(dotenv_path=os.path.join(WORKDIR, ".env"))
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")

url = "https://api.mercadopago.com/v1/payments/search"
all_records = []
offset = 0
limit = 1000

print(f"Fetching ALL pos_payment records from MP API...")
print(f"Caching to: {CACHE_FILE}")

while offset < 10000:  # safety cap
    params = {
        "access_token": MP_ACCESS_TOKEN,
        "begin_date": "2026-01-01T00:00:00.000+00:00",
        "end_date": "2026-03-23T23:59:00.000+00:00",
        "offset": offset,
        "limit": limit,
        "operation_type": "pos_payment"
    }
    
    response = requests.get(url, params=params)
    print(f"  offset={offset}: status={response.status_code}", end="")
    
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f" → Rate limited. Waiting {retry_after}s...")
        time.sleep(retry_after)
        continue
    
    if response.status_code != 200:
        print(f" → HTTP {response.status_code}: {response.text[:100]}")
        break
    
    data = response.json()
    ops = data.get("results", [])
    total = data.get("paging", {}).get("total", 0)
    
    print(f" → {len(ops)} fetched (total in API: {total})")
    
    if not ops:
        if offset >= total:
            print("  Done (offset >= total).")
            break
        print("  Empty response — waiting 30s before retry...")
        time.sleep(30)
        continue
    
    all_records.extend(ops)
    offset += limit
    
    if len(ops) < limit or offset >= total:
        print(f"  Reached end of results.")
        break
    
    time.sleep(5)  # Be nice to the API

print(f"\nTotal records fetched: {len(all_records)}")

# Deduplicate by id
seen = set()
unique = []
for r in all_records:
    pid = str(r.get("id"))
    if pid not in seen:
        seen.add(pid)
        unique.append(r)

print(f"Unique records: {len(unique)}")

# Save to cache
with open(CACHE_FILE, "w") as f:
    json.dump(unique, f)
print(f"✅ Saved to {CACHE_FILE}")

# Summary
from collections import Counter
dates = Counter(r.get("date_created", "")[:10] for r in unique)
print("\nBy day:")
for day in sorted(dates):
    print(f"  {day}: {dates[day]} sales")
