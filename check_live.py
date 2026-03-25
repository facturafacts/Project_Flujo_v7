import os, requests, json
from datetime import datetime, timedelta

TOKEN = os.getenv("MP_ACCESS_TOKEN")

today = datetime.now()
day_ago = today - timedelta(days=2)

begin = day_ago.strftime("%Y-%m-%dT%H:%M:%S.000-06:00")
end = today.strftime("%Y-%m-%dT%H:%M:%S.000-06:00")

url = "https://api.mercadopago.com/v1/payments/search"
params = {
    "access_token": TOKEN,
    "begin_date": begin,
    "end_date": end,
    "sort": "date_created",
    "criteria": "desc",
    "limit": 100
}

r = requests.get(url, params=params)
data = r.json()
results = data.get("results", [])
print(f"Total in last 2 days: {len(results)}")
print(f"Paging: {data.get('paging')}")
print()

from collections import Counter
ops = Counter(t.get("operation_type") for t in results)
print("By operation_type:", dict(ops))
print()

print("Last 15:")
for t in results[:15]:
    print(f"  [{t['operation_type']}] {t['date_created'][:19]} | ${t['transaction_amount']} | coll={t.get('collector_id')} | {t.get('description','')[:40]}")
