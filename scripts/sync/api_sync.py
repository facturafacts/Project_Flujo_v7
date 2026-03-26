"""
Mercado Pago API Sync — Single Account (v7)

Reusable sync script that pulls from one MP account and stores
all entries tagged with its source_account.

Usage:
    python -m scripts.sync.api_sync A           # sync Account A (incremental)
    python -m scripts.sync.api_sync B --full    # sync Account B from 2026-01-01
    python -m scripts.sync.api_sync B --full 2025-01-01  # custom start date
"""
import os
import json
import requests
import sys
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import init_db, insert_api_payments_batch, get_last_sync, set_last_sync

load_dotenv()

# ── Config ──────────────────────────────────────────────────────
# When imported by sync_account_a.py or sync_account_b.py, ACCOUNT is set
# before run_sync() is called. Only parse CLI args when run directly.
if __name__ == "__main__":
    ACCOUNT = sys.argv[1].upper() if len(sys.argv) > 1 else "A"
else:
    if "ACCOUNT" not in dir():
        ACCOUNT = "A"   # safe default if used incorrectly
TOKEN_KEY = f"MP_ACCESS_TOKEN_{ACCOUNT}"
MP_ACCESS_TOKEN = os.getenv(TOKEN_KEY)

if not MP_ACCESS_TOKEN or "REPLACE_WITH" in MP_ACCESS_TOKEN:
    print(f"❌ Token '{TOKEN_KEY}' is missing or not configured.")
    print(f"   Add your {TOKEN_KEY} to the .env file.")
    sys.exit(1)

BASE_URL = "https://api.mercadopago.com/v1"
MEX_TZ = timezone(timedelta(hours=-6))

# pos_payment records are HIDDEN when using sort=date_created+criteria=desc (API quirk)
OP_TYPES = ["pos_payment", "money_transfer", "regular_payment", "account_fund"]


# ── Helpers ────────────────────────────────────────────────────

def utc_now():
    return datetime.now(MEX_TZ).astimezone(timezone.utc)


def to_utc_str(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")


def get_paged_payments(begin_utc, end_utc, operation_type=None):
    begin_str = to_utc_str(begin_utc)
    end_str   = to_utc_str(end_utc)
    use_sort  = (operation_type != "pos_payment")

    print(f"  [{ACCOUNT}] {operation_type or 'all':20s}  "
          f"{begin_utc.strftime('%Y-%m-%d %H:%M')} → {end_utc.strftime('%Y-%m-%d %H:%M')} CST")

    url = f"{BASE_URL}/payments/search"
    offset = 0
    limit = 100
    total_captured = 0

    while offset < 1000:
        params = {
            "access_token": MP_ACCESS_TOKEN,
            "begin_date": begin_str,
            "end_date": end_str,
            "offset": offset,
            "limit": limit,
        }
        if use_sort:
            params["sort"] = "date_created"
            params["criteria"] = "desc"
        if operation_type:
            params["operation_type"] = operation_type

        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            print(f"  ❌ API {resp.status_code}: {resp.text[:150]}")
            break

        res = resp.json()
        results = res.get("results", [])
        paging_total = res.get("paging", {}).get("total", 0)

        if not results:
            break

        batch = []
        for r in results:
            if r.get("status") not in ("approved", "authorized"):
                continue

            gross   = float(r.get("transaction_amount", 0) or 0)
            net     = float(r.get("transaction_details", {}).get("net_received_amount", 0) or 0)
            fee     = gross - net if gross > net > 0 else 0.0

            data = {
                "id":                  str(r.get("id")),
                "date_created":        r.get("date_created"),
                "date_approved":       r.get("date_approved"),
                "operation_type":      r.get("operation_type"),
                "payment_type_id":     r.get("payment_type_id"),
                "status":              r.get("status"),
                "status_detail":       r.get("status_detail"),
                "description":         r.get("description", "Unknown"),
                "transaction_amount":  gross,
                "net_received_amount": net,
                "fee_amount":          fee,
                "payer_email":        r.get("payer", {}).get("email", "N/A"),
                "payment_method_id":  r.get("payment_method_id"),
                "collector_id":       r.get("collector_id"),
                "payer_id":           r.get("payer", {}).get("id"),
            }
            batch.append((data, json.dumps(r)))
            total_captured += 1

        if batch:
            insert_api_payments_batch(batch, ACCOUNT)

        offset += limit
        if len(results) < limit:
            break
        time.sleep(0.3)

    if offset >= 1000 and paging_total > 1000:
        print(f"  ⚠️  {paging_total} records in window — offset limit hit. Narrow date range.")

    return total_captured


# ── Main ────────────────────────────────────────────────────────

def run_sync(start_date_str=None, full=False):
    """Sync Account {ACCOUNT} from start_date (or 7-day lookback) to now."""
    init_db()

    if full and start_date_str:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=MEX_TZ)
        label = f"full backfill from {start_date_str}"
    elif full:
        start_dt = utc_now().astimezone(MEX_TZ) - timedelta(days=365)
        label = "full backfill (1 year)"
    else:
        last_ts = get_last_sync(ACCOUNT)
        if last_ts:
            try:
                start_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00")).astimezone(MEX_TZ) - timedelta(hours=6)
                label = f"incremental from last sync ({last_ts[:10]})"
            except Exception:
                start_dt = utc_now().astimezone(MEX_TZ) - timedelta(days=7)
                label = "incremental (parse error, 7-day lookback)"
        else:
            start_dt = utc_now().astimezone(MEX_TZ) - timedelta(days=7)
            label = "first run — 7-day lookback"

    end_dt  = utc_now().astimezone(MEX_TZ)
    window  = timedelta(days=7)
    grand   = 0

    print(f"\n🔄  Account {ACCOUNT} sync — {label}")
    print(f"    {start_dt.strftime('%Y-%m-%d %H:%M')} CST → {end_dt.strftime('%Y-%m-%d %H:%M')} CST\n")

    for op_type in OP_TYPES:
        total  = 0
        current = start_dt
        while current < end_dt:
            next_dt = min(current + window, end_dt)
            captured = get_paged_payments(current, next_dt, operation_type=op_type)
            total  += captured
            current = next_dt
            time.sleep(0.5)
        grand += total
        print(f"  📊 [{op_type}] +{total} records")

    now_str = to_utc_str(utc_now())
    set_last_sync(ACCOUNT, now_str, f"{label} | {grand} records")

    print(f"\n✅  Account {ACCOUNT} done — +{grand} new records.\n")


if __name__ == "__main__":
    full  = "--full" in sys.argv
    args  = [a for a in sys.argv if not a.startswith("--")]
    date_arg = args[2] if len(args) > 2 else None
    run_sync(start_date_str=date_arg, full=full)
