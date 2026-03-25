"""
Mercado Pago LIVE Sync — Incremental + Idempotent
Fixed: fetches each operation_type separately to avoid API quirks with sort+pos_payment.
"""
import os
import json
import requests
import sys
import time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.db_manager import init_db, insert_api_payments_batch, get_last_sync, set_last_sync

load_dotenv()
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")
BASE_URL = "https://api.mercadopago.com/v1"
SOURCE_NAME = "mp_api_payments"
MEX_TZ = timezone(timedelta(hours=-6))

# IMPORTANT: pos_payment records are HIDDEN when using sort=date_created+criteria=desc
# We fetch each operation_type separately: pos_payment gets no-sort (API quirk),
# other types use sort=date_created+criteria=desc (efficient newest-first).
OP_TYPES = ["pos_payment", "money_transfer", "regular_payment", "account_fund"]


def utc_now():
    return datetime.now(MEX_TZ).astimezone(timezone.utc)


def to_utc_str(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")


def get_paged_payments(begin_utc, end_utc, operation_type=None):
    """
    Fetch transactions in a UTC date range.
    pos_payment: NO sort params (API hides them with sort=date_created+criteria=desc)
    other types: use sort=date_created+criteria=desc for efficiency
    """
    begin_str = to_utc_str(begin_utc)
    end_str   = to_utc_str(end_utc)
    use_sort = (operation_type != "pos_payment")

    print(f"🔄 Fetching [{operation_type or 'all'}] {begin_utc.strftime('%Y-%m-%d %H:%M')} → {end_utc.strftime('%Y-%m-%d %H:%M')} CST  (sort={use_sort})")

    url = f"{BASE_URL}/payments/search"
    offset = 0
    limit = 100
    total_found = 0

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

        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"❌ API Error {response.status_code}: {response.text[:200]}")
            break

        res = response.json()
        results = res.get("results", [])
        paging_total = res.get("paging", {}).get("total", 0)

        if not results:
            break

        batch = []
        for r in results:
            if r.get("status") not in ["approved", "authorized"]:
                continue

            data = {
                'id': str(r.get("id")),
                'date_created': r.get("date_created"),
                'date_approved': r.get("date_approved"),
                'operation_type': r.get("operation_type"),
                'payment_type_id': r.get("payment_type_id"),
                'status': r.get("status"),
                'status_detail': r.get("status_detail"),
                'description': r.get("description", "Unknown"),
                'transaction_amount': float(r.get("transaction_amount", 0)),
                'net_received_amount': float(r.get("transaction_details", {}).get("net_received_amount", 0)),
                'fee_amount': 0.0,
                'payer_email': r.get("payer", {}).get("email", "N/A"),
                'payment_method_id': r.get("payment_method_id"),
                'collector_id': r.get("collector_id"),
                'payer_id': r.get("payer", {}).get("id")
            }

            if data['transaction_amount'] > data['net_received_amount'] and data['net_received_amount'] > 0:
                data['fee_amount'] = data['transaction_amount'] - data['net_received_amount']

            batch.append((data, json.dumps(r)))
            total_found += 1

        if batch:
            insert_api_payments_batch(batch)

        offset += limit
        if len(results) < limit:
            break
        time.sleep(0.3)

    if offset >= 1000 and paging_total > 1000:
        print(f"⚠️  {paging_total} records ({operation_type}) in window — offset limit hit. Consider a narrower date range.")

    return total_found


def run_incremental_sync():
    """Incremental sync — fetches only records newer than last_sync_ts."""
    init_db()

    last_ts = get_last_sync(SOURCE_NAME)
    lookback = timedelta(days=7)

    if last_ts:
        try:
            last_dt_utc = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            start_dt = last_dt_utc.astimezone(MEX_TZ) - timedelta(hours=6)
            print(f"📡 Incremental — resuming from {start_dt.strftime('%Y-%m-%d %H:%M')} CST  (last: {last_ts[:19]})")
        except Exception:
            start_dt = utc_now().astimezone(MEX_TZ) - lookback
            print(f"⚠️  Could not parse last_sync_ts '{last_ts}' — using 7-day lookback")
    else:
        start_dt = utc_now().astimezone(MEX_TZ) - lookback
        print(f"📡 First run — fetching last 7 days.")

    end_dt = utc_now().astimezone(MEX_TZ)
    window  = timedelta(days=7)
    grand_total = 0

    for op_type in OP_TYPES:
        total = 0
        current = start_dt
        while current < end_dt:
            next_dt = min(current + window, end_dt)
            captured = get_paged_payments(current, next_dt, operation_type=op_type)
            total += captured
            print(f"   📊 [{op_type}] +{captured} records")
            current = next_dt
            time.sleep(0.5)
        grand_total += total

    now_utc_str = to_utc_str(utc_now())
    set_last_sync(SOURCE_NAME, now_utc_str, f"incremental:{grand_total}records")

    print(f"\n🎉 Incremental sync done. +{grand_total} new records.")


def run_full_sync(start_date_str):
    """Full backfill from start_date to now."""
    init_db()

    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=MEX_TZ)
    except Exception:
        print(f"❌ Invalid date format: {start_date_str}  (use YYYY-MM-DD)")
        return

    end_dt = utc_now().astimezone(MEX_TZ)
    window  = timedelta(days=7)
    grand_total = 0

    print(f"🔄 Full backfill: {start_date_str} → {end_dt.strftime('%Y-%m-%d')}")

    for op_type in OP_TYPES:
        total = 0
        current = start_dt
        while current < end_dt:
            next_dt = min(current + window, end_dt)
            captured = get_paged_payments(current, next_dt, operation_type=op_type)
            total += captured
            print(f"   📊 [{op_type}] +{captured} records")
            current = next_dt
            time.sleep(0.5)
        grand_total += total

    now_utc_str = to_utc_str(utc_now())
    set_last_sync(SOURCE_NAME, now_utc_str, f"full:{grand_total}records")

    print(f"\n🎉 Full backfill done. +{grand_total} total records.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--full":
            start = sys.argv[2] if len(sys.argv) > 2 else "2026-01-01"
            run_full_sync(start)
        else:
            run_incremental_sync()
    else:
        run_incremental_sync()
