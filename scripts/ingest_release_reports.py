"""
Mercado Pago Release Report Pipeline
Fully automated: creates a fresh 7-day report, polls until ready, downloads, and ingests.
"""
import csv
import json
import requests
import os
import time
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.db_manager import get_connection, insert_release_report_batch, get_last_sync, set_last_sync

load_dotenv()
MP_TOKEN = os.getenv("MP_ACCESS_TOKEN")
BASE_URL = "https://api.mercadopago.com/v1"
REPORTS_DIR = Path("/home/subsc/.openclaw/workspace/workspaces/CLIENTS/Pescadero-62/data/reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
SOURCE_NAME = "mp_release_reports"
MEX_TZ = timezone(timedelta(hours=-6))


def create_release_report(begin_date_utc: str, end_date_utc: str) -> dict | None:
    """
    Create a new release report via MP API.
    Returns the creation response dict (has id, status='pending') or None on failure.
    """
    url = f"{BASE_URL}/account/release_report"
    headers = {"Authorization": f"Bearer {MP_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "begin_date": begin_date_utc,
        "end_date": end_date_utc
    }
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code == 202:
        return r.json()
    print(f"❌ Failed to create report: {r.status_code} | {r.text[:200]}")
    return None


def poll_for_report(pre_existing_ids: set, timeout_sec: int = 300, poll_sec: int = 10) -> dict | None:
    """
    Poll the report list until our newly-created report appears with status='enabled'.
    
    Args:
        pre_existing_ids: set of report IDs already in the list before we called POST
        timeout_sec: give up after this long
        poll_sec: seconds between each poll
    
    Returns:
        The report dict when ready, or None if timeout
    """
    url = f"{BASE_URL}/account/release_report/list"
    params = {"access_token": MP_TOKEN}
    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print(f"⚠️  List error {r.status_code}, retrying...")
            time.sleep(poll_sec)
            continue

        reports = r.json()
        for rep in reports:
            rep_id = rep.get("id")
            # Skip old reports
            if rep_id in pre_existing_ids:
                continue
            # A candidate is enabled and has a file_name
            if rep.get("status") == "enabled" and rep.get("file_name"):
                return rep

        remaining = int(deadline - time.time())
        print(f"   ⏳ Still pending... ({remaining}s left, {len(reports)} reports in list)")
        time.sleep(poll_sec)

    return None


def download_report(file_name: str) -> str | None:
    """Download a release report CSV and return its text content."""
    url = f"{BASE_URL}/account/release_report/{file_name}"
    headers = {"Authorization": f"Bearer {MP_TOKEN}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.text
    print(f"❌ Download failed {r.status_code}: {file_name}")
    return None


def parse_and_ingest(content: str, file_name: str) -> int:
    """Parse a release report CSV and insert all new rows into the DB. Returns row count."""
    for delimiter in [';', ',', '\t']:
        try:
            reader = csv.DictReader(StringIO(content), delimiter=delimiter)
            rows = list(reader)
            if rows and ('SOURCE_ID' in rows[0] or 'SOURCEID' in rows[0]):
                print(f"   ✅ Delimiter '{delimiter}', {len(rows)} rows")
                break
        except Exception:
            continue
    else:
        print(f"❌ Could not parse {file_name}")
        return 0

    batch = []
    for row in rows:
        normalized = _normalize_row(row)
        if normalized:
            batch.append(normalized)

    if batch:
        insert_release_report_batch(batch)

    return len(batch)


def _normalize_row(row: dict) -> dict | None:
    """Normalize a CSV row to our standard format."""
    normalized = {k.upper().strip(): v for k, v in row.items() if k}

    source_id = (
        normalized.get('SOURCE_ID') or
        normalized.get('SOURCEID') or
        normalized.get('ID') or
        normalized.get('EXTERNAL_REFERENCE')
    )
    if not source_id:
        return None

    date = (
        normalized.get('TRANSACTION_DATE') or
        normalized.get('DATE') or
        normalized.get('CREATION_DATE') or
        ''
    )

    def to_float(val):
        if val is None:
            return 0.0
        try:
            return float(str(val).replace(',', '.'))
        except Exception:
            return 0.0

    return {
        'source_id': str(source_id).strip(),
        'date': date[:10],
        'description': str(normalized.get('DESCRIPTION', 'Unknown')).strip(),
        'gross_amount': to_float(normalized.get('GROSS_AMOUNT')),
        'net_credit_amount': to_float(normalized.get('NET_CREDIT_AMOUNT')),
        'net_debit_amount': to_float(normalized.get('NET_DEBIT_AMOUNT')),
        'raw_csv_row': json.dumps(row)
    }


def run_pipeline():
    """
    Main entry point:
    1. Create a fresh 7-day release report via MP API
    2. Poll until it's ready
    3. Download and ingest with deduplication
    4. Update sync_metadata
    """
    today = datetime.now(MEX_TZ)
    last_7_days = today - timedelta(days=7)

    # UTC strings for the API (no timezone suffix, MP expects bare UTC or Z)
    begin_utc = last_7_days.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_utc   = today.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    print(f"📋 Creating release report: {last_7_days.strftime('%Y-%m-%d')} → {today.strftime('%Y-%m-%d')}  (UTC: {begin_utc} → {end_utc})")

    # Snapshot existing report IDs so we can detect the new one
    r_list = requests.get(
        f"{BASE_URL}/account/release_report/list",
        params={"access_token": MP_TOKEN}
    )
    pre_existing = {rep['id'] for rep in r_list.json()} if r_list.ok else set()
    print(f"   (Pre-existing reports: {len(pre_existing)})")

    # Step 1: Create the report
    created = create_release_report(begin_utc, end_utc)
    if not created:
        print("❌ Could not create release report. Aborting.")
        return

    report_id = created.get('id')
    print(f"   📝 Report creation accepted (id={report_id}, status=pending)")

    # Step 2: Poll until ready
    print(f"   ⏳ Waiting for generation (this can take 1-5 minutes)...")
    ready = poll_for_report(pre_existing, timeout_sec=600, poll_sec=15)

    if not ready:
        print("⏰ Timeout waiting for report generation.")
        return

    file_name = ready['file_name']
    print(f"   ✅ Report ready: {file_name}")

    # Step 3: Download and ingest
    content = download_report(file_name)
    if not content:
        return

    # Save raw CSV
    save_path = REPORTS_DIR / file_name
    with open(save_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"   💾 Saved: {save_path.name}")

    count = parse_and_ingest(content, file_name)
    print(f"   📥 Ingested {count} new rows")

    # Step 4: Update metadata
    now_str = datetime.now(MEX_TZ).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    set_last_sync(SOURCE_NAME, now_str, f"auto:{file_name}:{count}rows")
    print(f"\n🎉 Pipeline complete! Report: {file_name} | Rows: {count}")


if __name__ == "__main__":
    run_pipeline()
