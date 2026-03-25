"""
Ingest Release Reports for both accounts.

Fetches the list of available CSV reports from:
    GET https://api.mercadopago.com/v1/account/release_report/list

Then downloads each CSV and shreds it into:
    source_release_reports (tagged by source_account)

Usage:
    python scripts/sync/ingest_releases.py           # both accounts
    python scripts/sync/ingest_releases.py A         # Account A only
    python scripts/sync/ingest_releases.py B          # Account B only
"""
import os, requests, csv, sys, io, time
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import init_db, insert_release_reports_batch, get_last_release_sync, set_last_release_sync

load_dotenv()

MEX_TZ  = timezone(timedelta(hours=-6))
ACCOUNTS = sys.argv[1:] if len(sys.argv) > 1 else ["A", "B"]
REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "reports")

os.makedirs(REPORT_DIR, exist_ok=True)


def get_token(account):
    token = os.getenv(f"MP_ACCESS_TOKEN_{account}")
    if not token or "REPLACE_WITH" in token:
        print(f"  ⚠️  MP_ACCESS_TOKEN_{account} not configured — skipping.")
        return None
    return token


def list_reports(token, account):
    url = "https://api.mercadopago.com/v1/account/release_report/list"
    params = {"access_token": token}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("files", [])
    except Exception as e:
        print(f"  ❌ Failed to list reports for {account}: {e}")
        return []


def download_and_parse(csv_url, token):
    resp = requests.get(csv_url, params={"access_token": token}, timeout=60)
    resp.raise_for_status()
    text = resp.text.strip()

    # Detect delimiter
    sample = text[:500]
    if ";" in sample and "," not in sample:
        delimiter = ";"
    elif "\t" in sample:
        delimiter = "\t"
    else:
        delimiter = ","

    rows = []
    for line in csv.DictReader(io.StringIO(text), delimiter=delimiter):
        # Normalize field names
        source_id  = line.get("id") or line.get("source_id") or line.get("release_id", "").strip()
        date       = line.get("date") or line.get("date_created") or line.get("transaction_date", "").strip()
        desc_raw   = line.get("description") or line.get("transaction_description") or ""
        gross_raw  = line.get("gross_amount") or line.get("gross") or line.get("amount") or "0"
        credit_raw = line.get("net_credit_amount") or line.get("net_credit") or line.get("credit") or "0"
        debit_raw  = line.get("net_debit_amount") or line.get("net_debit") or line.get("debit") or "0"

        try:
            gross   = float(gross_raw.replace(",", "").replace("$", "").replace(" ", ""))
        except (ValueError, AttributeError):
            gross = 0.0
        try:
            credit = float(credit_raw.replace(",", "").replace("$", "").replace(" ", ""))
        except (ValueError, AttributeError):
            credit = 0.0
        try:
            debit  = float(debit_raw.replace(",", "").replace("$", "").replace(" ", ""))
        except (ValueError, AttributeError):
            debit = 0.0

        rows.append({
            "source_id":          source_id,
            "date":               date,
            "description":        desc_raw,
            "gross_amount":       gross,
            "net_credit_amount":  credit,
            "net_debit_amount":   debit,
            "raw_csv_row":        line.get("raw_row", ""),
        })
    return rows


def ingest_account(account):
    token = get_token(account)
    if not token:
        return 0

    print(f"\n📥  Account {account} — release reports\n")
    files = list_reports(token, account)
    if not files:
        print(f"  No reports found for {account}.")
        return 0

    total = 0
    for f in files:
        fname = f.get("name") or f.get("file_name") or "unknown"
        created = f.get("created") or ""
        print(f"  ▶  {fname}  ({created})")

        # Skip if already downloaded (basic dedup by filename)
        out_path = os.path.join(REPORT_DIR, f"{account}_{fname}")
        if os.path.exists(out_path):
            print(f"     ✓ already downloaded — skipping.")
            continue

        csv_url = f.get("url") or f.get("file_url")
        if not csv_url:
            print(f"     ⚠ no URL found in file entry.")
            continue

        try:
            parsed = download_and_parse(csv_url, token)
        except Exception as e:
            print(f"     ❌ download failed: {e}")
            continue

        insert_release_reports_batch(parsed, account)
        with open(out_path, "w") as fh:
            fh.write(f"# Downloaded: {datetime.now(MEX_TZ)}\n# URL: {csv_url}\n")
        total += len(parsed)
        print(f"     ✅ +{len(parsed)} rows ingested.")
        time.sleep(0.5)

    now_str = datetime.now(MEX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")
    set_last_release_sync(account, now_str, f"{total} rows from {len(files)} reports")
    print(f"\n✅  Account {account}: +{total} rows total.")
    return total


if __name__ == "__main__":
    init_db()
    grand = 0
    for acc in ACCOUNTS:
        grand += ingest_account(acc.upper())
    print(f"\n🎉 All done — +{grand} rows across {len(ACCOUNTS)} account(s).\n")
