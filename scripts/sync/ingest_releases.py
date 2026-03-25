"""
Ingest Release Reports for both accounts — v7

Two modes:
  1. INGEST MODE (default): download + ingest new CSVs from the API
  2. SHRED MODE: parse already-downloaded CSVs in data/reports/

Mercado Pago download URL:
    GET https://api.mercadopago.com/v1/account/release_report/{file_name}
    Authorization: Bearer {token}

Usage:
    python scripts/sync/ingest_releases.py           # both accounts, ingest new
    python scripts/sync/ingest_releases.py A         # Account A only
    python scripts/sync/ingest_releases.py B         # Account B only
    python scripts/sync/ingest_releases.py --shred A  # shred existing CSVs, tag as A
"""
import os, requests, csv, sys, io, time, glob
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data.db_manager import init_db, insert_release_reports_batch, get_last_release_sync, set_last_release_sync

load_dotenv()

MEX_TZ    = timezone(timedelta(hours=-6))
BASE_URL  = "https://api.mercadopago.com/v1"
REPORTS_DIR = Path(__file__).parent.parent.parent / "data" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

ACCOUNTS   = sys.argv[1:] if len(sys.argv) > 1 else ["A", "B"]
SHRED_MODE = "--shred" in ACCOUNTS
ACCOUNTS   = [a for a in ACCOUNTS if a not in ("--shred", "--shred-only")]


# ── Helpers ────────────────────────────────────────────────────

def get_token(account):
    token = os.getenv(f"MP_ACCESS_TOKEN_{account}")
    if not token or "REPLACE_WITH" in token:
        print(f"  ⚠️  MP_ACCESS_TOKEN_{account} not configured — skipping.")
        return None
    return token


def list_reports(token):
    resp = requests.get(
        f"{BASE_URL}/account/release_report/list",
        params={"access_token": token},
        timeout=30
    )
    if resp.status_code != 200:
        print(f"  ❌ List error {resp.status_code}: {resp.text[:150]}")
        return []
    return resp.json()   # returns a LIST directly


def download_csv(file_name, token):
    resp = requests.get(
        f"{BASE_URL}/account/release_report/{file_name}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60
    )
    if resp.status_code != 200:
        print(f"  ❌ Download failed {resp.status_code}: {file_name}")
        return None
    return resp.text


def parse_csv(text):
    """Detect delimiter and parse CSV into dicts."""
    text = text.strip()
    if not text:
        return []

    sample = text[:500]
    if ";" in sample and "," not in sample:
        delim = ";"
    elif "\t" in sample:
        delim = "\t"
    else:
        delim = ","

    rows = []
    for line in csv.DictReader(io.StringIO(text), delimiter=delim):
        normalized = {k.upper().strip(): v for k, v in line.items() if k}
        source_id = (
            normalized.get("SOURCE_ID") or normalized.get("SOURCEID")
            or normalized.get("ID") or ""
        ).strip()
        if not source_id:
            continue

        def f(val):
            if val is None:
                return 0.0
            try:
                return float(str(val).replace(",", "").replace("$", "").replace(" ", ""))
            except Exception:
                return 0.0

        rows.append({
            "source_id":          source_id,
            "date":               (normalized.get("TRANSACTION_DATE") or normalized.get("DATE") or "")[:10],
            "description":        (normalized.get("DESCRIPTION") or "Unknown").strip(),
            "gross_amount":       f(normalized.get("GROSS_AMOUNT")),
            "net_credit_amount":  f(normalized.get("NET_CREDIT_AMOUNT")),
            "net_debit_amount":  f(normalized.get("NET_DEBIT_AMOUNT")),
            "raw_csv_row":        "",
        })
    return rows


# ── Shred existing CSV files ────────────────────────────────────

def shred_existing_csvs(account):
    """Parse CSVs already on disk and ingest them as source_account."""
    csv_files = sorted(REPORTS_DIR.glob("*.csv"))
    if not csv_files:
        print(f"  No CSV files found in {REPORTS_DIR}.")
        return 0

    total = 0
    for csv_path in csv_files:
        try:
            text = csv_path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  ❌ Could not read {csv_path.name}: {e}")
            continue

        rows = parse_csv(text)
        if not rows:
            continue

        insert_release_reports_batch(rows, account)
        total += len(rows)
        print(f"  ✅ {csv_path.name}: +{len(rows)} rows")

    return total


# ── Ingest new from API ─────────────────────────────────────────

def ingest_account(account):
    token = get_token(account)
    if not token:
        return 0

    print(f"\n📥  Account {account} — release reports\n")

    if SHRED_MODE:
        print(f"  🔄 Shred mode — parsing existing CSVs in {REPORTS_DIR}")
        total = shred_existing_csvs(account)
        print(f"\n✅  Account {account}: +{total} rows from existing CSVs.")
        return total

    # Normal: list → download → ingest
    files = list_reports(token)
    if not files:
        print(f"  No reports found for {account}.")
        return 0

    total = 0
    for f in files:
        fname = f.get("file_name") or "unknown"
        status = f.get("status", "")
        created = f.get("date_created", "")[:10]

        # Only download enabled (finished) reports
        if status != "enabled":
            continue

        out_path = REPORTS_DIR / f"{account}_{fname}"
        if out_path.exists():
            print(f"  ✓  {fname} — already downloaded, skipping.")
            continue

        print(f"  ▶  {fname}  ({created})")
        content = download_csv(fname, token)
        if not content:
            continue

        rows = parse_csv(content)
        if rows:
            insert_release_reports_batch(rows, account)
            total += len(rows)
            print(f"     ✅ +{len(rows)} rows ingested.")

        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(f"# Downloaded: {datetime.now(MEX_TZ)}\n")
        time.sleep(0.3)

    now_str = datetime.now(MEX_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")
    set_last_release_sync(account, now_str, f"{total} rows from {len(files)} reports")
    print(f"\n✅  Account {account}: +{total} rows total.")
    return total


# ── Main ────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    grand = 0
    for acc in [a.upper() for a in ACCOUNTS]:
        grand += ingest_account(acc)
    print(f"\n🎉 All done — +{grand} rows across {len(ACCOUNTS)} account(s).\n")
