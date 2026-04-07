"""
E15 ingest — Cheq + Mashgin POS systems → ADLS bronze.

Sources:
  - Cheq orders     → bronze/cheq_orders/pending/
  - Mashgin txns    → bronze/mashgin_transactions/pending/
  - Mashgin kiosks  → bronze/mashgin_kiosks/pending/  (overwritten each run)

Cursors stored in blob:
  - cursors/cheq_orders/cursor.json         {"max_updated_at": "..."}
  - cursors/mashgin_transactions/cursor.json {"last_complete_date": "YYYY-MM-DD"}
"""

import os
import json
import time
import logging
import datetime
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# ── Cheq ──────────────────────────────────────────────────────────────────────
CHEQ_URL = os.getenv("CHEQ_URL", "https://api.cheq.tools/api/orders")
CHEQ_API_KEY = os.getenv("CHEQ_API_KEY")

# ── Mashgin ───────────────────────────────────────────────────────────────────
MASHGIN_URL = os.getenv("MASHGIN_URL", "https://graph.mashgin.com/v1/transactions")
MASHGIN_KIOSK_URL = os.getenv("MASHGIN_KIOSK_URL", "https://graph.mashgin.com/v1/kiosks")
MASHGIN_TOKEN = os.getenv("MASHGIN_TOKEN")

# ── ADLS ──────────────────────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "e15")

# ── Databricks ────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")

# ── Defaults ──────────────────────────────────────────────────────────────────
CHEQ_DEFAULT_START = "2024-06-23T00:00:00Z"
MASHGIN_DEFAULT_START = datetime.date(2024, 6, 23)
LATE_UPDATE_OVERLAP_DAYS = 1  # re-query last N days each run to catch late refunds/uploads

# ── Logging ───────────────────────────────────────────────────────────────────
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log = logging.getLogger("e15")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingest_errors.log"))
_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(_fh)


# ── ADLS helpers ──────────────────────────────────────────────────────────────
def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


def _blob_upload(blob_client, path, data: bytes):
    blob_client.get_container_client(ADLS_CONTAINER) \
               .get_blob_client(path) \
               .upload_blob(data, overwrite=True)


def _blob_download(blob_client, path):
    try:
        return blob_client.get_container_client(ADLS_CONTAINER) \
                          .get_blob_client(path) \
                          .download_blob().readall()
    except Exception:
        return None


def write_batch(blob_client, table, records, suffix=""):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = f"bronze/{table}/pending/{ts}{suffix}.json"
    ndjson = "\n".join(json.dumps(r, default=str) for r in records)
    _blob_upload(blob_client, path, ndjson.encode("utf-8"))
    return path


# ── Cursor helpers ────────────────────────────────────────────────────────────
def load_cursor(blob_client, table):
    raw = _blob_download(blob_client, f"cursors/{table}/cursor.json")
    if raw is None:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def save_cursor(blob_client, table, cursor):
    path = f"cursors/{table}/cursor.json"
    _blob_upload(blob_client, path, json.dumps(cursor).encode())


# ── Cheq ──────────────────────────────────────────────────────────────────────
def fetch_cheq_page(session, page, start_range, end_range):
    payload = {"start_range": start_range, "end_range": end_range}
    r = session.get(CHEQ_URL, params={"page": page}, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()


def ingest_cheq(blob_client):
    table = "cheq_orders"
    cursor = load_cursor(blob_client, table)

    last_updated = cursor.get("max_updated_at", CHEQ_DEFAULT_START)
    # Re-query the last N days to catch late refunds/updates
    try:
        last_dt = datetime.datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        last_dt = datetime.datetime.strptime(CHEQ_DEFAULT_START, "%Y-%m-%dT%H:%M:%SZ")
    start_dt = last_dt - datetime.timedelta(days=LATE_UPDATE_OVERLAP_DAYS)

    start_range = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_range = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[cheq_orders] Querying {start_range} → {end_range}")

    session = requests.Session()
    session.headers.update({
        "x-api-key": CHEQ_API_KEY,
        "Content-Type": "application/json",
    })

    page = 1
    total = 0
    new_max_updated = last_updated

    while True:
        try:
            data = fetch_cheq_page(session, page, start_range, end_range)
        except Exception as e:
            log.error(f"[cheq_orders] page {page} failed: {e}")
            print(f"[cheq_orders] page {page} ERROR: {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        write_batch(blob_client, table, results)
        total += len(results)

        # Track max updated_at for cursor
        for rec in results:
            updated = rec.get("order", {}).get("updated_at")
            if updated and updated > new_max_updated:
                new_max_updated = updated

        if page % 10 == 0:
            print(f"[cheq_orders] page {page}: {total:,} records so far")

        # Cheq pagination: stop if we got fewer than max_per_page
        max_per_page = data.get("max_results_per_page", 100)
        if len(results) < max_per_page:
            break
        page += 1

    if total > 0:
        save_cursor(blob_client, table, {"max_updated_at": new_max_updated})

    print(f"[cheq_orders] Done — {total:,} records, cursor → {new_max_updated}")
    return total


# ── Mashgin ───────────────────────────────────────────────────────────────────
def fetch_mashgin_kiosks(session):
    r = session.get(MASHGIN_KIOSK_URL, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])


def fetch_mashgin_day(session, day):
    """Fetch all transactions for a single day, paginating until no more."""
    all_txns = []
    page = 0
    start = day.strftime("%Y-%m-%dT00:00:00")
    end = day.strftime("%Y-%m-%dT23:59:59")

    while True:
        params = {"start": start, "end": end, "page": page, "limit": 1000}
        r = session.get(MASHGIN_URL, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        txns = data.get("data", [])
        if not txns:
            break
        all_txns.extend(txns)
        if not data.get("has_more"):
            break
        page += 1

    return all_txns


def ingest_mashgin(blob_client):
    session = requests.Session()
    session.auth = (MASHGIN_TOKEN, "")

    # 1. Kiosks (small lookup table — overwrite each run)
    try:
        kiosks = fetch_mashgin_kiosks(session)
        if kiosks:
            write_batch(blob_client, "mashgin_kiosks", kiosks)
            print(f"[mashgin_kiosks] {len(kiosks)} kiosks")
    except Exception as e:
        log.error(f"[mashgin_kiosks] {e}")
        print(f"[mashgin_kiosks] ERROR: {e}")

    # 2. Transactions (day-by-day windows)
    table = "mashgin_transactions"
    cursor = load_cursor(blob_client, table)

    last_str = cursor.get("last_complete_date")
    if last_str:
        last_complete = datetime.date.fromisoformat(last_str)
    else:
        last_complete = MASHGIN_DEFAULT_START - datetime.timedelta(days=1)

    # Reprocess most recent days to catch late uploads
    start_day = last_complete - datetime.timedelta(days=LATE_UPDATE_OVERLAP_DAYS - 1)
    today = datetime.date.today()
    print(f"[mashgin_transactions] Walking days {start_day} → {today}")

    total = 0
    cur = start_day
    while cur <= today:
        try:
            txns = fetch_mashgin_day(session, cur)
        except Exception as e:
            log.error(f"[mashgin_transactions] {cur}: {e}")
            print(f"[mashgin_transactions] {cur} ERROR: {e}")
            cur += datetime.timedelta(days=1)
            continue

        if txns:
            write_batch(blob_client, table, txns, suffix=f"_{cur.isoformat()}")
            total += len(txns)
            print(f"[mashgin_transactions] {cur}: {len(txns)} records")

        # Save cursor after each completed day (resume-safe)
        # Don't advance past yesterday — today is still in flight
        if cur < today:
            save_cursor(blob_client, table, {"last_complete_date": cur.isoformat()})
        cur += datetime.timedelta(days=1)

    print(f"[mashgin_transactions] Done — {total:,} records")
    return total


# ── Databricks trigger ────────────────────────────────────────────────────────
def trigger_databricks_job():
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID]):
        print("Databricks job trigger not configured — skipping")
        return
    resp = requests.post(
        f"{DATABRICKS_HOST.rstrip('/')}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"job_id": int(DATABRICKS_JOB_ID)},
        timeout=30,
    )
    if resp.status_code == 200:
        print(f"Databricks job triggered — run_id: {resp.json().get('run_id')}")
    else:
        log.error(f"Databricks trigger failed: {resp.text}")
        print(f"WARNING: Databricks trigger failed — {resp.text}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    print(f"E15 ingest starting at {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    blob_client = get_blob_client()

    cheq_total = 0
    mashgin_total = 0

    try:
        cheq_total = ingest_cheq(blob_client)
    except Exception as e:
        log.error(f"Cheq ingest failed: {e}")
        print(f"Cheq ingest ERROR: {e}")

    try:
        mashgin_total = ingest_mashgin(blob_client)
    except Exception as e:
        log.error(f"Mashgin ingest failed: {e}")
        print(f"Mashgin ingest ERROR: {e}")

    elapsed = time.time() - start
    print(f"\nE15 ingest complete in {elapsed:.1f}s")
    print(f"  cheq_orders:          {cheq_total:,}")
    print(f"  mashgin_transactions: {mashgin_total:,}")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
