import os
import json
import time
import logging
import datetime
import argparse
import requests
import pytz
from requests.auth import HTTPBasicAuth
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
APP_ID     = os.getenv("FORTRESS_APP_ID", "com.commanders")
API_KEY    = os.getenv("FORTRESS_API_KEY")
AGENCY     = os.getenv("FORTRESS_AGENCY", "commanders")
BASIC_USER = os.getenv("FORTRESS_USER")
BASIC_PASS = os.getenv("FORTRESS_PASS")

ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY  = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER    = os.getenv("ADLS_CONTAINER", "fortress")

DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("FORTRESS_DATABRICKS_JOB_ID")

PAGE_SIZE      = 50000
LOOKBACK_HOURS = 24
TZ             = pytz.timezone("America/New_York")
DT_FMT         = "%Y-%m-%dT%H:%M:%S"
RETRY_STATUSES = {406, 408, 429, 500, 502, 503, 504}
BACKOFFS       = [0, 30, 60, 120, 300]  # seconds between retries (0 = immediate first attempt)

logging.basicConfig(
    filename="fortress_ingest.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s",
)

# chunk=True: splits the window into monthly sub-requests to stay under API record limits
ENDPOINTS = {
    "event_information": {"url": "https://commandersapis.fortressus.com/FGB_WebApplication/FGB/Production/api/CRM/EventInformation_PagingStatistics/",  "chunk": False},
    "time_attendance":   {"url": "https://commandersapis.fortressus.com/FGB_WebApplication/FGB/Production/api/CRM/TimeAttendanceInformation_Paging/",   "chunk": True},
}


# ── Time window ───────────────────────────────────────────────────────────────
def get_window(backfill_from: str | None = None) -> tuple[str, str]:
    now = datetime.datetime.now(TZ)
    if backfill_from:
        start = TZ.localize(datetime.datetime.strptime(backfill_from, "%Y-%m-%d"))
    else:
        start = now - datetime.timedelta(hours=LOOKBACK_HOURS)
    return start.strftime(DT_FMT), now.strftime(DT_FMT)


def monthly_chunks(from_iso: str, to_iso: str) -> list[tuple[str, str]]:
    """Split a date range into monthly sub-windows to stay under API record limits."""
    start  = datetime.datetime.strptime(from_iso, DT_FMT)
    end    = datetime.datetime.strptime(to_iso,   DT_FMT)
    chunks = []
    cur    = start
    while cur < end:
        if cur.month == 12:
            nxt = cur.replace(year=cur.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            nxt = cur.replace(month=cur.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
        chunks.append((cur.strftime(DT_FMT), min(nxt, end).strftime(DT_FMT)))
        cur = nxt
    return chunks


# ── API fetch ─────────────────────────────────────────────────────────────────
def _extract_rows(payload) -> list:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                return v
    return []


def fetch_chunk(session: requests.Session, url: str, auth, from_iso: str, to_iso: str) -> list:
    """Fetch all pages for a single monthly chunk."""
    headers  = {"Content-Type": "application/json", "Accept": "application/json"}
    all_rows = []
    page     = 1

    while True:
        body = {
            "Header": {
                "Client_AppID":      APP_ID,
                "Client_APIKey":     API_KEY,
                "Client_AgencyCode": AGENCY,
                "UniqID":            page,
            },
            "FromDateTime": from_iso,
            "ToDateTime":   to_iso,
            "PageSize":     PAGE_SIZE,
            "PageNumber":   page,
        }

        rows = None
        last_err = None
        for attempt, delay in enumerate(BACKOFFS):
            if delay:
                print(f"    retry {attempt}/{len(BACKOFFS)-1} — waiting {delay}s (last error: {last_err})")
                time.sleep(delay)
            try:
                r = session.post(url, headers=headers, json=body, auth=auth, timeout=30)
                if r.status_code in RETRY_STATUSES:
                    last_err = f"HTTP {r.status_code}"
                    continue
                r.raise_for_status()  # non-retryable errors (403, 404, etc.) raise immediately
                rows = _extract_rows(r.json())
                break
            except requests.HTTPError as e:
                raise  # don't retry hard errors
            except (requests.RequestException, ValueError) as e:
                last_err = str(e)
                continue  # retry on network errors / bad JSON

        if rows is None:
            raise RuntimeError(f"All retries exhausted on page {page} ({from_iso} → {to_iso}) — last error: {last_err}")
        if not rows:
            break

        all_rows.extend(rows)
        print(f"    page {page}: {len(rows):,} rows")

        if len(rows) < PAGE_SIZE:
            break
        page += 1

    return all_rows


# ── ADLS helpers ──────────────────────────────────────────────────────────────
def get_blob_client() -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


def _upload(blob_client: BlobServiceClient, path: str, data: bytes):
    blob_client.get_container_client(ADLS_CONTAINER) \
               .get_blob_client(path) \
               .upload_blob(data, overwrite=True)


def write_ndjson(blob_client: BlobServiceClient, table: str, rows: list):
    ts    = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    ndjson = "\n".join(json.dumps(r) for r in rows)
    _upload(blob_client, f"bronze/fortress_{table}/pending/{ts}.json", ndjson.encode())


def write_window(blob_client: BlobServiceClient, table: str, from_iso: str, to_iso: str):
    window = {"from_ts": from_iso, "to_ts": to_iso}
    _upload(blob_client, f"bronze/fortress_{table}/pending/window.json", json.dumps(window).encode())


# ── Databricks job trigger ────────────────────────────────────────────────────
def trigger_databricks_job():
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID]):
        print("Databricks job trigger not configured — skipping")
        return
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"job_id": int(DATABRICKS_JOB_ID)},
    )
    if resp.status_code == 200:
        print(f"Databricks job triggered — run_id: {resp.json().get('run_id')}")
    else:
        logging.error(f"Databricks job trigger failed: {resp.text}")
        print(f"WARNING: Databricks trigger failed — {resp.text}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fortress ingest → ADLS")
    parser.add_argument(
        "--from", dest="backfill_from", metavar="YYYY-MM-DD", default=None,
        help="Backfill start date (midnight ET). Omit for default 24h lookback.",
    )
    args = parser.parse_args()

    from_iso, to_iso = get_window(args.backfill_from)
    auth             = HTTPBasicAuth(BASIC_USER, BASIC_PASS)
    blob_client      = get_blob_client()

    print(f"Fortress ingest | {from_iso} → {to_iso}\n")

    for table, cfg in ENDPOINTS.items():
        url    = cfg["url"]
        chunks = monthly_chunks(from_iso, to_iso) if cfg["chunk"] else [(from_iso, to_iso)]
        print(f"[{table}] Fetching {len(chunks)} chunk(s)...")
        total = 0
        with requests.Session() as session:
            for chunk_from, chunk_to in chunks:
                print(f"  {chunk_from} → {chunk_to}")
                rows = fetch_chunk(session, url, auth, chunk_from, chunk_to)
                if rows:
                    write_ndjson(blob_client, table, rows)
                    total += len(rows)

        if not total:
            print(f"[{table}] No data in window\n")
            continue

        write_window(blob_client, table, from_iso, to_iso)
        print(f"[{table}] {total:,} rows → bronze/fortress_{table}/pending/\n")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
