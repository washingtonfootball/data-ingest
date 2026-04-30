from dotenv import load_dotenv
import os
import logging
import json
import requests
import time
import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ── API credentials ──────────────────────────────────────────────────────────
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
BASE_URL = os.getenv('BASE_URL')
ACCESS_URL = os.getenv('ACCESS_URL')

# ── ADLS / Blob Storage ───────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv('ADLS_ACCOUNT_NAME')
ADLS_ACCOUNT_KEY = os.getenv('ADLS_ACCOUNT_KEY')
ADLS_CONTAINER = os.getenv('ADLS_CONTAINER', 'seatgeek')

# ── Databricks job trigger ────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_JOB_ID = os.getenv('DATABRICKS_JOB_ID')

PER_PAGE = 25000

# ── Logging — SeatGeek API only ──────────────────────────────────────────────
# Suppress Azure SDK and other library noise
logging.getLogger('azure').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

log = logging.getLogger('seatgeek')
log.setLevel(logging.INFO)
_fh = logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ingest_errors.log'))
_fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
log.addHandler(_fh)

ISO_DATETIME_REGEX = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')

# ── Token management ──────────────────────────────────────────────────────────
_token_cache = {"token": None, "expires_at": None}
_token_lock = __import__('threading').Lock()


def _refresh_token():
    resp = requests.post(
        ACCESS_URL,
        headers={"content-type": "application/json"},
        data=json.dumps({
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "audience": BASE_URL,
            "grant_type": "client_credentials",
        }),
    )
    body = resp.json()
    if 'access_token' not in body:
        raise RuntimeError(f"Token fetch failed: {body}")
    _token_cache['token'] = f"{body['token_type']} {body['access_token']}"
    _token_cache['expires_at'] = time.time() + body['expires_in'] - 30  # 30s buffer


def get_access_token():
    with _token_lock:
        if _token_cache['token'] is None or time.time() >= _token_cache['expires_at']:
            _refresh_token()
    return _token_cache['token']


# ── API call ──────────────────────────────────────────────────────────────────
def call_api(endpoint, cursor='', amount=PER_PAGE):
    cursor_short = (cursor[:40] + '...') if cursor and len(cursor) > 40 else cursor
    for attempt in range(3):
        try:
            headers = {
                "content-type": "application/json",
                "Authorization": get_access_token(),
            }
            params = {'cursor': cursor or '', 'limit': amount}
            resp = requests.get(
                url=f"{BASE_URL}/v1/{endpoint}",
                headers=headers,
                params=params,
                timeout=30,
            )
            log.info(f"[{endpoint}] response={resp.status_code} cursor='{cursor_short}' limit={amount}")
            if resp.status_code == 401:
                log.info(f"[{endpoint}] 401 — refreshing token")
                with _token_lock:
                    _token_cache['token'] = None  # force refresh
                headers["Authorization"] = get_access_token()
                resp = requests.get(
                    url=f"{BASE_URL}/v1/{endpoint}",
                    headers=headers,
                    params=params,
                    timeout=30,
                )
                log.info(f"[{endpoint}] post-refresh response={resp.status_code}")
            return resp
        except requests.RequestException as e:
            log.error(f"[{endpoint}] attempt {attempt+1}/3 failed (cursor='{cursor_short}'): {e}")
            time.sleep(2 ** attempt)
    log.error(f"[{endpoint}] all 3 attempts failed (cursor='{cursor_short}')")
    return None


# ── ADLS helpers ──────────────────────────────────────────────────────────────
def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


def _blob_upload(blob_client, path, data: bytes):
    container = blob_client.get_container_client(ADLS_CONTAINER)
    container.get_blob_client(path).upload_blob(data, overwrite=True)


def _blob_download(blob_client, path) -> bytes | None:
    try:
        container = blob_client.get_container_client(ADLS_CONTAINER)
        return container.get_blob_client(path).download_blob().readall()
    except Exception:
        return None


def save_cursor(blob_client, table, cursor_value):
    path = f"cursors/{table}/cursor.json"
    payload = {
        "cursor": cursor_value,
        "updated_at": datetime.datetime.utcnow().isoformat(),
    }
    _blob_upload(blob_client, path, json.dumps(payload).encode())


def load_cursor(blob_client, table) -> str | None:
    raw = _blob_download(blob_client, f"cursors/{table}/cursor.json")
    if raw is None:
        return None
    return json.loads(raw).get("cursor")


def set_full_refresh_flag(blob_client, table):
    path = f"cursors/{table}/full_refresh.json"
    _blob_upload(blob_client, path, b'{"full_refresh": true}')


def clear_full_refresh_flag(blob_client, table):
    try:
        container = blob_client.get_container_client(ADLS_CONTAINER)
        container.get_blob_client(f"cursors/{table}/full_refresh.json").delete_blob()
    except Exception:
        pass


def load_schema(blob_client, table) -> set | None:
    """Load the known column names for a table from ADLS. Returns None if not stored yet."""
    raw = _blob_download(blob_client, f"cursors/{table}/schema.json")
    if raw is None:
        return None
    return set(json.loads(raw))


def save_schema(blob_client, table, columns: set):
    path = f"cursors/{table}/schema.json"
    _blob_upload(blob_client, path, json.dumps(sorted(columns)).encode())


def check_schema(blob_client, table, records: list) -> bool:
    """Compare top-level keys in records against stored schema.
    If new columns are added, reset the cursor so the ingest re-fetches
    the full history with the new fields populated. Sets full_refresh flag
    so the merge step does an overwrite instead of a row-by-row MERGE.
    Returns True if new columns were detected (caller should restart).
    """
    batch_cols = set().union(*(r.keys() for r in records))
    known_cols = load_schema(blob_client, table)

    if known_cols is None:
        # First run — just store and move on
        save_schema(blob_client, table, batch_cols)
        return False

    added = batch_cols - known_cols
    removed = known_cols - batch_cols
    if added:
        print(f"[{table}] New columns detected: {added} — resetting cursor for full backfill")
        log.info(f"[{table}] new columns: {added} — cursor reset")
        save_schema(blob_client, table, batch_cols)
        save_cursor(blob_client, table, None)
        clear_pending_files(blob_client, table)
        set_full_refresh_flag(blob_client, table)
        return True

    if removed:
        print(f"[{table}] Columns removed: {removed} — updating schema (no re-fetch needed)")
        log.info(f"[{table}] columns removed: {removed}")
        save_schema(blob_client, table, batch_cols)

    return False


def clear_pending_files(blob_client, table):
    """Remove any pending batch files from a previous incomplete run."""
    try:
        container = blob_client.get_container_client(ADLS_CONTAINER)
        prefix = f"bronze/{table}/pending/"
        blobs = list(container.list_blobs(name_starts_with=prefix))
        for blob in blobs:
            container.get_blob_client(blob.name).delete_blob()
        if blobs:
            log.info(f"[{table}] cleared {len(blobs)} stale pending files")
    except Exception as e:
        log.error(f"[{table}] failed to clear pending files: {e}")


def write_batch(blob_client, table, records):
    """Write a page of records as NDJSON to bronze/{table}/pending/."""
    timestamp = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')
    path = f"bronze/{table}/pending/{timestamp}.json"
    ndjson = '\n'.join(json.dumps(r) for r in records)
    _blob_upload(blob_client, path, ndjson.encode('utf-8'))


def check_status(blob_client, table) -> tuple[int | None, dict | None]:
    """Probe the API with current cursor. Returns (status_code, response_body)."""
    cursor = load_cursor(blob_client, table)
    resp = call_api(endpoint=table, cursor=cursor or '', amount=1)
    if resp is None:
        return None, None
    body = resp.json()
    status = resp.status_code
    if status == 205:
        reason = body.get('reason', 'unknown')
        schema_changes = body.get('schema_changes', None)
        log.info(f"[{table}] FULL REFRESH — status={status} reason='{reason}' cursor_was='{cursor}'")
        if schema_changes:
            log.info(f"[{table}] schema changes: {json.dumps(schema_changes)}")
    else:
        log.info(f"[{table}] status={status} cursor='{cursor}'")
    return status, body


# ── Per-table ingestion ───────────────────────────────────────────────────────
def fetch_table(table, blob_client):
    print(f"[{table}] Starting")
    status, probe_body = check_status(blob_client, table)

    if status is None:
        log.error(f"[{table}] could not reach SeatGeek API")
        return table, 0

    full_refresh = status == 205
    if full_refresh:
        print(f"[{table}] Full refresh required (API returned 205)")
        save_cursor(blob_client, table, None)
        clear_pending_files(blob_client, table)
        set_full_refresh_flag(blob_client, table)

    cursor = load_cursor(blob_client, table)
    has_more = True
    total = 0
    batch = 0

    while has_more:
        resp = call_api(endpoint=table, cursor=cursor or '', amount=PER_PAGE)
        if resp is None:
            log.error(f"[{table}] pagination stopped at {total} records — API unreachable")
            break

        data = resp.json()
        has_more = data.get('has_more', False)
        cursor = data.get('cursor')
        records = data.get('data', [])

        if not records:
            print(f"[{table}] No new data")
            break

        if batch == 0:
            schema_changed = check_schema(blob_client, table, records)
            if schema_changed:
                # Cursor was reset to null and pending cleared.
                # Restart pagination from the beginning.
                cursor = None
                continue

        write_batch(blob_client, table, records)
        save_cursor(blob_client, table, cursor)

        batch += 1
        total += len(records)
        print(f"[{table}] Batch {batch}: {len(records)} records (total {total})")

    log.info(f"[{table}] finished — {total} records (full_refresh={full_refresh})")
    print(f"[{table}] Done — {total} records")
    return table, total


# ── Databricks job trigger ────────────────────────────────────────────────────
def trigger_databricks_job():
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_JOB_ID]):
        print("Databricks job trigger not configured — skipping auto-trigger")
        return

    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"job_id": int(DATABRICKS_JOB_ID)},
    )
    if resp.status_code == 200:
        run_id = resp.json().get('run_id')
        print(f"Databricks job triggered — run_id: {run_id}")
        print(f"Monitor at: {DATABRICKS_HOST}/#job/{DATABRICKS_JOB_ID}/run/{run_id}")
    else:
        log.error(f"failed to trigger Databricks job: {resp.text}")
        print(f"WARNING: Databricks job trigger failed — {resp.text}")


# ── Main ──────────────────────────────────────────────────────────────────────
TABLES = [
    'sales', 'clients', 'products', 'payments',
    'installments', 'manifests', 'attendance', 'pricing', 'attribution',
    'behaviors',
]


def main():
    blob_client = get_blob_client()

    print(f"Starting parallel ingest for {len(TABLES)} tables...")
    start = time.time()

    results = {}
    with ThreadPoolExecutor(max_workers=len(TABLES)) as executor:
        futures = {executor.submit(fetch_table, t, blob_client): t for t in TABLES}
        for future in as_completed(futures):
            table = futures[future]
            try:
                _, count = future.result()
                results[table] = count
            except Exception as e:
                log.error(f"[{table}] unhandled error: {e}")
                results[table] = 0

    elapsed = time.time() - start
    print(f"\nIngest complete in {elapsed:.1f}s")
    for t, n in results.items():
        print(f"  {t}: {n} records")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
