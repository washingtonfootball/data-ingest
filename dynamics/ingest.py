import os
import json
import time
import logging
import datetime
import argparse
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TENANT_ID     = os.getenv("TENANT_ID")
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")          # e.g. https://org.crm.dynamics.com

ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY  = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER    = os.getenv("ADLS_CONTAINER", "dynamics")

DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DYNAMICS_DATABRICKS_JOB_ID")

# Dataverse entity → ADLS/Databricks table name
ENTITIES = {
    "systemusers": "users",
}

API_VERSION    = "v9.2"
PAGE_SIZE      = 5000   # Dataverse max page size
RETRY_STATUSES = {408, 429, 500, 502, 503, 504}
BACKOFFS       = [0, 30, 60, 120, 300]

logging.basicConfig(
    filename="dynamics_ingest.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ── Auth ──────────────────────────────────────────────────────────────────────
_token_cache: dict = {}


def get_token() -> str:
    now = time.time()
    if _token_cache.get("expires_at", 0) > now + 30:
        return _token_cache["token"]

    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope":         f"{DATAVERSE_URL}/.default",
        },
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    _token_cache["token"]      = body["access_token"]
    _token_cache["expires_at"] = now + body["expires_in"]
    return _token_cache["token"]


def auth_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_token()}",
        "Accept":        "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version":    "4.0",
    }


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


def _download(blob_client: BlobServiceClient, path: str) -> bytes | None:
    try:
        return blob_client.get_container_client(ADLS_CONTAINER) \
                          .get_blob_client(path) \
                          .download_blob().readall()
    except Exception:
        return None


def load_cursor(blob_client: BlobServiceClient, table: str) -> str | None:
    """Return the stored OData deltaLink, or None for a full load."""
    raw = _download(blob_client, f"cursors/dynamics_{table}/cursor.json")
    if raw is None:
        return None
    return json.loads(raw).get("delta_link")


def save_cursor(blob_client: BlobServiceClient, table: str, delta_link: str):
    payload = {"delta_link": delta_link, "updated_at": datetime.datetime.utcnow().isoformat()}
    _upload(blob_client, f"cursors/dynamics_{table}/cursor.json", json.dumps(payload).encode())


def write_ndjson(blob_client: BlobServiceClient, table: str, rows: list):
    ts    = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    ndjson = "\n".join(json.dumps(r) for r in rows)
    _upload(blob_client, f"bronze/dynamics_{table}/pending/{ts}.json", ndjson.encode())


# ── Dataverse CDC fetch ───────────────────────────────────────────────────────
def _get_with_retry(session: requests.Session, url: str, headers: dict) -> requests.Response:
    last_err = None
    for attempt, delay in enumerate(BACKOFFS):
        if delay:
            print(f"    retry {attempt}/{len(BACKOFFS)-1} — waiting {delay}s (last error: {last_err})")
            time.sleep(delay)
        try:
            r = session.get(url, headers=headers, timeout=60)
            if r.status_code in RETRY_STATUSES:
                last_err = f"HTTP {r.status_code} — {r.text[:300]}"
                continue
            r.raise_for_status()
            return r
        except requests.HTTPError:
            raise
        except requests.RequestException as e:
            last_err = str(e)
            continue
    raise RuntimeError(f"All retries exhausted — last error: {last_err}")


def fetch_entity(entity: str, table: str, blob_client: BlobServiceClient) -> tuple[list, str]:
    """
    Fetch all records for a Dataverse entity using OData delta tracking.

    First run: full load with Prefer: odata.track-changes → saves deltaLink as cursor.
    Subsequent runs: uses stored deltaLink → only changed/deleted records returned.

    Deleted records from Dataverse have a '@removed' annotation and are tagged
    with _cdc_operation='D'. All others get _cdc_operation='U' (upsert).

    Returns (rows, new_delta_link).
    """
    cursor = load_cursor(blob_client, table)

    if cursor:
        print(f"  Incremental — using stored deltaLink")
        start_url = cursor
        extra_headers = {}
    else:
        print(f"  Full load — requesting delta tracking")
        start_url   = f"{DATAVERSE_URL}/api/data/{API_VERSION}/{entity}"
        extra_headers = {"Prefer": f"odata.track-changes,odata.maxpagesize={PAGE_SIZE}"}

    all_rows   = []
    delta_link = None
    url        = start_url

    with requests.Session() as session:
        while url:
            headers = {**auth_headers(), **extra_headers}
            r       = _get_with_retry(session, url, headers)
            body    = r.json()

            for record in body.get("value", []):
                if "@removed" in record:
                    record["_cdc_operation"] = "D"
                else:
                    record["_cdc_operation"] = "U"
                all_rows.append(record)

            next_link  = body.get("@odata.nextLink")
            delta_link = body.get("@odata.deltaLink", delta_link)

            print(f"    page fetched: {len(body.get('value', [])):,} records")

            # deltaLink is only present on the final page — keep following nextLink until gone
            url = next_link

    return all_rows, delta_link


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
    parser = argparse.ArgumentParser(description="Dynamics Dataverse → ADLS Bronze (CDC)")
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="Ignore stored deltaLink and do a full load.",
    )
    args = parser.parse_args()

    blob_client = get_blob_client()

    for entity, table in ENTITIES.items():
        print(f"\n[{table}] Fetching {entity}...")

        if args.full_refresh:
            # Wipe cursor so fetch_entity starts from scratch
            _upload(blob_client, f"cursors/dynamics_{table}/cursor.json", b"{}")
            print(f"  Full refresh requested — cursor cleared")

        rows, delta_link = fetch_entity(entity, table, blob_client)

        if not rows:
            print(f"[{table}] No changes since last run")
        else:
            write_ndjson(blob_client, table, rows)
            upserts = sum(1 for r in rows if r["_cdc_operation"] == "U")
            deletes = sum(1 for r in rows if r["_cdc_operation"] == "D")
            print(f"[{table}] {upserts:,} upserts, {deletes:,} deletes → bronze/dynamics_{table}/pending/")

        if delta_link:
            save_cursor(blob_client, table, delta_link)
            print(f"[{table}] deltaLink saved")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
