import os
import json
import time
import logging
import datetime
import requests
from html import unescape
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# ── Forsta API ────────────────────────────────────────────────────────────────
FORSTA_BASE   = os.getenv("FORSTA_BASE", "").rstrip("/")
FORSTA_API    = f"{FORSTA_BASE}/api/v1"
FORSTA_KEY    = os.getenv("FORSTA_API_KEY")
FEED_NAME     = os.getenv("FEED_NAME", "surveys_feed_new")
SURVEY_PATHS  = [p.strip() for p in os.getenv("SURVEY_PATHS", "").split(",") if p.strip()]

# Table alias map: survey_path → table name
_raw_aliases  = os.getenv("SURVEY_TABLE_ALIASES", "")
TABLE_ALIASES = {}
for pair in filter(None, [p.strip() for p in _raw_aliases.split(",")]):
    if "=" in pair:
        k, v = pair.split("=", 1)
        TABLE_ALIASES[k.strip()] = v.strip()

# ── ADLS ──────────────────────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY  = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER    = os.getenv("ADLS_CONTAINER", "vof")

# ── Databricks ────────────────────────────────────────────────────────────────
DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log = logging.getLogger("vof")
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


def write_batch(blob_client, table, records, suffix=""):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{ts}{suffix}.json"
    path = f"bronze/vof_{table}/pending/{filename}"
    ndjson = "\n".join(json.dumps(r) for r in records)
    _blob_upload(blob_client, path, ndjson.encode("utf-8"))
    return path


# ── Forsta API helpers ────────────────────────────────────────────────────────
def _clean(s):
    """Unescape HTML entities and fix double-encoded UTF-8."""
    if isinstance(s, str):
        s = unescape(s)
        try:
            s = s.encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
    return s


def get_feed(session, params):
    r = session.get(
        f"{FORSTA_API}/datafeed/{FEED_NAME}",
        params=params,
        timeout=120,
    )
    r.raise_for_status()
    return r.json()


def ack_feed(session, ack_token):
    r = session.post(
        f"{FORSTA_API}/datafeed/{FEED_NAME}/ack",
        json={"ack": ack_token},
        timeout=60,
    )
    r.raise_for_status()


def fetch_datamap(session, survey_path):
    r = session.get(
        f"{FORSTA_API}/surveys/{survey_path}/datamap",
        params={"format": "json"},
        timeout=120,
    )
    r.raise_for_status()
    variables = r.json().get("variables", [])
    rows = []
    for v in variables:
        rows.append({
            "survey_path": survey_path,
            "label":    _clean(v.get("label")),
            "title":    _clean(v.get("title")),
            "qtitle":   _clean(v.get("qtitle")),
            "qlabel":   _clean(v.get("qlabel")),
            "row":      v.get("row"),
            "col":      v.get("col"),
            "rowTitle": _clean(v.get("rowTitle")),
            "colTitle": _clean(v.get("colTitle")),
        })
    return rows


def table_name_for_path(path):
    return TABLE_ALIASES.get(path, path.replace("/", "_").replace("-", "_"))


# ── Databricks trigger ───────────────────────────────────────────────────────
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
        log.error(f"Databricks trigger failed: {resp.text}")
        print(f"WARNING: Databricks trigger failed — {resp.text}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    blob_client = get_blob_client()
    start = time.time()

    session = requests.Session()
    session.headers.update({
        "x-apikey": FORSTA_KEY,
        "Accept": "application/json",
    })

    params = {
        "paths":  ",".join(SURVEY_PATHS),
        "states": "live,closed,dev,testing",
        "cond":   "ALL",
        "limit":  10000,
    }

    total_responses = 0
    total_acks = 0
    tables_seen = set()

    while True:
        payload = get_feed(session, params)
        results = payload.get("results", [])
        complete = bool(payload.get("complete", True))
        ack_token = payload.get("ack")

        if not results:
            print("No new responses in feed")
            break

        # Flatten: extract $survey → survey_path
        for r in results:
            r["survey_path"] = r.pop("$survey", None)

        # Group by survey path and write each table's batch
        by_table = {}
        for r in results:
            table = table_name_for_path(r.get("survey_path", "unknown"))
            by_table.setdefault(table, []).append(r)

        for table, rows in by_table.items():
            path = write_batch(blob_client, table, rows)
            tables_seen.add(table)
            total_responses += len(rows)
            print(f"[vof_{table}] Wrote {len(rows)} responses → {path}")

        # Fetch and write datamap for each survey path in this batch
        paths_in_batch = set(r.get("survey_path") for r in results if r.get("survey_path"))
        for survey_path in paths_in_batch:
            try:
                datamap = fetch_datamap(session, survey_path)
                if datamap:
                    table = table_name_for_path(survey_path)
                    write_batch(blob_client, "survey_datamap", datamap, suffix=f"_{table}")
                    print(f"[vof_survey_datamap] Wrote {len(datamap)} variables for {survey_path}")
            except Exception as e:
                log.error(f"Datamap fetch failed for {survey_path}: {e}")
                print(f"WARNING: datamap fetch failed for {survey_path}: {e}")

        # ACK only after successful blob writes
        if ack_token:
            ack_feed(session, ack_token)
            total_acks += 1

        if complete:
            break

    elapsed = time.time() - start
    print(f"\nVOF ingest complete in {elapsed:.1f}s")
    print(f"  responses: {total_responses:,}")
    print(f"  tables: {', '.join(sorted(tables_seen)) or 'none'}")
    print(f"  acks: {total_acks}")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
