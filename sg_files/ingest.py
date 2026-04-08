"""SeatGeek file-feed ingest.

Pulls parquet files dropped by SeatGeek into s3://sg-data-external/commanders/{report}/
and copies them into ADLS bronze/{report}/pending/ for downstream Databricks merge.

State (key+etag of already-ingested files) is tracked in ADLS at
state/{report}/processed.json. Full-refresh files (full_refresh_*.parquet) set the
cursors/{report}/full_refresh.json flag so the merge step overwrites Silver.
"""

from dotenv import load_dotenv
import os
import json
import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ── AWS / S3 ──────────────────────────────────────────────────────────────────
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
S3_BUCKET = os.getenv('S3_BUCKET', 'sg-data-external')

# ── ADLS / Blob Storage ───────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv('ADLS_ACCOUNT_NAME')
ADLS_ACCOUNT_KEY = os.getenv('ADLS_ACCOUNT_KEY')
ADLS_CONTAINER = os.getenv('ADLS_CONTAINER', 'seatgeek')

# ── Databricks job trigger ────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_JOB_ID = os.getenv('DATABRICKS_JOB_ID')

# ── Reports ───────────────────────────────────────────────────────────────────
# Per commanders/data-dictionary.yaml:
#   listings      — method: UPSERT, PK: (listing_id, scrape_at)
#   event_mapping — method: REPLACE, PK: (sg_event_id)
# event_cart_abandonment is intentionally excluded.
REPORTS = {
    'listings':      {'prefix': 'commanders/listings/'},
    'event_mapping': {'prefix': 'commanders/event_mapping/'},
}

# Daily drops:  YYYY/MM/DD/NNNN_part_NN.parquet
# Full refresh: full_refresh_NNNN_part_NN.parquet
DAILY_REGEX = re.compile(r'\d{4}/\d{2}/\d{2}/\d{4}_part_\d{2}\.parquet$')
FULL_REFRESH_REGEX = re.compile(r'full_refresh_\d{4}_part_\d{2}\.parquet$')

S3_TRANSFER_WORKERS = 10
CHECKPOINT_EVERY = 100

# ── Logging ──────────────────────────────────────────────────────────────────
logging.getLogger('azure').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

log = logging.getLogger('sg_files')
log.setLevel(logging.INFO)
_fh = logging.FileHandler(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ingest_errors.log')
)
_fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
log.addHandler(_fh)


# ── Clients ──────────────────────────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )


def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


# ── ADLS helpers ──────────────────────────────────────────────────────────────
def _blob_upload(blob_client, path, data: bytes):
    container = blob_client.get_container_client(ADLS_CONTAINER)
    container.get_blob_client(path).upload_blob(data, overwrite=True)


def _blob_download(blob_client, path) -> bytes | None:
    try:
        container = blob_client.get_container_client(ADLS_CONTAINER)
        return container.get_blob_client(path).download_blob().readall()
    except Exception:
        return None


def load_processed(blob_client, report) -> dict:
    """Map of s3_key -> etag for files already ingested."""
    raw = _blob_download(blob_client, f"state/{report}/processed.json")
    if raw is None:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def save_processed(blob_client, report, processed: dict):
    _blob_upload(
        blob_client,
        f"state/{report}/processed.json",
        json.dumps(processed).encode(),
    )


def set_full_refresh_flag(blob_client, report):
    _blob_upload(
        blob_client,
        f"cursors/{report}/full_refresh.json",
        b'{"full_refresh": true}',
    )


# ── S3 listing ────────────────────────────────────────────────────────────────
def list_s3_files(s3, prefix):
    """Yield (key, etag, size) for all parquet objects under prefix."""
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            yield key, obj['ETag'].strip('"'), obj['Size']


# ── Transfer ──────────────────────────────────────────────────────────────────
def transfer_file(s3, blob_client, report, key, etag):
    """Download a parquet from S3 and upload it to ADLS bronze/{report}/pending/."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = obj['Body'].read()
    # Use the etag in the blob name to keep uploads idempotent.
    blob_path = f"bronze/{report}/pending/{etag}.parquet"
    _blob_upload(blob_client, blob_path, data)
    return key, etag, len(data)


def fetch_report(report, cfg, s3, blob_client):
    print(f"[{report}] Listing S3 files...")
    processed = load_processed(blob_client, report)

    new_files = []
    full_refresh_seen = False
    skipped_filter = 0
    for key, etag, size in list_s3_files(s3, cfg['prefix']):
        if not (DAILY_REGEX.search(key) or FULL_REFRESH_REGEX.search(key)):
            skipped_filter += 1
            continue
        if processed.get(key) == etag:
            continue
        if FULL_REFRESH_REGEX.search(key):
            full_refresh_seen = True
        new_files.append((key, etag, size))

    if skipped_filter:
        print(f"[{report}] Filtered out {skipped_filter} non-data parquet objects")

    if not new_files:
        print(f"[{report}] No new files")
        return report, 0

    print(f"[{report}] {len(new_files)} new files to download (full_refresh={full_refresh_seen})")

    if full_refresh_seen:
        print(f"[{report}] Setting full_refresh flag")
        set_full_refresh_flag(blob_client, report)

    transferred = 0
    failures = 0
    with ThreadPoolExecutor(max_workers=S3_TRANSFER_WORKERS) as ex:
        futures = {
            ex.submit(transfer_file, s3, blob_client, report, key, etag): (key, etag)
            for key, etag, _ in new_files
        }
        for fut in as_completed(futures):
            key, etag = futures[fut]
            try:
                fut.result()
                processed[key] = etag
                transferred += 1
                if transferred % CHECKPOINT_EVERY == 0:
                    print(f"[{report}] {transferred}/{len(new_files)} transferred — checkpointing state")
                    save_processed(blob_client, report, processed)
            except Exception as e:
                failures += 1
                log.error(f"[{report}] failed to transfer {key}: {e}")
                print(f"[{report}] FAIL {key}: {e}")

    save_processed(blob_client, report, processed)
    print(f"[{report}] Done — {transferred} transferred, {failures} failed")
    return report, transferred


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
def main():
    s3 = get_s3_client()
    blob_client = get_blob_client()

    print(f"Starting parallel ingest for {len(REPORTS)} SeatGeek file reports...")
    start = time.time()

    results = {}
    with ThreadPoolExecutor(max_workers=len(REPORTS)) as ex:
        futures = {
            ex.submit(fetch_report, report, cfg, s3, blob_client): report
            for report, cfg in REPORTS.items()
        }
        for fut in as_completed(futures):
            report = futures[fut]
            try:
                _, count = fut.result()
                results[report] = count
            except Exception as e:
                log.error(f"[{report}] unhandled error: {e}")
                results[report] = 0

    elapsed = time.time() - start
    print(f"\nIngest complete in {elapsed:.1f}s")
    for r, n in results.items():
        print(f"  {r}: {n} new files")

    if any(results.values()):
        trigger_databricks_job()
    else:
        print("No new files — skipping Databricks trigger")


if __name__ == "__main__":
    main()
