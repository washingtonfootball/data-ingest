from dotenv import load_dotenv
import os
import io
import logging
import json
import requests
import time
import datetime
import paramiko
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ── SFTP credentials ─────────────────────────────────────────────────────────
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_BASE = os.getenv("SFTP_BASE", "/IVRSS/PROD")

# ── ADLS / Blob Storage ──────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "fanatics")

# ── Databricks job trigger ────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")

CATEGORIES = ["sales", "location", "itemMaster", "inventory", "department", "category"]
BATCH_FLUSH_SIZE = 10_000  # flush NDJSON batch after this many records
SFTP_WORKERS = 8  # parallel SFTP download threads

# ── Logging ──────────────────────────────────────────────────────────────────
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("paramiko").setLevel(logging.WARNING)

log = logging.getLogger("fanatics")
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
    container = blob_client.get_container_client(ADLS_CONTAINER)
    container.get_blob_client(path).upload_blob(data, overwrite=True)


def _blob_download(blob_client, path):
    try:
        container = blob_client.get_container_client(ADLS_CONTAINER)
        return container.get_blob_client(path).download_blob().readall()
    except Exception:
        return None


def write_batch(blob_client, category, records):
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = f"bronze/{category}/pending/{timestamp}.json"
    ndjson = "\n".join(json.dumps(r) for r in records)
    _blob_upload(blob_client, path, ndjson.encode("utf-8"))
    return path


# ── File tracking in blob ─────────────────────────────────────────────────────
def load_processed(blob_client, category):
    raw = _blob_download(blob_client, f"tracking/{category}/processed.json")
    if raw is None:
        return set()
    return set(json.loads(raw))


def save_processed(blob_client, category, filenames):
    path = f"tracking/{category}/processed.json"
    _blob_upload(blob_client, path, json.dumps(sorted(filenames)).encode())


# ── SFTP helpers ──────────────────────────────────────────────────────────────
def sftp_connect():
    for attempt in range(3):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS, timeout=30)
            return ssh, ssh.open_sftp()
        except Exception as e:
            log.error(f"SFTP connect attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError("Failed to connect to SFTP after 3 attempts")


def sftp_read_json(sftp, remote_path):
    """Download a JSON file from SFTP into memory and parse it."""
    buf = io.BytesIO()
    sftp.getfo(remote_path, buf)
    buf.seek(0)
    raw = buf.read()
    if not raw or raw.strip() == b"":
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Try JSON Lines format
        lines = raw.decode("utf-8").strip().split("\n")
        data = [json.loads(line) for line in lines if line.strip()]
    if isinstance(data, dict):
        data = [data]
    return data


# ── Per-category processing ──────────────────────────────────────────────────
def _download_one(remote_dir, fname):
    """Download a single file using its own SFTP connection (for thread pool)."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SFTP_HOST, username=SFTP_USER, password=SFTP_PASS, timeout=30)
    sftp = ssh.open_sftp()
    try:
        buf = io.BytesIO()
        sftp.getfo(f"{remote_dir}/{fname}", buf)
        buf.seek(0)
        raw = buf.read()
        if not raw or raw.strip() == b"":
            return fname, []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            lines = raw.decode("utf-8").strip().split("\n")
            data = [json.loads(line) for line in lines if line.strip()]
        if isinstance(data, dict):
            data = [data]
        return fname, data
    finally:
        sftp.close()
        ssh.close()


def process_category(blob_client, sftp, category):
    processed = load_processed(blob_client, category)
    remote_dir = f"{SFTP_BASE.rstrip('/')}/{category}"

    try:
        all_files = sftp.listdir(remote_dir)
    except FileNotFoundError:
        print(f"[{category}] Remote path not found: {remote_dir}")
        return 0, 0

    new_files = sorted(f for f in all_files if f not in processed)
    if not new_files:
        print(f"[{category}] No new files")
        return 0, 0

    print(f"[{category}] {len(new_files)} new files to process (of {len(all_files)} total)")

    total_records = 0
    total_files = 0
    batch_records = []
    batch_source_files = []

    # Process in parallel chunks
    chunk_size = 200
    for chunk_start in range(0, len(new_files), chunk_size):
        chunk = new_files[chunk_start:chunk_start + chunk_size]

        with ThreadPoolExecutor(max_workers=SFTP_WORKERS) as executor:
            futures = {
                executor.submit(_download_one, remote_dir, fname): fname
                for fname in chunk
            }
            for future in as_completed(futures):
                fname = futures[future]
                try:
                    _, records = future.result()
                except Exception as e:
                    log.error(f"[{category}] Failed to read {fname}: {e}")
                    continue

                if not records:
                    processed.add(fname)
                    total_files += 1
                    continue

                batch_records.extend(records)
                batch_source_files.append(fname)

                if len(batch_records) >= BATCH_FLUSH_SIZE:
                    path = write_batch(blob_client, category, batch_records)
                    total_records += len(batch_records)
                    print(f"[{category}] Wrote {len(batch_records):,} records from {len(batch_source_files)} files")
                    batch_records = []
                    for sf in batch_source_files:
                        processed.add(sf)
                    total_files += len(batch_source_files)
                    batch_source_files = []

        # Flush remainder after each chunk
        if batch_records:
            write_batch(blob_client, category, batch_records)
            total_records += len(batch_records)
            print(f"[{category}] Wrote {len(batch_records):,} records from {len(batch_source_files)} files")
            for sf in batch_source_files:
                processed.add(sf)
            total_files += len(batch_source_files)
            batch_records = []
            batch_source_files = []

        # Save progress after each chunk (resume-safe)
        save_processed(blob_client, category, processed)
        done = chunk_start + len(chunk)
        print(f"[{category}] Progress: {done}/{len(new_files)} files")

    print(f"[{category}] Done — {total_records:,} records from {total_files} files")
    return total_files, total_records


# ── Databricks job trigger ────────────────────────────────────────────────────
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
        run_id = resp.json().get("run_id")
        print(f"Databricks job triggered — run_id: {run_id}")
    else:
        log.error(f"Failed to trigger Databricks job: {resp.text}")
        print(f"WARNING: Databricks job trigger failed — {resp.text}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start = time.time()
    print(f"Run starting at {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

    blob_client = get_blob_client()
    ssh, sftp = sftp_connect()

    results = {}
    try:
        for cat in CATEGORIES:
            try:
                files, records = process_category(blob_client, sftp, cat)
                results[cat] = (files, records)
            except Exception as e:
                log.error(f"[{cat}] unhandled error: {e}")
                print(f"[{cat}] ERROR: {e}")
                results[cat] = (0, 0)
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            ssh.close()
        except Exception:
            pass

    elapsed = time.time() - start
    print(f"\nIngest complete in {elapsed:.1f}s")
    for cat, (files, records) in results.items():
        print(f"  {cat}: {files} files, {records:,} records")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
