from dotenv import load_dotenv
import os
import logging
import json
import time
import datetime
import requests
import zipfile
import io
import csv
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ── Qualtrics API ────────────────────────────────────────────────────────────
API_BASE = "https://wft.pdx1.qualtrics.com/API/v3"
API_TOKEN = os.getenv("QUALTRICS_API_TOKEN")
HEADERS = {
    "Accept": "application/json",
    "X-API-TOKEN": API_TOKEN,
}

# ── ADLS / Blob Storage ─────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "seatgeek")

# ── Databricks job trigger ───────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_QUALTRICS_JOB_ID")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

log = logging.getLogger("qualtrics")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingest_errors.log"))
_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(_fh)


# ── ADLS helpers ─────────────────────────────────────────────────────────────
def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.blob.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


def _blob_upload(blob_client, path, data: bytes):
    container = blob_client.get_container_client(ADLS_CONTAINER)
    container.get_blob_client(path).upload_blob(data, overwrite=True)


def write_batch(blob_client, table, records, suffix=""):
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{timestamp}{suffix}.json"
    path = f"bronze/{table}/pending/{filename}"
    ndjson = "\n".join(json.dumps(r) for r in records)
    _blob_upload(blob_client, path, ndjson.encode("utf-8"))


# ── Qualtrics API ────────────────────────────────────────────────────────────
def fetch_surveys():
    resp = requests.get(f"{API_BASE}/surveys", headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()["result"]["elements"]


def fetch_survey_definitions(survey_id):
    """Fetch survey definition — returns question_id → {questionText, choices} mapping.

    Skips questions sitting in a Trash block; Qualtrics keeps deleted questions
    in the survey definition and they often share an export_tag with the
    replacement question, which would cause the response/definition join to
    return duplicate rows with stale text.
    """
    resp = requests.get(f"{API_BASE}/survey-definitions/{survey_id}", headers=HEADERS, timeout=30)
    resp.raise_for_status()
    result = resp.json()["result"]
    questions = result["Questions"]

    trashed_qids = set()
    for block in (result.get("Blocks") or {}).values():
        if block.get("Type") == "Trash":
            for el in block.get("BlockElements", []):
                if el.get("Type") == "Question" and el.get("QuestionID"):
                    trashed_qids.add(el["QuestionID"])

    definitions = []
    if isinstance(questions, list):
        items = ((q.get("QuestionID", ""), q) for q in questions)
    else:
        items = questions.items()
    for qid, q in items:
        if qid in trashed_qids:
            continue
        if not isinstance(q, dict):
            log.warning(f"[{survey_id}] skipping question {qid} — not a dict")
            continue
        try:
            qtype = q.get("QuestionType", "")
            entry = {
                "_survey_id": survey_id,
                "question_id": qid,
                "export_tag": q.get("DataExportTag", ""),
                "question_text": q.get("QuestionText", ""),
                "question_type": qtype,
            }
            # For MC questions, Qualtrics CSV exports replace the position-key
            # with the RecodeValue. Re-key choices so view lookups by
            # response_value resolve to the correct label. Matrix questions
            # store their choices as row labels (not rating values) and their
            # CSV exports do not appear to apply the recoding, so we leave
            # those alone.
            recode = q.get("RecodeValues") if qtype == "MC" else None
            recode = recode or {}
            choices = q.get("Choices", {})
            if choices and isinstance(choices, dict):
                entry["choices"] = {
                    str(recode.get(k, k)): (v.get("Display", "") if isinstance(v, dict) else str(v))
                    for k, v in choices.items()
                    if k != "Order"
                }
            elif choices and isinstance(choices, list):
                entry["choices"] = {
                    str(recode.get(str(i), i)): (c.get("Display", "") if isinstance(c, dict) else str(c))
                    for i, c in enumerate(choices)
                }
            # SubQuestions / Answers — used by Matrix questions for row labels
            subs = q.get("SubQuestions") or q.get("Answers") or {}
            if subs and isinstance(subs, dict):
                entry["sub_questions"] = {
                    k: v.get("Display", "") if isinstance(v, dict) else str(v)
                    for k, v in subs.items()
                    if k != "Order"
                }
            elif subs and isinstance(subs, list):
                entry["sub_questions"] = {
                    str(i): s.get("Display", "") if isinstance(s, dict) else str(s)
                    for i, s in enumerate(subs)
                }
            definitions.append(entry)
        except Exception as e:
            log.warning(f"[{survey_id}] skipping question {qid}: {e}")
    return definitions


def export_survey_responses(survey_id):
    export_url = f"{API_BASE}/surveys/{survey_id}/export-responses/"

    # Step 1: Create export
    resp = requests.post(
        export_url,
        headers={**HEADERS, "Content-Type": "application/json"},
        json={"format": "csv"},
        timeout=30,
    )
    resp.raise_for_status()
    progress_id = resp.json()["result"]["progressId"]

    # Step 2: Poll until complete
    while True:
        check = requests.get(f"{export_url}{progress_id}", headers=HEADERS, timeout=30)
        check.raise_for_status()
        result = check.json()["result"]
        if result["status"] == "complete":
            file_id = result["fileId"]
            break
        if result["status"] == "failed":
            raise Exception(f"Export failed for survey {survey_id}")
        time.sleep(1)

    # Step 3: Download and unzip
    download = requests.get(f"{export_url}{file_id}/file", headers=HEADERS, stream=True, timeout=60)
    download.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(download.content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
            rows = list(reader)
            # Skip the 2 Qualtrics metadata header rows
            return rows[2:] if len(rows) > 2 else rows


# ── Databricks job trigger ───────────────────────────────────────────────────
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
        run_id = resp.json().get("run_id")
        print(f"Databricks job triggered — run_id: {run_id}")
    else:
        log.error(f"failed to trigger Databricks job: {resp.text}")
        print(f"WARNING: Databricks job trigger failed — {resp.text}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    blob_client = get_blob_client()
    start = time.time()

    # ── Survey metadata ──────────────────────────────────────────────────────
    surveys = fetch_surveys()
    print(f"Found {len(surveys)} surveys")
    write_batch(blob_client, "qualtrics_surveys", surveys)
    print(f"[qualtrics_surveys] Wrote {len(surveys)} records")

    # ── Survey definitions + responses ───────────────────────────────────────
    total_responses = 0
    total_definitions = 0
    for survey in surveys:
        survey_id = survey["id"]
        survey_name = survey["name"]

        try:
            # Definitions (question text + choice mappings)
            defs = fetch_survey_definitions(survey_id)
            if defs:
                write_batch(blob_client, "qualtrics_definitions", defs, suffix=f"_{survey_id}")
                total_definitions += len(defs)
                print(f"[{survey_name}] Wrote {len(defs)} question definitions")

            # Responses
            rows = export_survey_responses(survey_id)
            if not rows:
                print(f"[{survey_name}] No responses — skipping")
                continue

            for row in rows:
                row["_survey_id"] = survey_id
                row["_survey_name"] = survey_name

            write_batch(blob_client, "qualtrics_responses", rows, suffix=f"_{survey_id}")
            total_responses += len(rows)
            print(f"[{survey_name}] Wrote {len(rows)} responses")
        except Exception as e:
            log.error(f"[{survey_name}] {e}")
            print(f"[{survey_name}] ERROR: {e}")

    elapsed = time.time() - start
    print(f"\nQualtrics ingest complete in {elapsed:.1f}s")
    print(f"  surveys: {len(surveys)}")
    print(f"  definitions: {total_definitions}")
    print(f"  responses: {total_responses}")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
