from dotenv import load_dotenv
import os, json, time, logging
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient

load_dotenv()

# ── Emplifi API ──────────────────────────────────────────────────────────────
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# ── ADLS / Blob Storage ─────────────────────────────────────────────────────
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_CONTAINER = os.getenv("ADLS_CONTAINER", "emplifi")

# ── Databricks job trigger ───────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")

BACKFILL_START = "2024-01-01"
MAX_WINDOW_DAYS = 365
POSTS_REFRESH_DAYS = 30
POSTS_BACKFILL_WINDOW_DAYS = 30
METRICS_REFRESH_DAYS = 7  # rolling window — re-fetch last N days each run so live/late-settling metrics catch up

# ── Logging ──────────────────────────────────────────────────────────────────
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger("emplifi")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "ingest_errors.log")
)
_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(_fh)


# ── Emplifi API ──────────────────────────────────────────────────────────────
def call_api(endpoint, payload, retries=3):
    url = f"https://api.emplifi.io{endpoint}"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    for attempt in range(retries):
        try:
            resp = requests.post(
                url,
                auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET),
                headers=headers,
                data=json.dumps(payload),
                timeout=120,
            )
            if resp.status_code == 200:
                body = resp.json()
                if body.get("success") is False:
                    log.error(f"[{endpoint}] API success=false: {json.dumps(body.get('errors', []))[:200]}")
                else:
                    return body
            else:
                log.error(f"[{endpoint}] HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as e:
            log.error(f"[{endpoint}] attempt {attempt + 1}/{retries}: {e}")
            time.sleep(2**attempt)
    return None


PLATFORMS = ["facebook", "instagram", "twitter", "linkedin", "tiktok"]


def get_profiles():
    """Fetch all connected profiles from the Emplifi API (GET per network)."""
    by_platform = {}
    for platform in PLATFORMS:
        url = f"https://api.emplifi.io/3/{platform}/profiles"
        try:
            resp = requests.get(
                url,
                auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET),
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=30,
            )
            if resp.status_code != 200:
                log.error(f"[profiles/{platform}] HTTP {resp.status_code}: {resp.text[:200]}")
                continue
            data = resp.json()
            print(f"  [{platform}] API response keys: {list(data.keys())}, sample: {json.dumps(data)[:300]}")
            profiles = data.get("profiles", [])
            by_platform[platform] = {
                "all": [p["id"] for p in profiles],
                "insights": [p["id"] for p in profiles if p.get("insights_enabled")],
            }
        except requests.RequestException as e:
            log.error(f"[profiles/{platform}] {e}")

    return by_platform


# ── ADLS helpers ─────────────────────────────────────────────────────────────
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


def save_cursor(blob_client, table, date_value):
    payload = {"last_date": date_value, "updated_at": datetime.utcnow().isoformat()}
    _blob_upload(
        blob_client, f"cursors/{table}/cursor.json", json.dumps(payload).encode()
    )


def load_cursor(blob_client, table):
    raw = _blob_download(blob_client, f"cursors/{table}/cursor.json")
    if raw is None:
        return None
    return json.loads(raw).get("last_date")


def write_batch(blob_client, table, records):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = f"bronze/{table}/pending/{ts}.json"
    ndjson = "\n".join(json.dumps(r) for r in records)
    _blob_upload(blob_client, path, ndjson.encode("utf-8"))


# ── Date windowing ───────────────────────────────────────────────────────────
def get_metrics_date_range(blob_client, table):
    last_date = load_cursor(blob_client, table)
    if last_date:
        # Rewind by METRICS_REFRESH_DAYS so today + recent days get re-pulled
        # every run. Emplifi metrics are live for "today" and Emplifi backfills
        # late-arriving engagement, so a rolling window keeps silver fresh.
        date_start = (
            datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=METRICS_REFRESH_DAYS - 1)
        ).strftime("%Y-%m-%d")
        if date_start < BACKFILL_START:
            date_start = BACKFILL_START
    else:
        date_start = BACKFILL_START

    date_end = datetime.now().strftime("%Y-%m-%d")

    start_dt = datetime.strptime(date_start, "%Y-%m-%d")
    end_dt = datetime.strptime(date_end, "%Y-%m-%d")
    if (end_dt - start_dt).days > MAX_WINDOW_DAYS:
        date_end = (start_dt + timedelta(days=MAX_WINDOW_DAYS)).strftime("%Y-%m-%d")

    return date_start, date_end


# ── Metrics parsing ──────────────────────────────────────────────────────────
def parse_metrics_response(response, extra_dim=None):
    header = response.get("header")
    if not header:
        return []

    dates = header[0]["rows"]
    profiles = header[1]["rows"]

    if extra_dim:
        extras = header[2]["rows"]
        metrics = header[3]["rows"]
        return [
            {"date": date_val, "profile": prof, extra_dim: ext_val,
             **{m: v for m, v in zip(metrics, values) if v is not None}}
            for date_idx, date_val in enumerate(dates)
            for prof_idx, prof in enumerate(profiles)
            for ext_idx, ext_val in enumerate(extras)
            for values in [response["data"][date_idx][prof_idx][ext_idx]]
        ]
    else:
        metrics = header[2]["rows"]
        return [
            {"date": date_val, "profile": prof,
             **{m: v for m, v in zip(metrics, values) if v is not None}}
            for date_idx, date_val in enumerate(dates)
            for prof_idx, prof in enumerate(profiles)
            for values in [response["data"][date_idx][prof_idx]]
        ]


# ── Metrics ingestion ────────────────────────────────────────────────────────
def fetch_metrics_table(blob_client, table, endpoint, payload, extra_dim=None):
    today = datetime.now().strftime("%Y-%m-%d")
    total = 0

    while True:
        date_start, date_end = get_metrics_date_range(blob_client, table)

        if date_start >= today:
            print(f"[{table}] Up to date")
            break

        print(f"[{table}] {date_start} → {date_end}")
        payload["date_start"] = date_start
        payload["date_end"] = date_end

        response = call_api(endpoint, payload)

        if not response or not response.get("header"):
            print(f"[{table}] No data for window — skipping")
            save_cursor(blob_client, table, date_end)
            if date_end >= today:
                break
            continue

        records = parse_metrics_response(response, extra_dim)
        if records:
            write_batch(blob_client, table, records)
            total += len(records)
            print(f"[{table}] {len(records)} records")

        save_cursor(blob_client, table, date_end)
        if date_end >= today:
            break

    print(f"[{table}] Done — {total} total records")
    return table, total


# ── Posts ingestion ──────────────────────────────────────────────────────────
def _serialize_value(v):
    return json.dumps(v) if isinstance(v, (dict, list)) else v


def _max_created_time(posts):
    """Extract max created_time (date portion) from a batch of posts."""
    dates = [p.get("created_time", "")[:10] for p in posts if p.get("created_time")]
    return max(dates) if dates else None


def _fetch_posts_window(table, blob_client, endpoint, payload, date_start, date_end):
    """Fetch all posts in a date window, writing batches to ADLS as we go."""
    base = dict(payload)
    base["date_start"] = date_start
    base["date_end"] = date_end

    total = 0
    page = 0
    max_date = None
    response = call_api(endpoint, base)
    if not response:
        return 0, None

    posts = response.get("data", {}).get("posts", [])
    nxt = response.get("data", {}).get("next")
    if posts:
        records = [{k: _serialize_value(v) for k, v in p.items()} for p in posts]
        write_batch(blob_client, table, records)
        total += len(records)
        page += 1
        batch_max = _max_created_time(posts)
        if batch_max and (not max_date or batch_max > max_date):
            max_date = batch_max

    while nxt:
        time.sleep(5)
        response = call_api(endpoint, {"after": nxt})
        if not response:
            break
        posts = response.get("data", {}).get("posts", [])
        nxt = response.get("data", {}).get("next")
        if not posts:
            break
        records = [{k: _serialize_value(v) for k, v in p.items()} for p in posts]
        write_batch(blob_client, table, records)
        total += len(records)
        page += 1
        batch_max = _max_created_time(posts)
        if batch_max and (not max_date or batch_max > max_date):
            max_date = batch_max
        if page % 10 == 0:
            print(f"[{table}] ...{total} posts so far (page {page})")

    return total, max_date


def _get_latest_post_date(blob_client, table):
    """Scan the most recent blob file to find the max created_time."""
    try:
        container = blob_client.get_container_client(ADLS_CONTAINER)
        # Check both pending and archive
        blobs = []
        for prefix in [f"bronze/{table}/pending/", f"bronze/{table}/archive/"]:
            blobs.extend(container.list_blobs(name_starts_with=prefix))
        if not blobs:
            return None
        # Sort by name (timestamp-based) and read the latest
        latest = sorted(blobs, key=lambda b: b.name)[-1]
        data = container.get_blob_client(latest.name).download_blob().readall().decode("utf-8")
        max_date = None
        for line in data.strip().split("\n"):
            row = json.loads(line)
            ct = row.get("created_time", "")[:10]
            if ct and (not max_date or ct > max_date):
                max_date = ct
        return max_date
    except Exception as e:
        log.error(f"[{table}] Error scanning blob files: {e}")
        return None


def fetch_posts_table(blob_client, table, endpoint, payload):
    today_dt = datetime.now()
    today = today_dt.strftime("%Y-%m-%d")
    total = 0
    max_date_seen = None

    last_date = load_cursor(blob_client, table)

    if not last_date:
        # Check blob files for existing data
        blob_max = _get_latest_post_date(blob_client, table)
        if blob_max:
            # Data exists in blob — pick up 30 days before latest
            start_dt = datetime.strptime(blob_max, "%Y-%m-%d") - timedelta(days=POSTS_REFRESH_DAYS)
            print(f"[{table}] Found data in blob up to {blob_max} — refreshing from {start_dt.strftime('%Y-%m-%d')}")
        else:
            # True backfill
            start_dt = datetime.strptime(BACKFILL_START, "%Y-%m-%d")
            print(f"[{table}] Backfill from {BACKFILL_START}")

        while start_dt < today_dt:
            end_dt = min(start_dt + timedelta(days=POSTS_BACKFILL_WINDOW_DAYS), today_dt)
            ds = start_dt.strftime("%Y-%m-%d")
            de = end_dt.strftime("%Y-%m-%d")
            print(f"[{table}] Window {ds} → {de}")

            count, window_max = _fetch_posts_window(table, blob_client, endpoint, payload, ds, de)
            total += count
            if window_max and (not max_date_seen or window_max > max_date_seen):
                max_date_seen = window_max
            if count:
                print(f"[{table}] {count} posts")

            start_dt = end_dt + timedelta(days=1)
            time.sleep(5)
    else:
        # Daily: 30 days before the last actual post date, chunked into 90-day windows
        start_dt = datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=POSTS_REFRESH_DAYS)
        print(f"[{table}] Rolling refresh {start_dt.strftime('%Y-%m-%d')} → {today} (last post: {last_date})")

        while start_dt < today_dt:
            end_dt = min(start_dt + timedelta(days=POSTS_BACKFILL_WINDOW_DAYS), today_dt)
            ds = start_dt.strftime("%Y-%m-%d")
            de = end_dt.strftime("%Y-%m-%d")
            print(f"[{table}] Window {ds} → {de}")

            count, window_max = _fetch_posts_window(table, blob_client, endpoint, payload, ds, de)
            total += count
            if window_max and (not max_date_seen or window_max > max_date_seen):
                max_date_seen = window_max
            if count:
                print(f"[{table}] {count} posts")

            start_dt = end_dt + timedelta(days=1)
            time.sleep(5)

    # Only update cursor if we actually fetched posts
    cursor_date = max_date_seen or last_date
    if cursor_date:
        save_cursor(blob_client, table, cursor_date)
    print(f"[{table}] Done — {total} posts (latest: {cursor_date or 'none'})")
    return table, total


# ── Table definitions ────────────────────────────────────────────────────────
METRIC_TABLES = [
    {
        "table": "facebook_metrics",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": False,
        "metrics": ["fans_lifetime", "fans_change"],
    },
    {
        "table": "facebook_insights_basic",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": True,
        "metrics": [
            "insights_fan_adds", "insights_fan_adds_unique",
            "insights_fan_removes", "insights_fan_removes_unique",
            "insights_post_reach", "insights_reach",
            "insights_reach_28_days", "insights_reach_7_days",
            "insights_video_complete_views_30s_repeat_views",
            "insights_video_complete_views_30s_unique",
            "insights_video_repeat_views",
            "insights_video_views_unique", "insights_views",
        ],
    },
    {
        "table": "facebook_insights_fans_lifetime",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": True,
        "metrics": ["insights_fans_lifetime"],
        "extra_dim": "city",
    },
    {
        "table": "facebook_insights_impressions",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": True,
        "metrics": ["insights_impressions"],
        "extra_dim": "activity_type",
    },
    {
        "table": "facebook_insights_reactions",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": True,
        "metrics": ["insights_reactions"],
        "extra_dim": "reaction_type",
    },
    {
        "table": "facebook_insights_video_complete_views_30s",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": True,
        "metrics": ["insights_video_complete_views_30s"],
        "extra_dim": "post_attribution",  # Fixed: was 'reaction_type' in original
    },
    {
        "table": "facebook_insights_video_views",
        "endpoint": "/3/facebook/metrics",
        "platform": "facebook",
        "insights": True,
        "metrics": ["insights_video_views"],
        "extra_dim": "post_attribution",  # Fixed: was 'reaction_type' in original
    },
    {
        "table": "instagram_metrics",
        "endpoint": "/3/instagram/metrics",
        "platform": "instagram",
        "insights": False,
        "metrics": ["followers_change", "followers_lifetime", "following_change", "following_lifetime"],
    },
    {
        "table": "instagram_insights_basic",
        "endpoint": "/3/instagram/metrics",
        "platform": "instagram",
        "insights": True,
        "metrics": [
            "insights_impressions", "insights_impressions_28_days",
            "insights_impressions_7_days", "insights_reach",
            "insights_reach_28_days", "insights_reach_7_days",
        ],
    },
    {
        "table": "instagram_insights_followers_gender_age",
        "endpoint": "/3/instagram/metrics",
        "platform": "instagram",
        "insights": True,
        "metrics": ["insights_followers"],
        "extra_dim": "gender_age",
    },
    {
        "table": "instagram_insights_followers_city",
        "endpoint": "/3/instagram/metrics",
        "platform": "instagram",
        "insights": True,
        "metrics": ["insights_followers"],
        "extra_dim": "city",
    },
    {
        "table": "twitter_metrics",
        "endpoint": "/3/twitter/metrics",
        "platform": "twitter",
        "insights": False,
        "metrics": [
            "ff_ratio", "followers_change", "followers_lifetime",
            "following_change", "following_lifetime",
            "listed_change", "listed_lifetime",
        ],
    },
    {
        "table": "linkedin_metrics",
        "endpoint": "/3/linkedin/metrics",
        "platform": "linkedin",
        "insights": False,
        "metrics": ["followers_change", "followers_lifetime"],
    },
    {
        "table": "tiktok_insights",
        "endpoint": "/3/tiktok/metrics",
        "platform": "tiktok",
        "insights": True,
        "metrics": [
            "insights_engagements", "insights_fans_change",
            "insights_fans_lifetime", "insights_profile_views",
            "insights_video_views",
        ],
    },
]

POST_TABLES = [
    {
        "table": "facebook_posts_basic",
        "endpoint": "/3/facebook/page/posts",
        "platform": "facebook",
        "insights": False,
        "fields": [
            "attachments", "authorId", "comments", "comments_sentiment", "content",
            "content_type", "created_time", "deleted", "grade", "id", "interactions",
            "interactions_per_1k_fans", "media_type", "origin", "page",
            "post_attribution", "post_labels", "profileId", "published", "reactions",
            "reactions_by_type", "sentiment", "shares", "spam",
            "universal_video_id", "url", "video",
        ],
    },
    {
        "table": "facebook_posts_insights",
        "endpoint": "/3/facebook/page/posts",
        "platform": "facebook",
        "insights": True,
        "fields": [
            "insights_engaged_users", "insights_engagements", "insights_impressions",
            "insights_impressions_by_post_attribution", "insights_impressions_engagement_rate",
            "insights_interactions", "insights_interactions_by_interaction_type",
            "insights_negative_feedback_unique", "insights_post_clicks",
            "insights_post_clicks_by_clicks_type", "insights_post_clicks_unique",
            "id", "insights_reach", "insights_reach_by_post_attribution",
            "insights_reach_engagement_rate", "insights_reactions",
            "insights_reactions_by_type", "insights_video_view_time",
            "insights_video_views", "created_time",
        ],
    },
    {
        "table": "instagram_posts_basic",
        "endpoint": "/3/instagram/profile/posts",
        "platform": "instagram",
        "insights": False,
        "fields": [
            "attachments", "authorId", "comments", "comments_sentiment", "content",
            "content_type", "created_time", "grade", "id", "interactions",
            "interactions_per_1k_fans", "likes", "media_type", "origin", "page",
            "post_attribution", "post_labels", "profileId", "url",
        ],
    },
]


# ── Payload builders ─────────────────────────────────────────────────────────
def _build_metrics_payload(profile_ids, table_def):
    dims = [{"type": "date.day"}, {"type": "profile"}]
    if table_def.get("extra_dim"):
        dims.append({"type": table_def["extra_dim"]})
    return {
        "profiles": profile_ids,
        "metrics": table_def["metrics"],
        "dimensions": dims,
    }


def _build_posts_payload(profile_ids, table_def):
    return {
        "profiles": profile_ids,
        "fields": table_def["fields"],
        "limit": 100,
    }


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
        log.error(f"Databricks trigger failed: {resp.text}")
        print(f"WARNING: Databricks trigger failed — {resp.text}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    blob_client = get_blob_client()

    print("Fetching profiles from Emplifi API...")
    profiles = get_profiles()
    if not profiles:
        print("ERROR: No profiles returned from API")
        return

    for plat, ids in profiles.items():
        print(f"  {plat}: {len(ids['all'])} profiles ({len(ids['insights'])} insights)")

    tasks = []

    for tdef in METRIC_TABLES:
        plat = tdef["platform"]
        if plat not in profiles:
            print(f"[{tdef['table']}] No {plat} profiles — skipping")
            continue
        pids = profiles[plat]["insights" if tdef["insights"] else "all"]
        if not pids:
            print(f"[{tdef['table']}] No eligible profiles — skipping")
            continue
        payload = _build_metrics_payload(pids, tdef)
        tasks.append(("metrics", tdef["table"], tdef["endpoint"], payload, tdef.get("extra_dim")))

    for tdef in POST_TABLES:
        plat = tdef["platform"]
        if plat not in profiles:
            print(f"[{tdef['table']}] No {plat} profiles — skipping")
            continue
        pids = profiles[plat]["insights" if tdef["insights"] else "all"]
        if not pids:
            print(f"[{tdef['table']}] No eligible profiles — skipping")
            continue
        payload = _build_posts_payload(pids, tdef)
        tasks.append(("posts", tdef["table"], tdef["endpoint"], payload, None))

    # Split into metrics (parallel) and posts (sequential with rate limiting)
    metric_tasks = [t for t in tasks if t[0] == "metrics"]
    post_tasks = [t for t in tasks if t[0] == "posts"]

    print(f"\nStarting ingest: {len(metric_tasks)} metric tables (parallel), {len(post_tasks)} post tables (sequential)...")
    start = time.time()
    results = {}

    def run_task(task):
        kind, table, endpoint, payload, extra_dim = task
        if kind == "metrics":
            return fetch_metrics_table(blob_client, table, endpoint, payload, extra_dim)
        return fetch_posts_table(blob_client, table, endpoint, payload)

    # Metrics: parallel
    if metric_tasks:
        with ThreadPoolExecutor(max_workers=min(len(metric_tasks), 8)) as executor:
            futures = {executor.submit(run_task, t): t[1] for t in metric_tasks}
            for future in as_completed(futures):
                table = futures[future]
                try:
                    _, count = future.result()
                    results[table] = count
                except Exception as e:
                    log.error(f"[{table}] unhandled error: {e}")
                    print(f"[{table}] ERROR: {e}")
                    results[table] = 0

    # Posts: sequential to respect rate limits
    for task in post_tasks:
        _, table, endpoint, payload, _ = task
        try:
            _, count = fetch_posts_table(blob_client, table, endpoint, payload)
            results[table] = count
        except Exception as e:
            log.error(f"[{table}] unhandled error: {e}")
            print(f"[{table}] ERROR: {e}")
            results[table] = 0

    elapsed = time.time() - start
    print(f"\nIngest complete in {elapsed:.1f}s")
    for t, n in sorted(results.items()):
        print(f"  {t}: {n} records")

    trigger_databricks_job()


if __name__ == "__main__":
    main()
