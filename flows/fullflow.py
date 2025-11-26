#!/usr/bin/env python3
from prefect import flow, task
import requests, time, pandas as pd, duckdb
from datetime import datetime, timezone
from prefect.blocks.system import Secret

# -------------------------
# Configuration
# -------------------------

REPOS = [
    "torvalds/linux",
    "microsoft/vscode",
    "facebook/react",
    "pytorch/pytorch",
    "kubernetes/kubernetes",
    "vercel/next.js",
    "flutter/flutter",
    "apache/spark",
    "ansible/ansible",
    "godotengine/godot"
]

BASE_URL = "https://api.github.com/repos/{repo}/events"
S3_DB_PATH = "s3://mistelehendershot-dp3/github.duckdb"
AWS_REGION = "us-east-1"

# Load secrets from Prefect Cloud
GITHUB_PAT = Secret.load("github-pat").get()

headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_PAT}"
}

RECORD_LIMIT = 300
SLEEP_AFTER_RUN = 120


# -------------------------
# Tasks
# -------------------------

@task(retries=3, retry_delay_seconds=15)
def fetch_repo_events(repo_name: str, max_pages: int = 5):
    """Fetch GitHub events with rate-limit handling."""
    all_events = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL.format(repo=repo_name)}?per_page=100&page={page}"
        r = requests.get(url, headers=headers)

        # Rate limits
        if r.status_code == 403:
            reset_time = r.headers.get("X-RateLimit-Reset")
            if reset_time:
                wait_sec = max(
                    (datetime.fromtimestamp(int(reset_time), tz=timezone.utc) -
                     datetime.now(timezone.utc)).total_seconds(),
                    60
                )
                print(f"⚠️ Rate limit hit for {repo_name}. Waiting {int(wait_sec)}s.")
                time.sleep(wait_sec)
                return fetch_repo_events.fn(repo_name, max_pages=max_pages)
            else:
                print("⚠️ 403 with no reset header — sleeping 5 min.")
                time.sleep(300)
                return fetch_repo_events.fn(repo_name, max_pages=max_pages)

        elif r.status_code != 200:
            print(f"Failed {repo_name} page {page}: {r.status_code}")
            break

        events = r.json()
        if not events:
            break

        all_events.extend(events)

        # Respect GitHub polling interval
        poll_interval = int(r.headers.get("X-Poll-Interval", "60"))
        time.sleep(poll_interval)

    print(f"{repo_name}: fetched {len(all_events)} events.")
    return all_events


@task
def flatten_events(events, repo_name):
    """Flatten GitHub API JSON."""
    rows = [{
        "id": e.get("id"),
        "type": e.get("type"),
        "repo": e.get("repo", {}).get("name", repo_name),
        "actor": e.get("actor", {}).get("login"),
        "org": e.get("org", {}).get("login"),
        "created_at": e.get("created_at"),
        "action": e.get("payload", {}).get("action"),
        "ref": e.get("payload", {}).get("ref"),
        "ref_type": e.get("payload", {}).get("ref_type"),
        "inserted_at": datetime.utcnow(),
    } for e in events]

    return pd.DataFrame(rows)


from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

@task
def append_duckdb_s3(df):

    # Load AWS credentials and S3 bucket block
    aws_creds = AwsCredentials.load("aws-creds")
    s3 = S3Bucket.load("dp3-bucket")

    # This is your S3 DuckDB destination folder
    S3_DB_PATH = "s3://mistelehendershot-dp3/github.duckdb/"

    # 1. Create or connect to a local DuckDB file inside the container
    conn = duckdb.connect("local.duckdb")

    # 2. Make table if missing
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id VARCHAR,
            type VARCHAR,
            repo VARCHAR,
            actor VARCHAR,
            org VARCHAR,
            created_at TIMESTAMP,
            action VARCHAR,
            ref VARCHAR,
            ref_type VARCHAR,
            inserted_at TIMESTAMP
        )
    """)

    # 3. Insert data
    conn.register("df_view", df)
    conn.execute("INSERT INTO events SELECT * FROM df_view")

    # 4. Export to S3 
    S3_BUCKET = "mistelehendershot-dp3"      
    S3_PREFIX = "duckdb_export"             # folder inside bucket

    conn.execute(f"SET s3_region='us-east-1';")
    conn.execute(f"SET s3_access_key_id='{aws_creds.aws_access_key_id}';")
    conn.execute(f"SET s3_secret_access_key='{aws_creds.aws_secret_access_key}';")

    conn.execute(f"""
        EXPORT DATABASE 's3://{S3_BUCKET}/{S3_PREFIX}'
        (FORMAT PARQUET);
    """)



    conn.close()
    print("Exported DuckDB database to S3")



# -------------------------
# Flow
# -------------------------

@flow(name="github-events-poller")
def github_events_flow():
    total_records = 0

    for repo in REPOS:
        events = fetch_repo_events(repo)
        df = flatten_events(events, repo)
        count = len(df)

        append_duckdb_s3(df)

        total_records += count
        print(f"{repo}: {count} rows inserted (total {total_records})")

        if total_records >= RECORD_LIMIT:
            print("Record limit reached — stopping.")
            break

        time.sleep(1)

    print(f"Sleeping {SLEEP_AFTER_RUN}s before exit...")
    time.sleep(SLEEP_AFTER_RUN)



if __name__ == "__main__":
    github_events_flow()
