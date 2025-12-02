#!/usr/bin/env python3
"""
Prefect flow that fetches public GitHub repository events for a
set of repositories, flattens them into a tabular structure, stores
them in a local DuckDB database, and exports the database to S3 as
Parquet files.

"""
from prefect import flow, task
import requests, time, pandas as pd, duckdb
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

import os
import logging
from logging import StreamHandler, FileHandler

# Configure logging: console + file with timestamps and exception tracebacks
LOG_FILE = os.path.join(os.path.dirname(__file__), "fullflow.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[StreamHandler(), FileHandler(LOG_FILE, encoding="utf-8")]
)
logger = logging.getLogger("fullflow")

# -------------------------
# Pipeline Configuration
# -------------------------

#list of popular repositories to fetch events from
#chosen to ensure active and diverse event streams
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

# Local + S3 paths
LOCAL_DB = "local.duckdb"
S3_BUCKET = "mistelehendershot-dp3"
S3_PREFIX = "duckdb_export"
AWS_REGION = "us-east-1"

# Load GitHub PAT from environment variable
GITHUB_PAT = os.getenv("GITHUB_PAT")
if not GITHUB_PAT:
    logger.error("Missing GITHUB_PAT environment variable!")
    raise ValueError("Missing GITHUB_PAT environment variable!")

# Load AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

if not AWS_ACCESS_KEY or not AWS_SECRET:
    logger.error("Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in environment!")
    raise ValueError("Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in environment!")

# standard headers for GitHub API requests
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_PAT}"
}

# Maximum number of records to ingest at once to avoid excessive data loads in one run
RECORD_LIMIT = 300

# GITHUB_PAT must be set in the environment for authenticated API
# AWS credentials are read from environment variables and used to configure DuckDB's S3 export settings.

# -------------------------
# Prefect Tasks
# -------------------------
 
@task(retries=3, retry_delay_seconds=15)
def fetch_repo_events(repo_name: str, max_pages: int = 5):
# Github has strict rate limits, waiting until reset time (or 5 minutes) ensures we don't violate usage policy or crash the flow. 
    """Fetch GitHub events with basic rate-limit handling.

    Args:
        repo_name: repository full name (owner/repo)
        max_pages: maximum number of event pages to request (per_page=100)

    Returns:
        A list of event JSON objects (may be empty if no events at that time).

    Behavior:
        - Tries to recover from network errors on its own
        - If a 403 rate-limit is encountered, waits until reset when
          the header is provided, otherwise sleeps for 5m.
    """
    all_events = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL.format(repo=repo_name)}?per_page=100&page={page}"
        # Attempt API request with timeout; log and stop if network issues
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException:
            logger.exception("Network error fetching %s page %d", repo_name, page)
            break

        try:
            # Handle API rate limiting (HTTP 403). GitHub returns a X-RateLimit-Reset epoch timestamp when the limit resets.
            if r.status_code == 403:
                reset_time = r.headers.get("X-RateLimit-Reset")
                if reset_time:
                    # Calculate wait time until reset, with a minimum of 60s
                    wait_sec = max(
                        (datetime.fromtimestamp(int(reset_time), tz=timezone.utc) -
                         datetime.now(timezone.utc)).total_seconds(), 60
                    )
                    logger.warning("Rate limit hit for %s. Sleeping %ds.", repo_name, int(wait_sec))
                    time.sleep(wait_sec)
                    return fetch_repo_events.fn(repo_name, max_pages=max_pages)
                else:
                    # safe default wait if no reset header
                    logger.warning("403 no reset header — sleeping 5m for %s.", repo_name)
                    time.sleep(300)
                    return fetch_repo_events.fn(repo_name, max_pages=max_pages)

            # abort page fetching for this repository on non-200 responses
            elif r.status_code != 200:
                logger.error("Failed %s page %d: %s", repo_name, page, r.status_code)
                break

            # Decode JSON payload. We expect a list of events.
            events = r.json()
            if not events:
                break

            all_events.extend(events)

            # `X-Poll-Interval` header to avoid excessive requests
            poll_interval = int(r.headers.get("X-Poll-Interval", "60"))
            logger.debug("Sleeping poll interval %d seconds for %s", poll_interval, repo_name)
            time.sleep(poll_interval)
        except Exception:
            # Log and stop on unexpected errors processing the response
            logger.exception("Unexpected error processing response for %s page %d", repo_name, page)
            break

    logger.info("%s: %d events.", repo_name, len(all_events))
    return all_events


@task
def flatten_events(events, repo_name):
    """Normalize GitHub event JSON into a flat pandas DataFrame.

    The function picks a set of useful fields and
    attaches an `inserted_at` timestamp. If the incoming events list
    contains unexpected structures, the function logs the exception
    and re-raises so the flow/task system can surface the failure.
    """
    rows = []
    try:
        for e in events:
            # Defensive lookups with defaults to avoid KeyError on partially-shaped event objects.
            repo_full = e.get("repo", {}).get("name", repo_name)
            parsed_org = repo_full.split("/")[0] if "/" in repo_full else None

            rows.append({
                "id": e.get("id"),
                "type": e.get("type"),
                "repo": repo_full,
                "actor": e.get("actor", {}).get("login"),
                "org": parsed_org,
                "created_at": e.get("created_at"),
                "action": e.get("payload", {}).get("action"),
                "ref": e.get("payload", {}).get("ref"),
                "ref_type": e.get("payload", {}).get("ref_type"),
                "inserted_at": datetime.utcnow(),
            })

        return pd.DataFrame(rows)
    except Exception:
        logger.exception("Failed to flatten events for %s", repo_name)
        raise



@task
def append_and_export(df):
    """Append events to a local DuckDB `events` table and export DB to S3 as Parquet.

    Steps:
      1. Ensure the `events` table exists in `LOCAL_DB`.
      2. Insert rows from the provided DataFrame.
      3. Configure DuckDB's S3 credentials and 
      4. export the full database to the configured S3 bucket/path as Parquet files.

    Errors are logged and re-raised so framework can mark the task as failed and optionally retry.
    """
    conn = None
    try:
        # Local DB
        conn = duckdb.connect(LOCAL_DB)

        # Ensure table exists and makes one if not
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

        # register dataframe and insert
        conn.register("df_view", df)
        conn.execute("INSERT INTO events SELECT * FROM df_view")

        # Configure S3
        conn.execute(f"SET s3_region='{AWS_REGION}';")
        conn.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
        conn.execute(f"SET s3_secret_access_key='{AWS_SECRET}';")

        # Export the entire database to S3.
        s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}"
        logger.info("Exporting DuckDB to %s ...", s3_path)

        conn.execute(f"""
            EXPORT DATABASE '{s3_path}'
            (FORMAT PARQUET);
        """)

        logger.info("Exported to S3.")
    except Exception:
        logger.exception("Failed to append/export dataframe to DuckDB/S3")
        raise
    finally:
        # Clean up connection
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Failed to close DuckDB connection cleanly")


# -------------------------
# Flow
# -------------------------

@flow(name="github-events-local-s3")
def github_events_flow():
    """
    Main flow orchestrating:
      1. Iterating over repositories
      2. Fetching events
      3. Flattening events
      4. Appending to DuckDB and exporting to S3
    Stops early if RECORD_LIMIT is reached to prevent excessive ingestion.
    """
    total_records = 0

    for repo in REPOS:
        events = fetch_repo_events(repo)
        df = flatten_events(events, repo)
        count = len(df)

        append_and_export(df)

        total_records += count
        logger.info("%s: %d rows added (total %d)", repo, count, total_records)

        # Stop early if record limit reached
        if total_records >= RECORD_LIMIT:
            logger.info("Record limit reached (%d). Stopping early.", RECORD_LIMIT)
            break

        # Small pause between repositories to avoid sporadic API traffic.
        time.sleep(1)

    logger.info("Flow finished.")


# -------------------------
# Serve locally
# implemented scheduling of production runs on prefect UI for easy monitoring
# -------------------------
if __name__ == "__main__":
    github_events_flow.serve(
        name="local-github-s3-service",
        schedule=None
    )
