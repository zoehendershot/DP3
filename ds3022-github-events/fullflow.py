#!/usr/bin/env python3
from prefect import flow, task
import requests, os, time, pandas as pd, duckdb
from datetime import datetime, timezone

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
DB_PATH = "data/github.duckdb"

TOKEN = os.getenv("GITHUB_TOKEN")
headers = {"Accept": "application/vnd.github+json"}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"

RECORD_LIMIT = 300
SLEEP_AFTER_RUN = 120


# Tasks ------------------

@task(retries=3, retry_delay_seconds=15)
def fetch_repo_events(repo_name: str, max_pages: int = 5):
    """Fetch GitHub events for a repo with pagination and rate-limit handling."""
    all_events = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL.format(repo=repo_name)}?per_page=100&page={page}"
        r = requests.get(url, headers=headers)

        # Handle rate limits
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
                print(f"⚠️ 403 for {repo_name}, sleeping 5 minutes.")
                time.sleep(300)
                return fetch_repo_events.fn(repo_name, max_pages=max_pages)

        elif r.status_code != 200:
            print(f"Failed {repo_name} page {page}: {r.status_code}")
            break

        events = r.json()
        if not events:
            break

        all_events.extend(events)

        poll_interval = int(r.headers.get("X-Poll-Interval", "60"))
        time.sleep(poll_interval)

    print(f"{repo_name}: fetched {len(all_events)} total events.")
    return all_events


@task
def flatten_events(events, repo_name):
    """Flatten GitHub event JSON to rows."""
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


@task
def append_duckdb(df):
    conn = duckdb.connect(DB_PATH)

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

    conn.register("df_view", df)
    conn.execute("INSERT INTO events SELECT * FROM df_view")
    conn.close()

    print(f"[+] Inserted {len(df)} rows into DuckDB.")



# Flow -------------------------------------


@flow(name="github-events-poller")
def github_events_flow():
    total_records = 0

    for repo in REPOS:
        events = fetch_repo_events(repo)
        df = flatten_events(events, repo)
        count = len(df)

        append_duckdb(df)

        total_records += count
        print(f"{repo}: {count} events inserted (running total {total_records})")

        if total_records >= RECORD_LIMIT:
            print(f"Record limit {RECORD_LIMIT} reached. Stopping early.")
            break

        time.sleep(1)

    print(f"Sleeping {SLEEP_AFTER_RUN}s before exit...")
    time.sleep(SLEEP_AFTER_RUN)


if __name__ == "__main__":
    github_events_flow()