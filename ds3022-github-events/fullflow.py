#!/usr/bin/env python3
from prefect import flow, task
import requests, os, json, time, pandas as pd, duckdb
from datetime import datetime, timezone

# ---------------------------------------
# Configuration
# ---------------------------------------
REPOS = [
    "pandas-dev/pandas",
    "tensorflow/tensorflow",
    "microsoft/vscode",
    "facebook/react",
    "torvalds/linux",
    "pytorch/pytorch",
    "apache/spark",
    "kubernetes/kubernetes",
    "numpy/numpy",
    "golang/go"
]
BASE_URL = "https://api.github.com/repos/{repo}/events"
OUTPUT_DIR = "data/raw"
DB_PATH = "data/github.duckdb"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TOKEN = os.getenv("GITHUB_TOKEN")
headers = {"Accept": "application/vnd.github+json"}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"

RECORD_LIMIT = 300        # maximum total events per run
SLEEP_AFTER_RUN = 120     # seconds to sleep before exiting


# ---------------------------------------
# Tasks
# ---------------------------------------

@task(retries=3, retry_delay_seconds=15)
def fetch_repo_events(repo_name: str):
    """Fetch recent GitHub events for one repo."""
    url = BASE_URL.format(repo=repo_name)
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Failed {repo_name}: {r.status_code}")

    data = r.json()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    repo_clean = repo_name.replace("/", "__")
    path = f"{OUTPUT_DIR}/github_{repo_clean}_{ts}.json"
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path, data


@task
def flatten_events(events, repo_name):
    """Flatten GitHub event JSON to rows."""
    rows = []
    for e in events:
        rows.append({
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
        })
    return pd.DataFrame(rows)


@task
def append_duckdb(df):
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events AS SELECT * FROM df WHERE 0=1
    """)
    conn.register("df_view", df)
    conn.execute("INSERT INTO events SELECT * FROM df_view")
    conn.close()
    print(f"[+] Appended {len(df)} rows to DuckDB")


# ---------------------------------------
# Flow
# ---------------------------------------

@flow(name="github-events-poller-capped")
def github_events_flow():
    total_records = 0
    all_dfs = []

    for repo in REPOS:
        path, events = fetch_repo_events(repo)
        df = flatten_events(events, repo)
        count = len(df)
        all_dfs.append(df)
        total_records += count
        print(f"{repo}: {count} events (running total {total_records})")

        if total_records >= RECORD_LIMIT:
            print(f"⚠️ Record limit {RECORD_LIMIT} reached. Stopping early.")
            break
        time.sleep(1)

    if all_dfs:
        merged = pd.concat(all_dfs, ignore_index=True)
        append_duckdb(merged)

    print(f"Sleeping {SLEEP_AFTER_RUN} seconds before exit...")
    time.sleep(SLEEP_AFTER_RUN)
    print("Run complete.")


if __name__ == "__main__":
    github_events_flow.serve(
        name="local-github-poller"
    )