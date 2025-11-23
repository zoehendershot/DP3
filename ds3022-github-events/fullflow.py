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
]

RAW_DIR = "data/raw"
DB_PATH = "data/github.duckdb"
SLEEP_AFTER_RUN = 10  # seconds

# ---------------------------------------
# Tasks
# ---------------------------------------
@task
def fetch_github_events(repo):
    url = f"https://api.github.com/repos/{repo}/events"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@task
def save_raw_events(repo, events):
    os.makedirs(RAW_DIR, exist_ok=True)
    path = os.path.join(RAW_DIR, f"{repo.replace('/', '_')}_{datetime.now().isoformat()}.json")
    with open(path, "w") as f:
        json.dump(events, f)
    return path

@task
def load_and_flatten(path):
    with open(path, "r") as f:
        data = json.load(f)
    rows = []
    for e in data:
        rows.append({
            "id": e.get("id"),
            "type": e.get("type"),
            "repo": e.get("repo", {}).get("name"),
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

    # Create the table with a primary key on 'id' if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id VARCHAR PRIMARY KEY,
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

    # Register the new dataframe
    conn.register("df_view", df)

    # Insert only new rows (ignore duplicates)
    conn.execute("""
        INSERT OR REPLACE INTO events
        SELECT * FROM df_view
    """)

    conn.close()
    print(f"[+] Upserted {len(df)} rows into DuckDB (no duplicates).")


# ---------------------------------------
# Flow Definition
# ---------------------------------------
@flow
def github_events_flow():
    all_dfs = []
    for repo in REPOS:
        events = fetch_github_events(repo)
        path = save_raw_events(repo, events)
        df = load_and_flatten(path)
        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        merged = pd.concat(all_dfs, ignore_index=True)
        append_duckdb(merged)

    print(f"Sleeping {SLEEP_AFTER_RUN} seconds before exit...")
    time.sleep(SLEEP_AFTER_RUN)
    print("Run complete.")

# ---------------------------------------
# Run the Flow
# ---------------------------------------
if __name__ == "__main__":
    github_events_flow()