#!/usr/bin/env python3
from prefect import flow, task
import os, json, pandas as pd, duckdb
from datetime import datetime

RAW_DIR = "data/raw"
DB_PATH = "data/github.duckdb"

@task
def list_new_files():
    return [os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".json")]

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
def overwrite_duckdb(df):
    """Drop and recreate the events table each run to avoid duplicates."""
    conn = duckdb.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS events")
    conn.register("df_view", df)
    conn.execute("CREATE TABLE events AS SELECT * FROM df_view")
    conn.close()
    print(f"[+] Overwrote DuckDB database with {len(df)} rows")

@flow(name="GitHub Events Auto-Loader")
def github_flow():
    all_data = []
    for path in list_new_files():
        df = load_and_flatten(path)
        if not df.empty:
            all_data.append(df)

    if all_data:
        merged = pd.concat(all_data, ignore_index=True)
        overwrite_duckdb(merged)
    else:
        print("⚠️  No JSON files found.")

if __name__ == "__main__":
    github_flow()
