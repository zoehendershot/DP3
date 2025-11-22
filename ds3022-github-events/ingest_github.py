import os
import time
import json
import requests
from datetime import datetime, timezone

# ---------------------------------------------------
# 1. Configure your high-traffic repositories
# ---------------------------------------------------
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

# Directory to save data
OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------
# 2. Read your GitHub token (optional but HIGHLY recommended)
# ---------------------------------------------------
TOKEN = os.getenv("GITHUB_TOKEN")  # export GITHUB_TOKEN="your_PAT_here"

headers = {"Accept": "application/vnd.github+json"}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"
else:
    print("⚠️  Warning: GITHUB_TOKEN not set — lower rate limits apply.")


# ---------------------------------------------------
# 3. Function to fetch events for one repo
# ---------------------------------------------------
def fetch_repo(repo_name: str):
    url = BASE_URL.format(repo=repo_name)

    print(f"\nRequesting: {url}")

    response = requests.get(url, headers=headers)

    print(f"HTTP {response.status_code}")
    poll_interval = response.headers.get("X-Poll-Interval", "60")
    print(f"X-Poll-Interval: {poll_interval} seconds")

    if response.status_code != 200:
        print("❌ Error fetching events.")
        return

    # Save file with timestamp including proper timezone-aware UTC time
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    repo_clean = repo_name.replace("/", "__")

    out_path = f"{OUTPUT_DIR}/github_{repo_clean}_{ts}.json"

    with open(out_path, "w") as f:
        json.dump(response.json(), f, indent=2)

    print(f"Saved: {out_path}")


# ---------------------------------------------------
# 4. Main continuous loop
# ---------------------------------------------------
def main():
    print("🚀 Starting continuous GitHub event ingestion…")
    print("Polling cycle: hitting multiple repos once per minute.\n")

    while True:
        start = time.time()

        for repo in REPOS:
            fetch_repo(repo)
            time.sleep(1)  # slight delay between repos

        # Enforce GitHub's 60-second rule
        elapsed = time.time() - start
        sleep_time = max(60 - elapsed, 0)

        print(f"\n⏳ Sleeping {sleep_time:.1f} seconds until next cycle…")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
