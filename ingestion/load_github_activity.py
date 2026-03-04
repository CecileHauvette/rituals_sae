

import logging
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# Load variables from .env file 
load_dotenv()

# ── 1. Configuration ──────────────────────────────────────────────────────────

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GCP_PROJECT  = os.environ["GCP_PROJECT"]           
BQ_DATASET   = os.environ["BQ_DATASET"]
REPO         = "dbt-labs/dbt-core"
SINCE        = os.environ["SINCE"]

PER_PAGE     = 100   # GitHub max is 100 items per page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── 2. GitHub API helpers ─────────────────────────────────────────────────────

GITHUB_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
GITHUB_BASE = "https://api.github.com"


def gh_paginate(path, params=None, stop_before=None):
    """
    Fetch every page from a GitHub API endpoint and return all items as a list.

    path        – API path, e.g. "repos/dbt-labs/dbt-core/commits"
    params      – extra query parameters (dict)
    stop_before – ISO-8601 string; stop pagination when an item's updated_at
                  (or committed_date) is older than this value. 
    """
    params = {**(params or {}), "per_page": PER_PAGE}
    url    = f"{GITHUB_BASE}/{path}"
    items  = []

    while url:
        response = requests.get(url, headers=GITHUB_HEADERS, params=params)

        # GitHub rate-limit: wait until the window resets
        if response.status_code == 403 and "rate limit" in response.text.lower():
            reset_ts  = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait_secs = max(reset_ts - time.time(), 1)
            log.warning(f"Rate limited — waiting {wait_secs:.0f} s …")
            time.sleep(wait_secs)
            continue  # retry the same URL

        response.raise_for_status()
        page = response.json()

        if not page:
            break

        # Optional early-stop: check the last item's date
        if stop_before:
            last_item = page[-1]
            item_date = (
                last_item.get("updated_at")
                or (last_item.get("commit") or {}).get("committer", {}).get("date")
            )
            if item_date and item_date <= stop_before:
                # Keep only items strictly newer than the watermark
                page = [
                    i for i in page
                    if (
                        i.get("updated_at", "") > stop_before
                        or (i.get("commit") or {})
                        .get("committer", {})
                        .get("date", "") > stop_before
                    )
                ]
                items.extend(page)
                log.info(f"  Early stop — {len(items)} items total (reached {stop_before})")
                break

        items.extend(page)
        log.info(f"  {len(items)} items fetched from {path} …")

        # Follow GitHub's Link: <url>; rel="next" header for the next page
        url    = response.links.get("next", {}).get("url")
        params = {}  # params are already encoded in the next URL

    return items


# ── 3. BigQuery helpers ───────────────────────────────────────────────────────

# Schema shared by all raw tables:
#   record_id  – GitHub's unique identifier → used by dbt to deduplicate
#   data       – full JSON payload as a string → dbt parses with JSON functions
#   loaded_at  – when the row was written (for auditing / incremental logic)
#   batch_id   – UUID of this script run (helps trace which rows came together)
RAW_SCHEMA = [
    bigquery.SchemaField(
        "record_id", "STRING",
        description="GitHub unique ID (SHA, numeric id …) — deduplicate in dbt"),
    bigquery.SchemaField(
        "data", "JSON",
        description="Full JSON payload from the GitHub API"),
    bigquery.SchemaField(
        "loaded_at", "TIMESTAMP",
        description="UTC timestamp when this row was loaded"),
    bigquery.SchemaField(
        "batch_id", "STRING",
        description="UUID identifying the script run that produced this row"),
]


def ensure_dataset(bq):
    """Create the BigQuery dataset if it doesn't already exist."""
    ref = bigquery.DatasetReference(GCP_PROJECT, BQ_DATASET)
    try:
        bq.get_dataset(ref)
        log.info(f"Dataset {GCP_PROJECT}.{BQ_DATASET} already exists — ok.")
    except Exception:
        dataset = bigquery.Dataset(ref)
        dataset.location = "EU"
        bq.create_dataset(dataset)
        log.info(f"Created dataset {GCP_PROJECT}.{BQ_DATASET} in EU.")


def append_to_bq(bq, rows, table_name):
    """
    Append a list of row-dicts to a BigQuery table.
    The table is created automatically if it doesn't exist yet.
    WRITE_APPEND means existing data is never touched.
    """
    if not rows:
        log.info(f"  No rows to write for {table_name} — skipping.")
        return

    table_ref  = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema             = RAW_SCHEMA,
        write_disposition  = bigquery.WriteDisposition.WRITE_APPEND,
        # CREATE_IF_NEEDED: first run creates the table, subsequent runs append
        create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED,
        # Partition by load date so incremental dbt models scan only recent partitions
        time_partitioning  = bigquery.TimePartitioning(
            type_  = bigquery.TimePartitioningType.DAY,
            field  = "loaded_at",
        ),
        # Cluster by record_id to speed up deduplication queries in dbt
        clustering_fields  = ["record_id"],
    )

    job = bq.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()   # blocks until the load job finishes
    log.info(f"  ✓ {len(rows):,} rows appended to {table_ref}")


def make_row(github_id, payload, batch_id, loaded_at):
    """Package one GitHub API object into the standard row format."""
    return {
        "record_id": str(github_id),
        "data":      payload,
        "loaded_at": loaded_at,
        "batch_id":  batch_id,
    }


# ── 4. Per-entity fetch functions ─────────────────────────────────────────────

def get_watermark(bq, table_name, json_path):
    """
    Return the latest GitHub timestamp already stored in the table,
    or fall back to SINCE if the table is empty / doesn't exist yet.
    json_path is dot-notation into the 'data' JSON column, e.g. 'updated_at'.
    """
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
    try:
        row = next(iter(bq.query(
            f"SELECT MAX(JSON_VALUE(data, '$.{json_path}')) AS max_ts FROM `{table_ref}`",
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
        ).result()))
        if row["max_ts"]:
            log.info(f"  Watermark for {table_name}: {row['max_ts']}")
            return row["max_ts"]
    except Exception as e:
        log.warning(f"  Watermark query failed for {table_name}: {e}")
    log.info(f"  No watermark for {table_name} — using SINCE={SINCE}")
    return SINCE


def load_commits(bq, batch_id, loaded_at):
    """
    Commits: who pushed what and when.
    SINCE filters server-side — only commits after that date are returned.
    record_id = commit SHA (globally unique).
    """
    log.info("── Commits ──────────────────────────────────────────────────")
    since = get_watermark(bq, "raw_commits", "commit.committer.date")
    items = gh_paginate(
        f"repos/{REPO}/commits",
        params={"since": since},
    )
    # GitHub's since is inclusive — drop items at exactly the watermark (already loaded)
    items = [i for i in items if (i.get("commit") or {}).get("committer", {}).get("date", "9999") > since]
    rows = [make_row(item["sha"], item, batch_id, loaded_at) for item in items]
    append_to_bq(bq, rows, "raw_commits")


def load_pull_requests(bq, batch_id, loaded_at):
    """
    Pull requests: lifecycle, labels, merge time …
    state=all fetches open + closed + merged.
    We paginate newest-first and stop when we reach the watermark.
    record_id = PR id (numeric, unique within GitHub).
    """
    log.info("── Pull Requests ────────────────────────────────────────────")
    since = get_watermark(bq, "raw_pull_requests", "updated_at")
    items = gh_paginate(
        f"repos/{REPO}/pulls",
        params={"state": "all", "sort": "updated", "direction": "desc"},
        stop_before=since,
    )
    rows = [make_row(item["id"], item, batch_id, loaded_at) for item in items]
    append_to_bq(bq, rows, "raw_pull_requests")


def load_issues(bq, batch_id, loaded_at):
    """
    Issues: bug reports, feature requests, discussions.
    The GitHub API returns PRs mixed with issues — we filter them out
    by checking for the presence of the 'pull_request' key.
    record_id = issue id.
    """
    log.info("── Issues ───────────────────────────────────────────────────")
    since = get_watermark(bq, "raw_issues", "updated_at")
    items = gh_paginate(
        f"repos/{REPO}/issues",
        params={"state": "all", "since": since},
    )
    # GitHub's since is inclusive — drop items at exactly the watermark (already loaded)
    items = [i for i in items if i.get("updated_at", "9999") > since]
    # Drop items that are actually pull requests (GitHub returns them together)
    issues_only = [i for i in items if "pull_request" not in i]
    log.info(f"  {len(issues_only)} true issues (filtered out {len(items) - len(issues_only)} PRs)")

    rows = [make_row(item["id"], item, batch_id, loaded_at) for item in issues_only]
    append_to_bq(bq, rows, "raw_issues")


# ── 5. Main ───────────────────────────────────────────────────────────────────

def main():
    bq        = bigquery.Client(project=GCP_PROJECT)
    batch_id  = str(uuid.uuid4())
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info("════════════════════════════════════════════════════════════")
    log.info(f"  Repo      : {REPO}")
    log.info(f"  Project   : {GCP_PROJECT}")
    log.info(f"  Dataset   : {BQ_DATASET}")
    log.info(f"  Since     : {SINCE}")
    log.info(f"  Batch ID  : {batch_id}")
    log.info("════════════════════════════════════════════════════════════")

    ensure_dataset(bq)

    load_commits(bq, batch_id, loaded_at)
    load_pull_requests(bq, batch_id, loaded_at)
    load_issues(bq, batch_id, loaded_at)

    log.info("════════════════════════════════════════════════════════════")
    log.info("  All done! ✓")
    log.info("════════════════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
