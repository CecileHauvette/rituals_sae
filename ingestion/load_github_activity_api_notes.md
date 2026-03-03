# Notes on load_github_activity_api.py

## What the script does
Fetches GitHub activity (commits, pull requests, issues) for `dbt-labs/dbt-core` from the GitHub API and loads it into BigQuery raw tables.

## How to run
Create a `.env` file in the same directory:
```
GITHUB_TOKEN=your_github_token
GCP_PROJECT=your_gcp_project_id
BQ_DATASET=github_raw
SINCE=2023-01-01T00:00:00Z
```
Then:
```bash
pip install requests python-dotenv google-cloud-bigquery
gcloud auth application-default login
python rituals_sae/ingestion/load_github_activity_api.py
```

## Key design decisions

### Watermark-based incremental loading (added)
Originally the script always fetched from the static `SINCE` env var, causing full re-fetches on every run and duplicate rows in BigQuery.

`get_watermark()` was added to query BigQuery for the latest GitHub timestamp already stored, falling back to `SINCE` on first run. Each entity uses the right GitHub-side field:
- Commits → `commit.committer.date`
- PRs → `updated_at`
- Issues → `updated_at`

**Important:** the watermark uses GitHub timestamps, not `loaded_at`. Using `loaded_at` would miss updates to existing records (e.g. a PR opened in 2023 but closed recently).

### Why `stop_before` is not redundant with the watermark
- Commits & issues: GitHub API supports a native `since` param → server-side filtering, no need for `stop_before`
- Pull requests: GitHub PR API has **no `since` param** → the script must paginate and stop manually when items are older than the watermark. `stop_before` does this client-side early exit.

### Raw zone + dbt deduplication
Tables use `WRITE_APPEND`. Each row has a `record_id` (GitHub's unique ID) that dbt uses to deduplicate downstream. With the watermark in place, duplicates should be minimal (only within the same second as the watermark).

### BigQuery schema
All three raw tables share the same schema:
- `record_id` — GitHub unique ID (SHA for commits, numeric id for PRs/issues)
- `data` — full JSON payload
- `loaded_at` — ingestion timestamp
- `batch_id` — UUID per script run (for tracing)

## Python patterns explained
- `os.environ["KEY"]` → required env var, crashes with KeyError if missing (intentional for required vars)
- `os.environ.get("KEY", "default")` → optional env var with fallback
- `{**(params or {}), "per_page": 100}` → merge dicts without mutating the original
- `logging.basicConfig(...)` + `log = logging.getLogger(__name__)` → standard Python logging setup
