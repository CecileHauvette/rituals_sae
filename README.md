# dbt-core GitHub Activity Analytics

## I. Overview

An end-to-end analytics pipeline tracking contribution activity on the [dbt-core](https://github.com/dbt-labs/dbt-core) open-source repository.

**Stack**
- **Ingestion**: Python script pulling from the GitHub REST API → BigQuery raw tables
- **Transformation**: dbt (Fusion or Core)
- **Destination**: BigQuery marts ready for analysis

**What it tracks**
- Commit, pull request, and issue activity since 2023
- Contributor profiles (human vs bot, active vs inactive)
- Monthly issue resolution health (open/close ratio, MoM trend)
- Per-contributor PR throughput, merge rate, and cycle time

---

## II. Setup

### Prerequisites

- **Python 3.9+**
- **Google Cloud project** with BigQuery enabled
- **Google Application Default Credentials (ADC)** configured — see the [ADC setup guide](https://cloud.google.com/docs/authentication/provide-credentials-adc)
- **dbt** — either [dbt Fusion](https://docs.getdbt.com/docs/fusion) or [dbt Core](https://docs.getdbt.com/docs/core/installation-overview) with the `dbt-bigquery` adapter

### Steps

**1. Configure environment variables**

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

```
GCP_PROJECT=your-gcp-project-id
BQ_DATASET=your-bigquery-dataset
GITHUB_TOKEN=ghp_your_token_here
SINCE=2023-01-01          # load data from this date onward
```

`SINCE` is used as the initial watermark. On subsequent runs, the script queries BigQuery for the latest GitHub timestamp already stored (e.g. `updated_at` for issues and PRs, `commit.committer.date` for commits) and uses that as the lower bound.

**2. Install Python dependencies**

```bash
pip install -r requirements.txt
```

**3. Load raw data from the GitHub API**

```bash
python scripts/load_github_activity.py
```

This writes three raw tables to BigQuery: `raw_commits`, `raw_pull_requests`, `raw_issues`. The script is incremental — re-running it only fetches new activity.

**4. Run the dbt project**

```bash
dbt build
```

---

## III. Data model overview
