{{ config(materialized='ephemeral') }}

-- One row per pull request, enriched with cycle time.
-- Grain: pr_number

with source as (
    select * from {{ref('stg_github_pull_requests')}}
)

select
    pr_number,
    author_id,
    is_bot,
    is_draft,
    state,
    created_at,
    merged_at,
    merged_at is not null as is_merged,
    timestamp_diff(merged_at, created_at, hour) as cycle_time_hours,
    commit_count
from source
