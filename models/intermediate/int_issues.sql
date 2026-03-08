-- One row per issue, enriched with resolution status and time.
-- Grain: issue_number.

with source as (
    select * from {{ref('stg_github_issues')}}
)

select
    issue_number,
    author_id,
    is_bot,
    state,
    created_at,
    closed_at,
    closed_at is not null as is_closed,
    timestamp_diff(closed_at, created_at, hour) as resolution_time_hours,
    comment_count,
    labels
from source
