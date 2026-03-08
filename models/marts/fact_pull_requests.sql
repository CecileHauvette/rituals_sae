select
    pr_number,
    author_id,
    is_bot,
    is_draft,
    state,
    created_at,
    merged_at,
    is_merged,
    cycle_time_hours,
    commit_count
from {{ref('int_pull_requests')}}
