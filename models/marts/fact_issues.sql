select
    issue_number,
    author_id,
    is_bot,
    state,
    created_at,
    closed_at,
    is_closed,
    resolution_time_hours,
    comment_count,
    labels
from {{ref('int_issues')}}
