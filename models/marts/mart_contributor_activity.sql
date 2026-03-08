-- One row per human contributor, aggregating their PR and issue activity.
-- Grain: author_id (unique).
-- Joins dim_contributors to fact_pull_requests and fact_issues.
with author_prs as (
    select
        author_id,
        count(pr_number) as total_prs,
        countif(is_merged) as merged_prs,
        avg(cycle_time_hours) as avg_cycle_time_hours,
        safe_divide(countif(is_merged), count(pr_number)) as merge_rate
    from {{ ref('fact_pull_requests') }}
    where not is_draft
    and not is_bot
    group by all
),
author_issues as (
    select
        author_id,
        count(issue_number) as total_issues_opened
    from {{ ref('fact_issues') }}
    where not is_bot
    group by all
)

select
    c.author_id,
    c.author_login,
    c.first_contribution_at,
    pr.avg_cycle_time_hours,
    coalesce(pr.total_prs, 0) as total_prs,
    coalesce(pr.merged_prs, 0) as merged_prs,
    pr.merge_rate,
    coalesce(i.total_issues_opened, 0) as total_issues_opened
from {{ ref('dim_contributors') }} c
left join author_prs pr on pr.author_id = c.author_id
left join author_issues i on i.author_id = c.author_id
where not c.is_bot
and c.is_active
order by total_prs desc
