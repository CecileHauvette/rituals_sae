-- Contributor concentration per quarter: how many contributors account for 80% of merged PRs?
-- Grain: one row per quarter.

with contributor_prs as (
    select
        author_id,
        timestamp_trunc(merged_at, quarter) as quarter,
        count(pr_number) as merged_prs
    from {{ ref('fact_pull_requests') }}
    where is_merged
    and not is_draft
    and not is_bot
    and merged_at >= '{{ var("start_date") }}'
    and merged_at < timestamp_trunc(current_timestamp(), quarter)
    group by all
),
ranked as (
    select
        quarter,
        author_id,
        merged_prs,
        sum(merged_prs) over (partition by quarter) as total_quarter_prs,
        count(*) over (partition by quarter) as total_contributors,
        sum(merged_prs) over (partition by quarter order by merged_prs desc rows between unbounded preceding and current row) as running_prs,
        row_number() over (partition by quarter order by merged_prs desc) as contributor_rank
    from contributor_prs
)
select
    quarter,
    total_quarter_prs,
    max(total_contributors) as total_contributors,
    min(contributor_rank) as contributors_for_80pct
from ranked
where safe_divide(running_prs, total_quarter_prs) >= 0.8
group by all
