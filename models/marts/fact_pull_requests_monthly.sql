-- Monthly PR health trend: cycle time, volume, and average size for merged PRs.
-- Grain: one row per calendar month.

with relevant_prs as (
    select *
    from {{ ref('fact_pull_requests') }}
    where is_merged
    and not is_draft
    and not is_bot
    -- exclude incomplete month
    and merged_at < timestamp_trunc(current_timestamp(), month)
  
),

monthly as (
    select
        timestamp_trunc(merged_at, month) as month,
        count(pr_number) as total_merged_prs,
        avg(cycle_time_hours) as avg_cycle_time_hours
    from relevant_prs
    group by all
)
select
    month,
    total_merged_prs,
    avg_cycle_time_hours,
    avg_cycle_time_hours
        - lag(avg_cycle_time_hours) over (order by month)
        as cycle_time_mom_change
from monthly
where month >= '{{ var("start_date") }}'