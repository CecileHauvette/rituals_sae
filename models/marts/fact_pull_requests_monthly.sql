-- One row per calendar month with PR volume and average cycle time.

select
    month,
    total_merged_prs,
    avg_cycle_time_hours,
    cycle_time_mom_change
from {{ ref('int_pull_requests_monthly') }}
