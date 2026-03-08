{{ config(materialized='ephemeral') }}

-- Monthly count of issues opened and closed, and their ratio.
-- Grain: one row per calendar month.

-- Note: months with zero activity in both dimensions will not appear in results.
-- Todo: use a calendar table to ensure all months appear.

with opened as (
    select
        timestamp_trunc(created_at, month) as month,
        count(*) as opened_count
    from {{ref('int_issues')}}
    where not is_bot
    group by all
),

closed as (
    select
        timestamp_trunc(closed_at, month) as month,
        count(*) as closed_count
    from {{ref('int_issues')}}
    where is_closed
    and not is_bot
    group by all
),

monthly as (
    select
        coalesce(o.month, c.month) as month,
        coalesce(o.opened_count, 0) as opened_count,
        coalesce(c.closed_count, 0) as closed_count,
        safe_divide(coalesce(c.closed_count, 0), o.opened_count) as closed_to_opened_ratio
    from opened o
    full outer join closed c on o.month = c.month
)

select
    month,
    opened_count,
    closed_count,
    closed_to_opened_ratio,
    closed_to_opened_ratio
        - lag(closed_to_opened_ratio) over (order by month)
        as ratio_mom_change
from monthly
where month >= '{{ var("start_date") }}'
and month < timestamp_trunc(current_timestamp(), month)
