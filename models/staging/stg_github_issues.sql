{{ config(materialized='table') }}

-- One row per issue in the dbt-core repository.
-- Materialized as table (not view) to avoid re-parsing JSON on every downstream query.
-- TODO: make incremental by issue_number.

with source as (
    select * from {{source('github_raw', 'raw_issues')}}
    qualify row_number() over (partition by record_id order by loaded_at desc) = 1
)

select
    json_value(data, '$.number') as issue_number,
    json_value(data, '$.title') as title,
    json_value(data, '$.state') as state,
    timestamp(json_value(data, '$.created_at')) as created_at,
    timestamp(json_value(data, '$.closed_at')) as closed_at,
    timestamp(json_value(data, '$.updated_at')) as updated_at,
    json_value(data, '$.user.login') as author_login,
    json_value(data, '$.user.id') as author_id,
    json_value(data, '$.user.type') = 'Bot'
        or lower(json_value(data, '$.user.login')) like '%bot%' as is_bot,
    cast(json_value(data, '$.comments') as int64) as comment_count,
    json_query(data, '$.labels') as labels
from source
