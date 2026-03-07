{{ config(materialized='table') }}

-- Materialized as table (not view) to avoid re-parsing JSON on every downstream query.

with source as (
    select * from {{source('github_raw', 'raw_pull_requests')}}
    qualify row_number() over (partition by record_id order by loaded_at desc) = 1
)

select
    timestamp(json_value(data, '$.created_at')) as created_at,
    timestamp(json_value(data, '$.updated_at')) as updated_at,
    timestamp(json_value(data, '$.closed_at')) as closed_at,
    timestamp(json_value(data, '$.merged_at')) as merged_at,
    json_value(data, '$.user.login') as author_login,
    json_value(data, '$.user.id') as author_id,
    json_value(data, '$.user.type') = 'Bot'
        or lower(json_value(data, '$.user.login')) like '%bot%' as is_bot,
    json_value(data, '$.state') as state,
    json_value(data, '$.draft') = 'true' as is_draft,
    json_value(data, '$.number') as pr_number,
    json_value(data, '$.merge_commit_sha') as merge_commit_sha,
    cast(json_value(data, '$.commits') as int64) as commit_count,
    cast(json_value(data, '$.additions') as int64) as lines_added,
    cast(json_value(data, '$.deletions') as int64) as lines_deleted,
    cast(json_value(data, '$.changed_files') as int64) as files_changed
from source
