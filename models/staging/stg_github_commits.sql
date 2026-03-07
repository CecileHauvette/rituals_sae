{{ config(materialized='table') }}

-- Materialized as table (not view) to avoid re-parsing JSON on every downstream query.

with source as (select * from {{source('github_raw', 'raw_commits')}}
    qualify row_number() over (partition by record_id order by loaded_at desc) = 1
)

select
timestamp(json_value(data, '$.commit.author.date')) as authored_at,
json_value(data, '$.author.login') as author_login,
json_value(data, '$.author.id') as author_id,
json_value(data, '$.sha') as commit_sha,
array_length(json_query_array(data, '$.parents')) > 1 as is_merge_commit,
json_value(data, '$.author.type') = 'Bot' or lower(json_value(data, '$.author.login')) like '%bot%' as is_bot
from source 