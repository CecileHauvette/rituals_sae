{{ config(materialized='ephemeral') }}

-- This model returns one row per contributor to the repository.

with commits as (
    select
        author_login,
        author_id,
        is_bot,
        authored_at as contribution_at
    from {{ref('stg_github_commits')}}
),

pull_requests as (
    select
        author_login,
        author_id,
        is_bot,
        created_at as contribution_at
    from {{ref('stg_github_pull_requests')}}
),

unioned as (
    select author_login, author_id, is_bot, contribution_at from commits
    union all
    select author_login, author_id, is_bot, contribution_at from pull_requests
)

select
    author_id,
    -- keeps login name from most recent contribution:
    array_agg(author_login order by contribution_at desc)[offset(0)] as author_login,
    max(is_bot) as is_bot,
    min(contribution_at) as first_contribution_at,
    max(contribution_at) as last_contribution_at,
    --TODO: improve logic and/or make the number of months a variable
    date(max(contribution_at)) >= date_sub(current_date(), interval 3 month) as is_active
from unioned
where author_id is not null
group by all
