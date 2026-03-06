{{ config(materialized='ephemeral') }}

-- This model returns one row per contributor to the repository.

with commits as (
    select
        author_login,
        author_id,
        is_bot,
        authored_at as contribution_date
    from {{ref('stg_github_commits')}}
),

pull_requests as (
    select
        author_login,
        author_id,
        is_bot,
        created_at as contribution_date
    from {{ref('stg_github_pull_requests')}}
),

unioned as (
    select author_login, author_id, is_bot, contribution_date from commits
    union all
    select author_login, author_id, is_bot, contribution_date from pull_requests
)

select
    author_id,
    -- keeps login name from most recent contribution:
    array_agg(author_login order by contribution_date desc)[offset(0)] as author_login,
    max(is_bot) as is_bot,
    min(contribution_date) as first_contribution_date
from unioned
where author_id is not null
group by author_id
