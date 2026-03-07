select
    author_id,
    author_login,
    is_bot,
    first_contribution_at,
    last_contribution_at,
    is_active
from {{ref('int_contributors')}}