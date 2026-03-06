select
    author_id,
    author_login,
    is_bot,
    first_contribution_date
from {{ref('int_contributors')}}