select
    month,
    opened_count,
    closed_count,
    closed_to_opened_ratio,
    ratio_mom_change
from {{ref('int_issues_monthly')}}
