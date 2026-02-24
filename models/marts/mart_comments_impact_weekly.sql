with base as (
  select
    week_start,
    category,
    listens,
    follows,
    comments
  from {{ ref('mart_show_weekly_feature_usage') }}
),
labeled as (
  select
    week_start,
    category,
    case when comments > 0 then 1 else 0 end as has_comments,
    listens,
    follows
  from base
),
agg as (
  select
    week_start,
    category,
    has_comments,
    sum(listens) as listens,
    sum(follows) as follows
  from labeled
  group by 1,2,3
)
select
  week_start,
  category,
  has_comments,
  listens,
  follows,
  case when listens = 0 then null else 1.0 * follows / listens end as follow_rate
from agg