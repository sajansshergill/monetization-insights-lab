with base as (
  select
    week_start,
    creator_id,
    listens,
    ad_revenue_usd,
    rpm_usd,
    ad_fill_rate
  from {{ ref('mart_creator_weekly_monetization') }}
),
w as (
  select
    *,
    lag(listens) over (partition by creator_id order by week_start) as prev_listens,
    lag(ad_revenue_usd) over (partition by creator_id order by week_start) as prev_revenue,
    lag(rpm_usd) over (partition by creator_id order by week_start) as prev_rpm,
    lag(ad_fill_rate) over (partition by creator_id order by week_start) as prev_fill
  from base
)
select
  week_start,
  creator_id,
  listens,
  ad_revenue_usd,
  rpm_usd,
  ad_fill_rate,
  prev_listens,
  prev_revenue,
  prev_rpm,
  prev_fill,
  (listens - prev_listens) as delta_listens,
  (ad_revenue_usd - prev_revenue) as delta_revenue,
  (rpm_usd - prev_rpm) as delta_rpm,
  (ad_fill_rate - prev_fill) as delta_fill
from w