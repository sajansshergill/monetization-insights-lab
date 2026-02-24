with creator_value as (
    select
        date_trunc('week', eveent_ts) as week_start,
        creator_id,
        count(*) as analytics_views
    from {{ ref('stg_events') }}
    where event_type = 'analytics_view'
    group by 1,2
),
monet as (
    select
        week_start,
        creator_id,
        listens,
        ad_revenue_usd,
        rpm_usd,
        aD_fill_rate
    from {{ ref('mart_creator_weekly_monetization') }}
),
creators as (
    select creator_id, tier, country
    from {{ ref('stg_creator') }}
)
select
    m.week_start,
    m.creator_id,
    c.tier,
    c.country,
    coalesce(c.analytics_views, 0) as analytics_views,
    case when coalesce(v.analytics_views, 0) > 0 then 1 else 0 end as analytics_active,
    m.listens,
    m.ad_revenue_usd,
    m.rpm_usd,
    m.ad_fill_rate
from monet m
left join creator_views v
    ON m.week_start = v.week_start
    and m.creator_id = v.creator_id
left join creators c
    ON m.creator_id = c.creator_id