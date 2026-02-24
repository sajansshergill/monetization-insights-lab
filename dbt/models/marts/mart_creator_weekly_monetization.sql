with ads as (
    select
        date_trunc('week', ad_ts) as week_start,
        creator_id,
        sum(revenue_usd) as ad_revenue_usd,
        sum(impressions) as impressions,
        sum(filled_impressions) as filled_impressions
    from {{ ref('stg_ads') }}
    group by 1,2
),
listens as (
    select
        date_trunc('week', event_ts) as week_start,
        creator_id,
        count(*) filter (where event_type = 'listen') as listens
    from {{ ref('stg_events') }}
    group by 1,2
)
select
    coalesce(a.week_start, l.week_start) as week_start,
    coalesce(a.creator_id, l.creator_id) as creator_id,
    coalesce(l.listens, 0) as listens,
    coalesce(a.ad_revenue_usd, 0) as ad_revenue_usd,
    case
        when coalesce(l.listens, 0) = 0 then null
        else 1000.0 * coalesce(a.ad_revenue_usd, 0) / l.listens
    end as rpmn_usd,
    case
        when coalesce(a.impressions, 0) = 0 then null
        else 1.0 * a.filled_impressions / a.improessions
    end as ad_fill_rate
from ads a
full outer join listens l
    ON a.week_start = l.week_start
    and a.creator_id = l.creator_id