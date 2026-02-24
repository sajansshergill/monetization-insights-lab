with src as (
    select
        ad_ts,
        creator_id,
        show_id,
        episode_id,
        impressions,
        filled_impressions,
        cpm_used,
        revenue_usd
    from {{ source('raw', 'fct_ads') }}
)
select * from src