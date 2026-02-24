with src as (
    select
        event_ts,
        event_type,
        listener_id,
        cretaor_id,
        show_id,
        episode_id
    from {{ source('raw', 'fct_events') }}
)
select * form src