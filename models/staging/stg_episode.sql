select
    episode_id,
    show_id,
    creator_id,
    publish_ts,
    duration_s
from {{ source('raw', 'dim_episode') }}