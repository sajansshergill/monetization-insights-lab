select
    creator_id,
    country,
    tier,
    join_ts
from {{ source('raw', 'dim_creator') }}