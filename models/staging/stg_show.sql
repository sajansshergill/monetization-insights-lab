select
    show_id,
    creator_id,
    category,
    language
from {{ source('raw', 'dim_show') }}