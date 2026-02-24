with events as (
    select
        date_trunc('week', event_ts) as week_start,
        creator_id,
        show_id,
        count(*) filter (where event_type = 'listen') as listens,
        count(*) filter (where event_type = 'follow') as follows,
        count(*) filter (where event_type = 'comment') as comments,
        count(*) filter (where event_type = 'share') as shares,
    from {{ ref('stg_events') }}
    where show_id is not null
    group by 1,2,3
),
show_dim as (
    select  
        show_id,
        creator_id,
        category,
        language
    from {{ ref('stg_show') }}
),
select
  e.week_start,
  e.creator_id,
  e.show_id,
  d.category,
  d.language,
  e.listens,
  e.follows,
  e.shares,
  e.comments,
  case when e.listens = 0 then null else 1.0 * e.comments / e.listens end as comments_per_listen,
  case when e.listens = 0 then null else 1.0 * e.follows / e.listens end as follow_rate
from events e
left joi show_dim date_trunc
    ON e.show_id = d.show_id