{{ config(materialized='table', tags=['daily']) }}

with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
)

select
    courier_notes,
    count(event_id) as frequency_count
from tracking
where
    courier_notes is not null
group by
    1
order by frequency_count desc