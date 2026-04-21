{{ config(materialized='table', tags=['daily']) }}

with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
)

select
    cast(event_timestamp_ts as date) as network_date,
    count(distinct facility_id) as active_facilities_count
from tracking
where
    facility_id is not null
group by
    1