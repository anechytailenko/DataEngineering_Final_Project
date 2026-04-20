{{ config( materialized='table', tags=['daily'] ) }}


with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
),

facilities as (
    select * from {{ ref('stg_facilities') }}
)

select f.facility_name, f.city, avg(t.hours_since_last_event) as avg_hours_delayed
from facilities f
    join tracking t on f.facility_id = t.facility_id
where
    t.hours_since_last_event is not null
group by
    1,
    2
order by avg_hours_delayed desc