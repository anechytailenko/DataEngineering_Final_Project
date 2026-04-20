{{ config(materialized='table', tags=['daily']) }}

with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
),
facilities as (
    select * from {{ ref('stg_facilities') }}
)

select
    f.facility_name,
    f.city,
    cast(t.event_timestamp_ts as date) as scan_date,
    count(t.event_id) as daily_scans,
    f.max_capacity_per_day,
    (
        count(t.event_id) * 100.0 / f.max_capacity_per_day
    ) as utilization_pct
from facilities f
    join tracking t on f.facility_id = t.facility_id
group by
    1,
    2,
    3,
    5