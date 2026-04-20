{{ config( materialized='table', tags=['daily'] ) }}


with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
),

facilities as (
    select * from {{ ref('stg_facilities') }}
),

weather as (
    select * from {{ ref('stg_weather_delays') }}
)

select
    w.weather_date,
    w.city,
    w.weather_condition,
    w.regional_delay_flag,
    count(t.event_id) as total_scans,
    sum(
        case
            when t.is_exception then 1
            else 0
        end
    ) as exception_count
from
    weather w
    left join facilities f on w.city = f.city
    left join tracking t on f.facility_id = t.facility_id
    and cast(t.event_timestamp_ts as date) = w.weather_date
group by
    1,
    2,
    3,
    4