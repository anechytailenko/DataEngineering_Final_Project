{{ config( materialized='view', tags=['daily'] ) }}

with source as (
    select * from {{ source('logistics', 'historical_weather_delays') }}
)

select
    date as weather_date,
    city,
    weather_condition,
    precipitation_mm,
    regional_delay_flag
from source