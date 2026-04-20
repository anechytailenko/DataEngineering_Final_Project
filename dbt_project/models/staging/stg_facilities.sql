{{ config( materialized='view', tags=['hourly', 'daily'] ) }}

with source as (
    select * from {{ source('logistics', 'facilities') }}
)

select
    facility_id,
    facility_name,
    facility_type,
    city,
    max_capacity_per_day
from source