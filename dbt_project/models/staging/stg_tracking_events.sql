{{ config( materialized='view', tags=['hourly', 'daily'] ) }}

with source as (
    select * from {{ source('logistics', 'tracking_events') }}
)

select
    event_id,
    shipment_id,
    status_code,
    facility_id,
    cast(event_timestamp as timestamp) as event_timestamp_ts,
    courier_notes,
    source_file
from source