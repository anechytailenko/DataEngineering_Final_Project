{{ config( materialized='view', tags=['hourly', 'daily'] ) }}

with source as (
    select * from {{ source('logistics', 'shipments') }}
)

select
    shipment_id,
    sender_id,
    origin_facility_id,
    destination_facility_id,
    weight_kg,
    declared_value,
    shipping_cost,
    cast(created_at as timestamp) as created_at_ts,
    estimated_delivery_date
from source