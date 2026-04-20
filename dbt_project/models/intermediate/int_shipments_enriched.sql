{{ config( materialized='view', tags=['hourly', 'daily'] ) }}


with shipments as (
    select * from {{ ref('stg_shipments') }}
),

facilities as (
    select * from {{ ref('stg_facilities') }}
)

select
    s.shipment_id,
    s.sender_id,
    s.origin_facility_id,
    o.facility_name as origin_facility_name,
    o.city as origin_city,
    s.destination_facility_id,
    d.facility_name as destination_facility_name,
    d.city as destination_city,
    s.weight_kg,
    s.declared_value,
    s.shipping_cost,
    s.created_at_ts,
    s.estimated_delivery_date
from
    shipments s
    left join facilities o on s.origin_facility_id = o.facility_id
    left join facilities d on s.destination_facility_id = d.facility_id