{{ config(materialized='table', tags=['daily']) }}

with deliveries as (
    -- "Delivered" (code 50)
    select shipment_id, event_timestamp_ts as actual_delivery_time 
    from {{ ref('int_tracking_with_lag') }} 
    where status_code = 50
),
shipments as (
    select * from {{ ref('int_shipments_enriched') }}
)

select
    s.shipment_id,
    s.estimated_delivery_date,
    cast(
        d.actual_delivery_time as date
    ) as actual_delivery_date,
    case
        when cast(
            d.actual_delivery_time as date
        ) <= s.estimated_delivery_date then true
        else false
    end as is_on_time
from shipments s
    join deliveries d on s.shipment_id = d.shipment_id