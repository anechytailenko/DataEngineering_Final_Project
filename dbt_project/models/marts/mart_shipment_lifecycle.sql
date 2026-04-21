{{ config( materialized='table', tags=['daily'] ) }}


with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
),

shipments as (
    select * from {{ ref('int_shipments_enriched') }}
)

select
    s.shipment_id,
    s.origin_city,
    s.destination_city,
    s.weight_kg,
    s.shipping_cost,
    count(t.event_id) as total_tracking_events,
    sum(t.hours_since_last_event) as total_transit_time_hours,
    bool_or (t.is_exception) as had_exception
from shipments s
    left join tracking t on s.shipment_id = t.shipment_id
group by
    1,
    2,
    3,
    4,
    5