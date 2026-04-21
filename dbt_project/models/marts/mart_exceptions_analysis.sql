{{ config(
    materialized='table',
    tags=['hourly', 'daily']
) }}


with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
),

shipments as (
    select * from {{ ref('int_shipments_enriched') }}
)

select
    t.event_id,
    t.shipment_id,
    s.origin_city,
    s.destination_city,
    t.status_name as exception_type,
    t.event_timestamp_ts as exception_time,
    t.courier_notes,
    s.declared_value as potential_financial_loss
from tracking t
    join shipments s on t.shipment_id = s.shipment_id
where
    t.is_exception = true