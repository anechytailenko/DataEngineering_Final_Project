{{ config(materialized='table', tags=['daily']) }}

with shipments as (
    select * from {{ ref('int_shipments_enriched') }}
)

select
    cast(created_at_ts as date) as shipment_date,
    count(shipment_id) as total_shipments,
    sum(shipping_cost) as total_revenue,
    sum(weight_kg) as total_weight_kg
from shipments
group by
    1