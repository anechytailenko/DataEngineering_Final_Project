{{ config(materialized='table', tags=['daily']) }}

with shipments as (
    select * from {{ ref('int_shipments_enriched') }}
)

select
    case
        when weight_kg < 5.0 then 'Light (<5kg)'
        when weight_kg < 15.0 then 'Medium (5-15kg)'
        else 'Heavy (>15kg)'
    end as weight_bracket,
    count(shipment_id) as shipment_count,
    avg(shipping_cost) as avg_cost_per_shipment
from shipments
group by
    1