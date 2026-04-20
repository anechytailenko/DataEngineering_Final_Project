{{ config(materialized='table', tags=['daily']) }}

with lifecycle as (
    select * from {{ ref('mart_shipment_lifecycle') }}
)

select
    origin_city,
    destination_city,
    count(shipment_id) as total_shipments,
    avg(total_transit_time_hours) as avg_transit_hours,
    sum(
        case
            when had_exception then 1
            else 0
        end
    ) as exception_count
from lifecycle
group by
    1,
    2