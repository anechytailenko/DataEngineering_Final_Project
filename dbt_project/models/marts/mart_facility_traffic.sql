{{ config( materialized='table', tags=['daily'] ) }}


with tracking as (
    select * from {{ ref('int_tracking_with_lag') }}
),

facilities as (
    select * from {{ ref('stg_facilities') }}
)

select
    f.facility_id,
    f.facility_name,
    f.city,
    count(t.event_id) as total_scans,
    count(distinct t.shipment_id) as unique_shipments_handled
from facilities f
    left join tracking t on f.facility_id = t.facility_id
group by
    1,
    2,
    3