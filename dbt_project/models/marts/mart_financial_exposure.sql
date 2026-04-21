{{ config(materialized='table', tags=['daily']) }}

with exceptions as (
    select * from {{ ref('mart_exceptions_analysis') }}
)

select
    origin_city,
    exception_type,
    count(shipment_id) as lost_or_damaged_count,
    sum(potential_financial_loss) as total_financial_exposure
from exceptions
where
    exception_type in ('Lost', 'Damaged')
group by
    1,
    2