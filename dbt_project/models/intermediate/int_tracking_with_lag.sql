{{ config( materialized='view', tags=['hourly', 'daily'] ) }}


with tracking as (
    select * from {{ ref('stg_tracking_events') }}
),

statuses as (
    select * from {{ ref('status_codes_mapping') }}
),

windowed_tracking as (
    select
        t.event_id,
        t.shipment_id,
        t.status_code,
        s.status_name,
        s.is_exception,
        t.facility_id,
        t.event_timestamp_ts,
        t.courier_notes,

-- time of previous status of the given shipment

lag(t.event_timestamp_ts) over (
            partition by t.shipment_id 
            order by t.event_timestamp_ts
        ) as previous_event_timestamp_ts

    from tracking t
    left join statuses s on t.status_code = s.status_code
)

select
    *,
    -- diff in hour
    date_diff (
        'minute',
        previous_event_timestamp_ts,
        event_timestamp_ts
    ) / 60.0 as hours_since_last_event
from windowed_tracking