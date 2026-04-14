"""Generate JSON tracking-event logs from the shipments in Postgres.

One file per hour, so the hourly ETL can pick them up incrementally.
Output: data/raw/events/events_YYYY-MM-DD_HH.json, each a JSON array of
{event_id, shipment_id, status_code, facility_id, event_timestamp, courier_notes}.
facility_id can be null (in transit); courier_notes is often missing.

Run after generate_oltp_data.py:
    python scripts/generate_tracking_events.py
"""
from __future__ import annotations

import json
import os
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2

DB_CONFIG = {
    "host": os.getenv("OLTP_HOST", "localhost"),
    "port": int(os.getenv("OLTP_PORT", "5433")),
    "user": os.getenv("OLTP_USER", "logistics"),
    "password": os.getenv("OLTP_PASSWORD", "logistics"),
    "dbname": os.getenv("OLTP_DB", "logistics"),
}

OUTPUT_DIR = Path(os.getenv("EVENTS_DIR", "data/raw/events"))
RANDOM_SEED = 42

# Must match dbt seed status_codes_mapping.csv (Student 2 maintains it).
STATUS_LABEL_CREATED    = 10
STATUS_PICKED_UP        = 20
STATUS_ARRIVED_AT_HUB   = 30
STATUS_DEPARTED_HUB     = 35
STATUS_OUT_FOR_DELIVERY = 40
STATUS_DELIVERED        = 50  # terminal
STATUS_DELIVERY_FAILED  = 60
STATUS_DAMAGED          = 70  # terminal
STATUS_LOST             = 80  # terminal

# Each flow is a list of (status, (min_hours, max_hours)) pairs.
# Gap is sampled uniformly within the range and added to the previous event.
NORMAL_FLOW = [
    (STATUS_LABEL_CREATED,    (0, 0)),
    (STATUS_PICKED_UP,        (1, 4)),
    (STATUS_ARRIVED_AT_HUB,   (2, 10)),
    (STATUS_DEPARTED_HUB,     (1, 6)),
    (STATUS_OUT_FOR_DELIVERY, (2, 18)),
    (STATUS_DELIVERED,        (1, 8)),
]

FAILED_ATTEMPT_FLOW = [
    (STATUS_LABEL_CREATED,    (0, 0)),
    (STATUS_PICKED_UP,        (1, 4)),
    (STATUS_ARRIVED_AT_HUB,   (2, 10)),
    (STATUS_DEPARTED_HUB,     (1, 6)),
    (STATUS_OUT_FOR_DELIVERY, (2, 18)),
    (STATUS_DELIVERY_FAILED,  (3, 10)),
    (STATUS_OUT_FOR_DELIVERY, (12, 24)),  # next day
    (STATUS_DELIVERED,        (1, 8)),
]

DELAYED_FLOW = [
    (STATUS_LABEL_CREATED,    (0, 0)),
    (STATUS_PICKED_UP,        (1, 6)),
    (STATUS_ARRIVED_AT_HUB,   (4, 12)),
    (STATUS_DEPARTED_HUB,     (24, 72)),  # stuck at hub
    (STATUS_OUT_FOR_DELIVERY, (4, 18)),
    (STATUS_DELIVERED,        (1, 8)),
]

DAMAGED_FLOW = [
    (STATUS_LABEL_CREATED,    (0, 0)),
    (STATUS_PICKED_UP,        (1, 4)),
    (STATUS_ARRIVED_AT_HUB,   (2, 10)),
    (STATUS_DAMAGED,          (1, 8)),
]

LOST_FLOW = [
    (STATUS_LABEL_CREATED,    (0, 0)),
    (STATUS_PICKED_UP,        (1, 4)),
    (STATUS_LOST,             (24, 120)),
]

LIFECYCLE_CHOICES = [
    (0.78, NORMAL_FLOW),
    (0.10, FAILED_ATTEMPT_FLOW),
    (0.07, DELAYED_FLOW),
    (0.03, DAMAGED_FLOW),
    (0.02, LOST_FLOW),
]

COURIER_NOTES_POOL = [
    None, None, None, None,  # most events have no note
    "Customer not at home",
    "Signature collected",
    "Left at front desk",
    "Package handed to recipient",
    "Partial address provided",
    "Delivery rescheduled",
    "Weather delay",
    "High volume at hub",
]


def pick_lifecycle() -> list[tuple[int, tuple[int, int]]]:
    r = random.random()
    cumulative = 0.0
    for prob, flow in LIFECYCLE_CHOICES:
        cumulative += prob
        if r <= cumulative:
            return flow
    return NORMAL_FLOW


def facility_for_status(
    status: int,
    origin_id: int,
    destination_id: int,
    hub_ids: list[int],
) -> int | None:
    """Decide where a given scan happens. None means 'in transit'."""
    if status in (STATUS_LABEL_CREATED, STATUS_PICKED_UP):
        return origin_id
    if status in (STATUS_OUT_FOR_DELIVERY, STATUS_DELIVERED, STATUS_DELIVERY_FAILED):
        return destination_id
    if status in (STATUS_ARRIVED_AT_HUB, STATUS_DEPARTED_HUB):
        return random.choice(hub_ids)
    if status == STATUS_DAMAGED:
        return random.choice(hub_ids)
    if status == STATUS_LOST:
        return None  # lost in transit
    return None


def generate_events_for_shipment(
    shipment_id: str,
    created_at: datetime,
    origin_id: int,
    destination_id: int,
    hub_ids: list[int],
    now: datetime,
) -> list[dict]:
    flow = pick_lifecycle()
    events: list[dict] = []
    current_time = created_at

    for status, (gap_lo, gap_hi) in flow:
        if status != STATUS_LABEL_CREATED:
            gap_hours = random.uniform(gap_lo, gap_hi)
            current_time = current_time + timedelta(hours=gap_hours)

        # shipment still in flight, stop at the last past event
        if current_time > now:
            break

        events.append({
            "event_id": str(uuid.uuid4()),
            "shipment_id": shipment_id,
            "status_code": status,
            "facility_id": facility_for_status(status, origin_id, destination_id, hub_ids),
            "event_timestamp": current_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "courier_notes": random.choice(COURIER_NOTES_POOL),
        })

    return events


def fetch_shipments_and_hubs(cur):
    cur.execute("""
        SELECT shipment_id, origin_facility_id, destination_facility_id, created_at
        FROM shipments
        ORDER BY created_at
    """)
    shipments = cur.fetchall()

    cur.execute("""
        SELECT facility_id FROM facilities
        WHERE facility_type IN ('National Hub', 'Regional')
    """)
    hub_ids = [row[0] for row in cur.fetchall()]
    return shipments, hub_ids


def bucket_key(ts_str: str) -> str:
    # "2026-04-18T14:30:00Z" -> "2026-04-18_14"
    return f"{ts_str[:10]}_{ts_str[11:13]}"


def clear_output_dir(path: Path) -> None:
    if path.exists():
        for item in path.iterdir():
            if item.is_file() and item.suffix == ".json":
                item.unlink()
    else:
        path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    random.seed(RANDOM_SEED)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            shipments, hub_ids = fetch_shipments_and_hubs(cur)

    if not shipments:
        print("No shipments found. Run scripts/generate_oltp_data.py first.")
        return
    if not hub_ids:
        print("No hub facilities found.")
        return

    print(f"Loaded {len(shipments)} shipments and {len(hub_ids)} hubs.")
    print("Simulating tracking events ...")

    hourly: dict[str, list[dict]] = defaultdict(list)
    total_events = 0
    for shipment_id, origin_id, destination_id, created_at in shipments:
        events = generate_events_for_shipment(
            shipment_id=str(shipment_id),
            created_at=created_at,
            origin_id=origin_id,
            destination_id=destination_id,
            hub_ids=hub_ids,
            now=now,
        )
        for ev in events:
            hourly[bucket_key(ev["event_timestamp"])].append(ev)
        total_events += len(events)

    print(f"Generated {total_events} events across {len(hourly)} hourly buckets.")

    print(f"Writing to {OUTPUT_DIR}/ ...")
    clear_output_dir(OUTPUT_DIR)
    for bucket, events in hourly.items():
        events.sort(key=lambda e: e["event_timestamp"])
        path = OUTPUT_DIR / f"events_{bucket}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(events, f, ensure_ascii=False, indent=2)

    print(f"Done. {len(hourly)} files written.")


if __name__ == "__main__":
    main()
