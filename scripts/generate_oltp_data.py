"""Populate the OLTP Postgres tables with generated facilities and shipments.

Idempotent: TRUNCATEs both tables before inserting.
Defaults: 24 facilities across Ukrainian cities, ~10k shipments over 60 days.

    python scripts/generate_oltp_data.py
    # or inside Airflow:
    docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_oltp_data.py
"""
from __future__ import annotations

import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
from faker import Faker
from psycopg2.extras import execute_values

# Default targets the host-exposed port; containerized runs set OLTP_HOST.
DB_CONFIG = {
    "host": os.getenv("OLTP_HOST", "localhost"),
    "port": int(os.getenv("OLTP_PORT", "5433")),
    "user": os.getenv("OLTP_USER", "logistics"),
    "password": os.getenv("OLTP_PASSWORD", "logistics"),
    "dbname": os.getenv("OLTP_DB", "logistics"),
}

N_SHIPMENTS = int(os.getenv("N_SHIPMENTS", "10000"))
DAYS_OF_HISTORY = int(os.getenv("DAYS_OF_HISTORY", "60"))
RANDOM_SEED = 42

FACILITY_SEED = [
    # (name, type, city, capacity)
    ("Kyiv Central Hub",       "National Hub",  "Kyiv",       50000),
    ("Lviv National Hub",      "National Hub",  "Lviv",       30000),
    ("Odesa National Hub",     "National Hub",  "Odesa",      28000),
    ("Kharkiv National Hub",   "National Hub",  "Kharkiv",    26000),
    ("Dnipro National Hub",    "National Hub",  "Dnipro",     24000),
    ("Kyiv East Regional",     "Regional",      "Kyiv",       12000),
    ("Kyiv West Regional",     "Regional",      "Kyiv",       12000),
    ("Lviv Regional",          "Regional",      "Lviv",       10000),
    ("Odesa Regional",         "Regional",      "Odesa",       9000),
    ("Kharkiv Regional",       "Regional",      "Kharkiv",     9000),
    ("Dnipro Regional",        "Regional",      "Dnipro",      8500),
    ("Vinnytsia Regional",     "Regional",      "Vinnytsia",   7000),
    ("Poltava Regional",       "Regional",      "Poltava",     6500),
    ("Zaporizhzhia Regional",  "Regional",      "Zaporizhzhia",7000),
    ("Kyiv Branch #12",        "Local Branch",  "Kyiv",        2000),
    ("Kyiv Branch #34",        "Local Branch",  "Kyiv",        2000),
    ("Lviv Branch #5",         "Local Branch",  "Lviv",        1500),
    ("Odesa Branch #3",        "Local Branch",  "Odesa",       1500),
    ("Kharkiv Branch #7",      "Local Branch",  "Kharkiv",     1500),
    ("Dnipro Branch #2",       "Local Branch",  "Dnipro",      1500),
    ("Ivano-Frankivsk Branch", "Local Branch",  "Ivano-Frankivsk", 1200),
    ("Chernivtsi Branch",      "Local Branch",  "Chernivtsi",  1000),
    ("Uzhhorod Branch",        "Local Branch",  "Uzhhorod",     900),
    ("Ternopil Branch",        "Local Branch",  "Ternopil",    1100),
]


def generate_facilities(cur) -> list[int]:
    """Insert facilities, return the assigned facility_ids."""
    rows = [
        (name, ftype, city, capacity)
        for (name, ftype, city, capacity) in FACILITY_SEED
    ]
    execute_values(
        cur,
        """
        INSERT INTO facilities (facility_name, facility_type, city, max_capacity_per_day)
        VALUES %s
        RETURNING facility_id
        """,
        rows,
    )
    return [row[0] for row in cur.fetchall()]


def generate_shipments(facility_ids: list[int], n: int, days: int) -> list[tuple]:
    fake = Faker()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = now - timedelta(days=days)

    shipments: list[tuple] = []
    for _ in range(n):
        origin, destination = random.sample(facility_ids, 2)
        offset_seconds = random.randint(0, days * 24 * 3600)
        created_at = start + timedelta(seconds=offset_seconds)
        # 0.1..30 kg, heavier is rarer
        weight_kg = round(random.triangular(0.1, 30.0, 2.0), 2)
        # base 40 UAH + 15 UAH/kg + noise
        shipping_cost = round(40 + weight_kg * 15 + random.uniform(-5, 25), 2)
        declared_value = round(random.uniform(50, 5000), 2)
        estimated_delivery_date = (created_at + timedelta(days=random.randint(1, 7))).date()

        shipments.append((
            str(uuid.uuid4()),
            fake.random_int(min=1000, max=9999),    # sender_id placeholder
            origin,
            destination,
            Decimal(str(weight_kg)),
            Decimal(str(declared_value)),
            Decimal(str(shipping_cost)),
            created_at,
            estimated_delivery_date,
        ))
    return shipments


def insert_shipments(cur, shipments: list[tuple]) -> None:
    execute_values(
        cur,
        """
        INSERT INTO shipments (
            shipment_id, sender_id,
            origin_facility_id, destination_facility_id,
            weight_kg, declared_value, shipping_cost,
            created_at, estimated_delivery_date
        )
        VALUES %s
        """,
        shipments,
        page_size=1000,
    )


def main() -> None:
    random.seed(RANDOM_SEED)
    Faker.seed(RANDOM_SEED)

    print(f"Connecting to {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']} ...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # order matters: shipments has an FK on facilities
            cur.execute("TRUNCATE TABLE shipments, facilities RESTART IDENTITY CASCADE;")

            print("Inserting facilities ...")
            facility_ids = generate_facilities(cur)
            print(f"  -> {len(facility_ids)} facilities")

            print(f"Generating {N_SHIPMENTS} shipments over the last {DAYS_OF_HISTORY} days ...")
            shipments = generate_shipments(facility_ids, N_SHIPMENTS, DAYS_OF_HISTORY)

            print("Inserting shipments ...")
            insert_shipments(cur, shipments)

        conn.commit()
    print("Done.")


if __name__ == "__main__":
    main()
