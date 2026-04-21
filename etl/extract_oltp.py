"""Load facilities and shipments from Postgres into DuckDB (raw schema).

facilities is always a full replace. shipments is full by default, or
incremental (created_at > max in DuckDB) when INCREMENTAL=1.

    python -m etl.extract_oltp
    INCREMENTAL=1 python -m etl.extract_oltp
"""

from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text

from etl.utils import duckdb_connect, get_oltp_conn_string


def extract_facilities(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
            SELECT facility_id,
                   facility_name,
                   facility_type,
                   city,
                   max_capacity_per_day
            FROM facilities
        """
            )
        )
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def extract_shipments(engine, since=None) -> pd.DataFrame:
    query = """
        SELECT shipment_id,
               sender_id,
               origin_facility_id,
               destination_facility_id,
               weight_kg,
               declared_value,
               shipping_cost,
               created_at,
               estimated_delivery_date
        FROM shipments
    """
    params: dict = {}
    if since is not None:
        query += " WHERE created_at > :since"
        params["since"] = since
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def get_max_shipment_ts(duck):
    exists = duck.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name = 'shipments'"
    ).fetchone()
    if not exists:
        return None
    row = duck.execute("SELECT MAX(created_at) FROM raw.shipments").fetchone()
    return row[0] if row and row[0] is not None else None


def run(incremental: bool = False) -> dict:
    engine = create_engine(get_oltp_conn_string())
    duck = duckdb_connect()

    facilities_df = extract_facilities(engine)
    duck.execute(
        "CREATE OR REPLACE TABLE raw.facilities AS SELECT * FROM facilities_df"
    )

    since = get_max_shipment_ts(duck) if incremental else None
    shipments_df = extract_shipments(engine, since=since)

    if incremental and since is not None:
        duck.execute("INSERT INTO raw.shipments SELECT * FROM shipments_df")
    else:
        duck.execute(
            "CREATE OR REPLACE TABLE raw.shipments AS SELECT * FROM shipments_df"
        )

    stats = {
        "facilities_loaded": len(facilities_df),
        "shipments_loaded": len(shipments_df),
        "mode": "incremental" if (incremental and since is not None) else "full",
        "since": str(since) if since else None,
    }
    duck.close()
    engine.dispose()
    print(f"extract_oltp: {stats}")
    return stats


if __name__ == "__main__":
    run(incremental=os.getenv("INCREMENTAL") == "1")
