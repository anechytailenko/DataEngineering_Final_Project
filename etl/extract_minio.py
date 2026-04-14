"""Pull the historical weather CSV from MinIO into raw.historical_weather_delays.

Full replace every run. Seed the bucket first with
`python scripts/seed_minio.py`.
"""
from __future__ import annotations

import io
import os

import pandas as pd
from minio import Minio

from etl.utils import duckdb_connect

WEATHER_OBJECT = "historical_weather_delays.csv"


def get_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    return Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def download_csv(client: Minio, bucket: str, object_name: str) -> pd.DataFrame:
    response = client.get_object(bucket, object_name)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()
    return pd.read_csv(io.BytesIO(data))


def run() -> dict:
    bucket = os.getenv("MINIO_BUCKET", "logistics-raw")
    client = get_minio_client()

    if not client.bucket_exists(bucket):
        raise RuntimeError(
            f"Bucket '{bucket}' does not exist. "
            f"Run scripts/seed_minio.py first."
        )

    df = download_csv(client, bucket, WEATHER_OBJECT)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    duck = duckdb_connect()
    duck.register("df", df)
    duck.execute(
        "CREATE OR REPLACE TABLE raw.historical_weather_delays AS SELECT * FROM df"
    )
    duck.unregister("df")
    duck.close()

    stats = {"weather_rows_loaded": len(df), "source_object": WEATHER_OBJECT}
    print(f"extract_minio: {stats}")
    return stats


if __name__ == "__main__":
    run()
