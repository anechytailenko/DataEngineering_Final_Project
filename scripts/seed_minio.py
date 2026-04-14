"""Generate a fake historical weather CSV and upload it to MinIO.

Run once; `python -m etl.extract_minio` then loads it into DuckDB.
Defaults: 180 days across the Ukrainian cities in facilities.city (~4300 rows).
"""
from __future__ import annotations

import io
import os
import random
from datetime import date, timedelta

import pandas as pd
from minio import Minio
from minio.error import S3Error

OBJECT_NAME = "historical_weather_delays.csv"
RANDOM_SEED = 42

CITIES = [
    "Kyiv", "Lviv", "Odesa", "Kharkiv", "Dnipro",
    "Vinnytsia", "Poltava", "Zaporizhzhia",
    "Ivano-Frankivsk", "Chernivtsi", "Uzhhorod", "Ternopil",
]

WEATHER_CONDITIONS = [
    ("Clear", 0.40, 0.0, 0.0),
    ("Cloudy", 0.25, 0.0, 0.5),
    ("Light Rain", 0.12, 0.5, 4.0),
    ("Heavy Rain", 0.07, 4.0, 30.0),
    ("Thunderstorm", 0.04, 10.0, 60.0),
    ("Light Snow", 0.06, 0.5, 5.0),
    ("Heavy Snow", 0.04, 5.0, 40.0),
    ("Fog", 0.02, 0.0, 0.1),
]


def pick_condition() -> tuple[str, float]:
    r = random.random()
    cumulative = 0.0
    for name, prob, lo, hi in WEATHER_CONDITIONS:
        cumulative += prob
        if r <= cumulative:
            return name, round(random.uniform(lo, hi), 2)
    return "Clear", 0.0


def build_dataframe(days: int = 180) -> pd.DataFrame:
    random.seed(RANDOM_SEED)
    today = date.today()
    start = today - timedelta(days=days)

    rows = []
    for offset in range(days + 1):
        day = start + timedelta(days=offset)
        for city in CITIES:
            condition, precip = pick_condition()
            delay_flag = condition in {"Heavy Rain", "Heavy Snow", "Thunderstorm"} and precip > 10
            rows.append({
                "date": day.isoformat(),
                "city": city,
                "weather_condition": condition,
                "precipitation_mm": precip,
                "regional_delay_flag": delay_flag,
            })
    return pd.DataFrame(rows)


def get_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    return Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Created bucket: {bucket}")
        else:
            print(f"Bucket exists: {bucket}")
    except S3Error as e:
        raise RuntimeError(f"Failed to access bucket {bucket}: {e}") from e


def main() -> None:
    bucket = os.getenv("MINIO_BUCKET", "logistics-raw")
    client = get_minio_client()
    ensure_bucket(client, bucket)

    df = build_dataframe()
    print(f"Generated {len(df)} weather rows across {df['city'].nunique()} cities.")

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    buf = io.BytesIO(csv_bytes)
    client.put_object(
        bucket,
        OBJECT_NAME,
        data=buf,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    print(f"Uploaded s3://{bucket}/{OBJECT_NAME}  ({len(csv_bytes):,} bytes)")


if __name__ == "__main__":
    main()
