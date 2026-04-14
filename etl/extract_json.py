"""Ingest hourly JSON event files into raw.tracking_events.

Incremental by default: files already in raw._ingested_files are skipped.
FULL_REFRESH=1 wipes both tables and reloads from scratch.

    python -m etl.extract_json
    FULL_REFRESH=1 python -m etl.extract_json
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from etl.utils import duckdb_connect

EVENTS_DIR = Path(os.getenv("EVENTS_DIR", "data/raw/events"))

EVENTS_DDL = """
    CREATE TABLE IF NOT EXISTS raw.tracking_events (
        event_id         VARCHAR,
        shipment_id      VARCHAR,
        status_code      INTEGER,
        facility_id      INTEGER,
        event_timestamp  VARCHAR,   -- cast to timestamp in dbt staging
        courier_notes    VARCHAR,
        source_file      VARCHAR    -- provenance
    )
"""

INGESTED_FILES_DDL = """
    CREATE TABLE IF NOT EXISTS raw._ingested_files (
        file_name     VARCHAR PRIMARY KEY,
        row_count     INTEGER,
        ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""


def list_new_files(duck, events_dir: Path) -> list[Path]:
    all_files = sorted(events_dir.glob("events_*.json"))
    already = {
        row[0] for row in duck.execute(
            "SELECT file_name FROM raw._ingested_files"
        ).fetchall()
    }
    return [f for f in all_files if f.name not in already]


def load_file(path: Path) -> pd.DataFrame:
    df = pd.read_json(path)
    if df.empty:
        return df
    # keep column order stable even if a field is missing in the file
    for col in ("event_id", "shipment_id", "status_code",
                "facility_id", "event_timestamp", "courier_notes"):
        if col not in df.columns:
            df[col] = None
    df["source_file"] = path.name
    return df[[
        "event_id", "shipment_id", "status_code",
        "facility_id", "event_timestamp", "courier_notes", "source_file",
    ]]


def run(full_refresh: bool = False) -> dict:
    duck = duckdb_connect()

    if full_refresh:
        duck.execute("DROP TABLE IF EXISTS raw.tracking_events")
        duck.execute("DROP TABLE IF EXISTS raw._ingested_files")
    duck.execute(EVENTS_DDL)
    duck.execute(INGESTED_FILES_DDL)

    if not EVENTS_DIR.exists():
        print(f"extract_json: events dir {EVENTS_DIR} does not exist, nothing to do.")
        duck.close()
        return {"files_loaded": 0, "events_loaded": 0}

    new_files = list_new_files(duck, EVENTS_DIR)
    print(f"extract_json: {len(new_files)} new file(s) to ingest.")

    total_events = 0
    for path in new_files:
        df = load_file(path)
        if df.empty:
            # mark as processed so we don't re-scan it
            duck.execute(
                "INSERT INTO raw._ingested_files (file_name, row_count) VALUES (?, 0)",
                [path.name],
            )
            continue

        duck.register("df", df)
        duck.execute("INSERT INTO raw.tracking_events SELECT * FROM df")
        duck.unregister("df")
        duck.execute(
            "INSERT INTO raw._ingested_files (file_name, row_count) VALUES (?, ?)",
            [path.name, len(df)],
        )
        total_events += len(df)

    stats = {
        "files_loaded": len(new_files),
        "events_loaded": total_events,
        "mode": "full_refresh" if full_refresh else "incremental",
    }
    duck.close()
    print(f"extract_json: {stats}")
    return stats


if __name__ == "__main__":
    run(full_refresh=os.getenv("FULL_REFRESH") == "1")
