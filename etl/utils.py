"""Shared helpers for the ETL scripts."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DEFAULT_DUCKDB_PATH = "data/warehouse/logistics.duckdb"


def get_duckdb_path() -> Path:
    path = Path(os.getenv("DUCKDB_PATH", DEFAULT_DUCKDB_PATH))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def duckdb_connect() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(get_duckdb_path()))
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    return conn


def get_oltp_conn_string() -> str:
    return (
        f"postgresql+psycopg2://"
        f"{os.getenv('OLTP_USER', 'logistics')}:"
        f"{os.getenv('OLTP_PASSWORD', 'logistics')}@"
        f"{os.getenv('OLTP_HOST', 'localhost')}:"
        f"{os.getenv('OLTP_PORT', '5433')}/"
        f"{os.getenv('OLTP_DB', 'logistics')}"
    )
