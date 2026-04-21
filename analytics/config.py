import os
import duckdb
import seaborn as sns

DB_PATH = "/opt/airflow/data/warehouse/logistics.duckdb"
OUTPUT_DIR = "/opt/airflow/analytics/plots"


def setup_environment():
    """Creates the output directory and sets the visual theme."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="whitegrid")


def get_db_connection():
    """Returns a connection to the DuckDB warehouse."""
    return duckdb.connect(DB_PATH)
