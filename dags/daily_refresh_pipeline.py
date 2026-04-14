"""Daily full refresh + dbt build on models tagged `daily`.

extract_oltp (full) -> extract_json (full refresh) -> extract_minio -> dbt build --select tag:daily

Runs at 02:00 UTC, offset from the hourly DAG. Tasks run sequentially
because DuckDB is a single-writer DB.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# skip dbt cleanly if Student 2's project isn't in place yet
DBT_BUILD_DAILY = (
    f'if [ -f {DBT_PROJECT_DIR}/dbt_project.yml ]; then '
    f'cd {DBT_PROJECT_DIR} && '
    f'DBT_PROFILES_DIR={DBT_PROJECT_DIR} dbt build --select tag:daily; '
    f'else echo "dbt_project not set up yet, skipping dbt build"; fi'
)


def _extract_oltp_full(**_):
    from etl import extract_oltp
    return extract_oltp.run(incremental=False)


def _extract_json_full_refresh(**_):
    from etl import extract_json
    return extract_json.run(full_refresh=True)


def _extract_minio(**_):
    from etl import extract_minio
    return extract_minio.run()


default_args = {
    "owner": "student_1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="daily_refresh_pipeline",
    description="Full refresh of all sources + dbt build --select tag:daily",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",    # 02:00 UTC every day
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "daily"],
) as dag:

    extract_oltp_task = PythonOperator(
        task_id="extract_oltp_full",
        python_callable=_extract_oltp_full,
    )

    extract_json_task = PythonOperator(
        task_id="extract_json_full_refresh",
        python_callable=_extract_json_full_refresh,
    )

    extract_minio_task = PythonOperator(
        task_id="extract_minio_weather",
        python_callable=_extract_minio,
    )

    dbt_build_daily = BashOperator(
        task_id="dbt_build_daily",
        bash_command=DBT_BUILD_DAILY,
    )

    extract_oltp_task >> extract_json_task >> extract_minio_task >> dbt_build_daily
