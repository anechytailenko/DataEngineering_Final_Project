"""Hourly incremental ETL + dbt build on models tagged `hourly`.

extract_oltp (incremental) -> extract_json (incremental) -> dbt build --select tag:hourly

Only the delta is touched; full refreshes are the daily DAG's job.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# skip dbt cleanly if Student 2's project isn't in place yet
DBT_BUILD_HOURLY = (
    f'if [ -f {DBT_PROJECT_DIR}/dbt_project.yml ]; then '
    f'cd {DBT_PROJECT_DIR} && '
    f'DBT_PROFILES_DIR={DBT_PROJECT_DIR} dbt build --select tag:hourly; '
    f'else echo "dbt_project not set up yet, skipping dbt build"; fi'
)


def _extract_oltp_incremental(**_):
    from etl import extract_oltp
    return extract_oltp.run(incremental=True)


def _extract_json_incremental(**_):
    from etl import extract_json
    return extract_json.run(full_refresh=False)


default_args = {
    "owner": "student_1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hourly_etl_pipeline",
    description="Incremental OLTP + JSON extract, then dbt build --select tag:hourly",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "hourly"],
) as dag:

    extract_oltp_task = PythonOperator(
        task_id="extract_oltp_incremental",
        python_callable=_extract_oltp_incremental,
    )

    extract_json_task = PythonOperator(
        task_id="extract_json_incremental",
        python_callable=_extract_json_incremental,
    )

    dbt_build_hourly = BashOperator(
        task_id="dbt_build_hourly",
        bash_command=DBT_BUILD_HOURLY,
    )

    extract_oltp_task >> extract_json_task >> dbt_build_hourly
