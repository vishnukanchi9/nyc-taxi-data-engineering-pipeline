from __future__ import annotations

import os
import urllib.request
import urllib.error

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import ExecutionMode

DBT_PROJECT_PATH = "/usr/local/airflow/include/nyc_taxi_dbt"


def _month_str(data_interval_start):
    return data_interval_start.strftime("%Y-%m")


def extract_taxi_data(**context):
    month = _month_str(context["data_interval_start"])
    data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
    local_path = f"/usr/local/airflow/include/yellow_tripdata_{month}.parquet"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        urllib.request.urlretrieve(data_url, local_path)
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"Could not download data for {month} (HTTP {e.code}). "
            f"NYC TLC publishes with a ~2 month lag, so this month may not be "
            f"released yet. URL tried: {data_url}"
        ) from e
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"Downloaded {local_path} ({size_mb:.1f} MB) for month {month}")


def create_stage_and_format():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE FILE FORMAT IF NOT EXISTS nyc_taxi.raw.parquet_format
              TYPE = PARQUET;
        """)
        cur.execute("""
            CREATE STAGE IF NOT EXISTS nyc_taxi.raw.taxi_stage
              FILE_FORMAT = nyc_taxi.raw.parquet_format;
        """)
    finally:
        cur.close()
        conn.close()


def upload_to_stage(**context):
    month = _month_str(context["data_interval_start"])
    local_path = f"/usr/local/airflow/include/yellow_tripdata_{month}.parquet"
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"PUT file://{local_path} @nyc_taxi.raw.taxi_stage OVERWRITE = TRUE AUTO_COMPRESS = FALSE")
        for row in cur.fetchall():
            print(row)
    finally:
        cur.close()
        conn.close()


def create_raw_table_and_load(**context):
    month = _month_str(context["data_interval_start"])
    stage_filename = f"yellow_tripdata_{month}.parquet"
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS nyc_taxi.raw.yellow_tripdata
              USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                  INFER_SCHEMA(
                    LOCATION => '@nyc_taxi.raw.taxi_stage/{stage_filename}',
                    FILE_FORMAT => 'nyc_taxi.raw.parquet_format'
                  )
                )
              );
        """)
        cur.execute(f"""
            COPY INTO nyc_taxi.raw.yellow_tripdata
            FROM @nyc_taxi.raw.taxi_stage/{stage_filename}
            FILE_FORMAT = (FORMAT_NAME = 'nyc_taxi.raw.parquet_format')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT';
        """)
        for row in cur.fetchall():
            print(row)
    finally:
        cur.close()
        conn.close()


profile_config = ProfileConfig(
    profile_name="nyc_taxi_dbt",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "nyc_taxi",
            "schema": "staging",
        },
    ),
)

project_config = ProjectConfig(DBT_PROJECT_PATH)
execution_config = ExecutionConfig(execution_mode=ExecutionMode.LOCAL)


with DAG(
    dag_id="nyc_taxi_full_pipeline",
    start_date=pendulum.datetime(2026, 3, 1, tz="UTC"),  # safely within TLC's ~2-month publish lag
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,  # run months sequentially, not in parallel
    tags=["nyc-taxi", "full-pipeline"],
) as dag:

    extract = PythonOperator(
        task_id="extract_taxi_data",
        python_callable=extract_taxi_data,
    )

    stage_setup = PythonOperator(
        task_id="create_stage_and_format",
        python_callable=create_stage_and_format,
    )

    upload = PythonOperator(
        task_id="upload_to_stage",
        python_callable=upload_to_stage,
    )

    load = PythonOperator(
        task_id="create_raw_table_and_load",
        python_callable=create_raw_table_and_load,
    )

    transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

    extract >> stage_setup >> upload >> load >> transform