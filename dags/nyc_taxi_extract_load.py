from __future__ import annotations

import os
import urllib.request

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TAXI_MONTH = "2024-01"
DATA_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{TAXI_MONTH}.parquet"
LOCAL_PATH = f"/usr/local/airflow/include/yellow_tripdata_{TAXI_MONTH}.parquet"
STAGE_FILENAME = f"yellow_tripdata_{TAXI_MONTH}.parquet"


def extract_taxi_data():
    """Download the monthly parquet file from NYC TLC's public CloudFront distribution."""
    os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)
    urllib.request.urlretrieve(DATA_URL, LOCAL_PATH)
    size_mb = os.path.getsize(LOCAL_PATH) / (1024 * 1024)
    print(f"Downloaded {LOCAL_PATH} ({size_mb:.1f} MB)")


def create_stage_and_format():
    """Create the Snowflake file format and internal stage (idempotent)."""
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


def upload_to_stage():
    """PUT the local parquet file into the Snowflake internal stage."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"PUT file://{LOCAL_PATH} @nyc_taxi.raw.taxi_stage OVERWRITE = TRUE AUTO_COMPRESS = FALSE")
        for row in cur.fetchall():
            print(row)
    finally:
        cur.close()
        conn.close()


def create_raw_table_and_load():
    """Infer schema from the staged parquet file, create the raw table if needed, then load it."""
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
                    LOCATION => '@nyc_taxi.raw.taxi_stage/{STAGE_FILENAME}',
                    FILE_FORMAT => 'nyc_taxi.raw.parquet_format'
                  )
                )
              );
        """)
        cur.execute(f"""
            COPY INTO nyc_taxi.raw.yellow_tripdata
            FROM @nyc_taxi.raw.taxi_stage/{STAGE_FILENAME}
            FILE_FORMAT = (FORMAT_NAME = 'nyc_taxi.raw.parquet_format')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT';
        """)
        for row in cur.fetchall():
            print(row)
    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="nyc_taxi_extract_load",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,  # trigger manually for now; we'll add a schedule later
    catchup=False,
    tags=["nyc-taxi", "extract-load"],
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

    extract >> stage_setup >> upload >> load