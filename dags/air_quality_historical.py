'''
TODO: make this pipeline run daily
also make pipeline idempotent with the use of a process log and expand params
'''

import logging
import requests
from decimal import Decimal
from typing import Any
import os
from dotenv import load_dotenv
from datetime import (
    datetime, 
    timedelta,
    timezone,
    date,
    )

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import (
    task_group, 
    task, 
    dag,
    )

from cosmos.profiles import ClickhouseUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.operators import DbtDocsS3Operator

logger = logging.getLogger(__name__)
load_dotenv()

CLICKHOUSE_CONN = "clickhouse_conn"
MINIO_CONN = "minio_conn"
INCLUDE_PATH = '/opt/airflow/include'
AIR_QUAILTY_DBT_PROJECT_PATH=f"{INCLUDE_PATH}/dbt/air_quality_feature_engineering"

SOURCE_SCHEMA_NAME = 'weather_raw'
RAW_TABLE_NAME = 'air_pollution'
CITY_COORDINATES_TABLE_NAME = 'city_coordinates'

MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "air-quality")
MINIO_CONN_ID="minio_conn"

OPENWEATHER_API_SECRET_KEY = Variable.get("OPENWEATHER_API_SECRET_KEY") 

RAW_TABLE_COLUMNS = [
    "longitude_coordinate",
    "latitude_coordinate",
    "co_concentration",
    "no_concentration",
    "no2_concentration",
    "o3_concentration",
    "so2_concentration",
    "pm2_5_concentration",
    "pm10_concentration",
    "nh3_concentration",
    "aqi",
    "time_of_measurement",
    "inserted_by",
    # created_at not included
]

profile_config = ProfileConfig(
    profile_name="air_quality_dbt_profile",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping( 
         conn_id=CLICKHOUSE_CONN, 
         profile_args={
            "schema": "weather_features",
            "port": 9000,
            "driver": "native" 
        },
     ), 
)

project_config = ProjectConfig(
    dbt_project_path=AIR_QUAILTY_DBT_PROJECT_PATH
    )

render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,
        should_detach_multiple_parents_tests=True,
    )

with open(f"{INCLUDE_PATH}/doc_md/air_quality_historical.md", "r", encoding="utf-8") as file:
    DOC_MD = file.read() 

default_args = {
    "depends_on_past": True, 
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=30),
    'trigger_rule': 'all_success',
    "params":{
        "schema_name": SOURCE_SCHEMA_NAME,
        "raw_table_name": RAW_TABLE_NAME,
        "city_coordinates_table_name": CITY_COORDINATES_TABLE_NAME,
        }
}

def read_sql_script(relative_include_path:str) -> str:
    '''
    pass in the path here the same way you would pass it in to an Airflow operator
    '''
    with open(f"{INCLUDE_PATH}/{relative_include_path}", "r") as f:
        sql = f.read()
    return sql

def build_rows(city_dict: dict[str, str], payload: dict, dag_run_id: str, output_rows: list[tuple]) -> None:
    '''
    modifies output_rows in place to contain insert ready rows with data from city_dict and json payload
    based on fields in RAW_TABLE_COLUMNS
    '''
    lat = float(city_dict["latitude_coordinate"])
    lon = float(city_dict["longitude_coordinate"])

    row_count = 0

    for entry in payload.get("list", []):
        comp = entry.get("components", {}) or {}
        if comp:
            row_count += 1
            output_rows.append((
                lon,
                lat,
                comp.get("co"),
                comp.get("no"),
                comp.get("no2"),
                comp.get("o3"),
                comp.get("so2"),
                comp.get("pm2_5"),
                comp.get("pm10"),
                comp.get("nh3"),
                (entry.get("main") or {}).get("aqi"),
                entry["dt"],
                dag_run_id,
            ))
    logger.info(f"{row_count} queued for insertion")

@dag(
    dag_id="air_quality_historical",
    start_date=datetime(2023, 1, 1),
    schedule="0 6 1 * *", # get data for previous month on first of the next month at 6 AM
    template_searchpath=[INCLUDE_PATH],
    default_args=default_args,
    catchup=True,
    doc_md=DOC_MD,
    max_active_runs=1
)
def air_quality_historical():

    create_schema_if_not_exists = SQLExecuteQueryOperator(
        task_id='create_databases_if_not_exists',
        sql = f"sql/create_database.sql",
        conn_id=CLICKHOUSE_CONN,
    )
    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables_if_not_exists',
        sql = f"sql/create_raw_data_table.sql",
        split_statements=True,
        conn_id=CLICKHOUSE_CONN,
    )   

    @task
    def get_city_coordinates() -> list[dict[str, Any]]:
        import csv

        city_coordinates_list = []

        with open(f"{INCLUDE_PATH}/csv/latitudes_and_longitudes.csv", mode="r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            
            column_headers = next(reader) 
            number_column_headers = []

            for indx in range(len(column_headers)):
                column_header = column_headers[indx]
                if "_coordinate" in column_header:
                    number_column_headers.append(indx)

            for row in reader: # Each 'row' is a list of strings
                try:
                    city_dict = {}
                    for indx in range(len(column_headers)):
                        column_header = column_headers[indx]
                        value = row[indx]
                        if column_header in number_column_headers:
                            value = float(value)
                        city_dict[column_header] = value
                    city_coordinates_list.append(city_dict)
                except Exception as e:
                    logger.warning(f"Error encountered parsing city row in CSV: {e}")
                    
        return city_coordinates_list

    @task_group
    def write_city_and_raw_air_pollution_data(city_coordinates):
        """
        arguments:
            city_coordinates: An XComArg resolving to a list of dicts with city,country,latitude_coordinate,longitude_coordinate
        """
        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN)
        
        truncate_city_coordinates = SQLExecuteQueryOperator(
            task_id='truncate_city_table',
            sql = f"TRUNCATE TABLE {SOURCE_SCHEMA_NAME}.{CITY_COORDINATES_TABLE_NAME}",
            conn_id=CLICKHOUSE_CONN,
        )   

        @task
        def write_city_data(_city_coordinates: list[dict[str, Any]]):
            if not _city_coordinates:
                logger.info(f"Empty city_coordinates: {_city_coordinates}; exiting...")
                return
            logger.info(f"city_coordinates: {_city_coordinates}; exiting...")
            
            city_coordinates_insert_data = []
            column_names = list(_city_coordinates[0].keys())

            for row in _city_coordinates:
                city_coordinates_insert_data.append([row.get(x) for x in column_names])
                    
            ch_hook.bulk_insert_rows(
                table=f"{SOURCE_SCHEMA_NAME}.{CITY_COORDINATES_TABLE_NAME}", 
                rows=city_coordinates_insert_data, 
                column_names=column_names
            )

        @task
        def write_air_pollution_data(_city_coordinates: list[dict[str, Any]]):
            # TODO: make writes idempotent
            context = get_current_context()
            dag_run_id = context["dag_run"].run_id

            insert_rows_with_city_data = []
            
            today = datetime.now(timezone.utc)
            first_day_this_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            first_day_prev_month = (first_day_this_month - timedelta(days=1)).replace(day=1)

            start_time_unix = int(first_day_prev_month.timestamp())
            end_time_unix = int(first_day_this_month.timestamp())

            for city_dict in _city_coordinates:
                logger.info(f"Fetching historical data from {first_day_prev_month} to {first_day_this_month} for {city_dict.get('city')}, {city_dict.get('country')}")
                lat = city_dict.get("latitude_coordinate")
                lon = city_dict.get("longitude_coordinate")
                resp = requests.get(
                        "http://api.openweathermap.org/data/2.5/air_pollution/history",
                        params={
                            "lat": lat,
                            "lon": lon,
                            "start": start_time_unix,
                            "end": end_time_unix,
                            "appid": OPENWEATHER_API_SECRET_KEY,
                        },
                        timeout=30,
                    )
                resp.raise_for_status()
                payload = resp.json()

                build_rows(city_dict, payload, dag_run_id, insert_rows_with_city_data)
                
            if not insert_rows_with_city_data:
                logger.warning("No rows fetched — skipping insert")
            else:
                ch_hook.bulk_insert_rows(
                    table=f"{SOURCE_SCHEMA_NAME}.{RAW_TABLE_NAME}", 
                    rows=insert_rows_with_city_data, 
                    column_names=RAW_TABLE_COLUMNS
                )

        write_city_data_task = write_city_data(city_coordinates)
        write_air_pollution_data_task = write_air_pollution_data(city_coordinates)

        truncate_city_coordinates >> write_city_data_task >> write_air_pollution_data_task
    
    @task()
    def create_bucket_if_not_exists():
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        # Check if the bucket already exists
        if not hook.check_for_bucket(MINIO_BUCKET_NAME):
            hook.create_bucket(bucket_name=MINIO_BUCKET_NAME)
            logger.info(f"Bucket {MINIO_BUCKET_NAME} created successfully.")
        else:
            logger.info(f"Bucket {MINIO_BUCKET_NAME} already exists. Skipping creation.")
    
    dbt_models = DbtTaskGroup(
        group_id = "dbt_models",
        project_config = project_config,
        profile_config = profile_config,
        render_config = render_config,
    )

    generate_dbt_docs_to_minio_bucket = DbtDocsS3Operator(
        task_id="generate_dbt_docs_to_minio_bucket",
        project_dir=AIR_QUAILTY_DBT_PROJECT_PATH,
        profile_config=profile_config,
        connection_id=MINIO_CONN_ID,
        bucket_name=MINIO_BUCKET_NAME,
        folder_dir="/dbt_docs"
    )

    city_coordinates = get_city_coordinates()
    wcarapt = write_city_and_raw_air_pollution_data(city_coordinates)
    cbine = create_bucket_if_not_exists()

    create_schema_if_not_exists >> create_tables 
    
    create_tables >> city_coordinates

    city_coordinates >> wcarapt >> dbt_models

    cbine >> dbt_models >> generate_dbt_docs_to_minio_bucket 
    
    # TODO: after dbt_models does feature extraction, the model should be loaded from minio and used to predict features in the serving layer

air_quality_historical_dag = air_quality_historical()