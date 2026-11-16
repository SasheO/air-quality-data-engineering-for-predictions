"""
# Air Quality Live

## Overview
Fetches realtime air pollution data from the OpenWeather Air Pollution History API for each city in the `city_coordinates` table, covering the entirety of the previous calendar month. Parsed rows are bulk-inserted into `air_quality_datasets.raw` in ClickHouse, where incremental dbt models downstream transform them into features.

## Schedule
- Cron: `0 * * * *` — beginning of every hour
- Catchup disabled; `max_active_runs=1`.


## Tasks

*TODO* UPDATE THIS

### `create_databases_if_not_exists`
SQLExecuteQueryOperator. Runs `sql/create_database.sql` against ClickHouse to create `air_quality_datasets` if it does not exist.

### `create_tables_if_not_exists`
SQLExecuteQueryOperator. Runs `sql/create_raw_data_table.sql` (split into statements) to create the `raw` and `city_coordinates` tables if they do not exist.

### `refresh_city_coordinates_table`
Reads `include/csv/latitudes_and_longitudes.csv`, casts `*_coordinate` columns to `float`, and bulk-inserts all rows into `air_quality_datasets.city_coordinates`.

### `get_city_coordinates`
Runs `sql/get_deduplicated_city_coordinates.sql` and returns a list of dicts (one per city) with the columns needed for the API call and row building (e.g. `city_coordinates_id`, `city`, `country`, `latitude_coordinate`, `longitude_coordinate`). Output is XCom'd to `write_previous_month_data`.

### `write_previous_month_data`
For each city:
1. Computes the previous-month Unix window (`start`, `end`).
2. Calls `http://api.openweathermap.org/data/2.5/air_pollution/history` with `lat`, `lon`, `start`, `end`, `appid`.
3. Parses each hourly entry via `build_rows` into a tuple matching `RAW_TABLE_COLUMNS`; entries with empty `components` are skipped.
4. Collects all rows across all cities and performs one bulk insert into `air_quality_datasets.raw`. Skips the insert with a warning if no rows were produced.

## Required Variables
| Variable | Description |
|---|---|
| `OPENWEATHER_API_SECRET_KEY` | OpenWeather API key used for history requests. Set as an encrypted Airflow Variable. |

Also required (as Airflow Connections):
| Connection | Description |
|---|---|
| `clickhouse_conn` | ClickHouse connection used by all SQL and insert tasks. |
"""
import logging
import requests
from typing import Any
import os
from dotenv import load_dotenv
from datetime import (
    datetime, 
    timedelta,
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

from air_quality_helpers import (
    build_rows,
    RAW_TABLE_COLUMNS,
    transform_feature_rows_for_model_readiness,
    )

logger = logging.getLogger(__name__)

CLICKHOUSE_CONN = "clickhouse_conn"
MINIO_CONN = "minio_conn"
INCLUDE_PATH = '/opt/airflow/include'
AIR_QUAILTY_DBT_PROJECT_PATH=f"{INCLUDE_PATH}/dbt/air_quality_feature_engineering"
EMAIL_ON_FAILURE_LIST = [os.getenv("MY_EMAIL")]

SOURCE_SCHEMA_NAME = 'weather_raw'
FEATURES_SCHEMA_NAME = 'weather_features'
AIR_QUALITY_RAW_TABLE_NAME = 'air_pollution'
CITY_COORDINATES_TABLE_NAME = 'city_coordinates'

MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "air-quality")
MINIO_CONN_ID="minio_conn"

OPENWEATHER_API_SECRET_KEY = Variable.get("OPENWEATHER_API_SECRET_KEY") 
SECRET_SIGNING_KEY = os.getenv("SECRET_SIGNING_KEY")

# TODO: think of an automatic way of getting the most recently trained and accurate model that does not involve passing in the model key
MODEL_KEY = "air_quality_models/train-2026-11-15/model.pkl"


profile_config = ProfileConfig(
    profile_name="air_quality_dbt_profile",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping( 
         conn_id=CLICKHOUSE_CONN, 
         profile_args={
            "schema": FEATURES_SCHEMA_NAME,
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
        exclude=["tag:pit-correctness-test"],
    )

default_args = {
    "depends_on_past": True, 
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    'email': EMAIL_ON_FAILURE_LIST,
    'email_on_failure': False, # TODO: should be True
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=30),
    'trigger_rule': 'all_success',
    "params":{
        "schema_name": SOURCE_SCHEMA_NAME,
        "air_quality_table_name": AIR_QUALITY_RAW_TABLE_NAME,
        "air_quality_staging_table_name": f"{AIR_QUALITY_RAW_TABLE_NAME}_staging",
        "city_coordinates_table_name": CITY_COORDINATES_TABLE_NAME,
        "air_quality_audit_table_name": f"{AIR_QUALITY_RAW_TABLE_NAME}_audit_table",
        }
}

def read_sql_script(relative_include_path:str) -> str:
    '''
    pass in the path here the same way you would pass it in to an Airflow operator
    '''
    with open(f"{INCLUDE_PATH}/{relative_include_path}", "r") as f:
        sql = f.read()
    return sql

@dag(
    dag_id="air_quality_live",
    start_date=datetime(2026, 11, 1),
    schedule="0 * * * *", # get data for this hour at **:00
    template_searchpath=[INCLUDE_PATH],
    default_args=default_args,
    catchup=False,
    doc_md=__doc__,
    max_active_runs=1,
    tags=["air_quality", "model_serving"],
)
def air_quality_live():

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
            
            logger.info(f"city_coordinates: {_city_coordinates}")
            
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
        def ingest_air_pollution_data(_city_coordinates: list[dict[str, Any]]):
            context = get_current_context()
            dag_run_id = context["dag_run"].run_id
            
            insert_rows_with_city_data = []

            for city_dict in _city_coordinates:
                logger.info(f"Fetching current air pollution data for {city_dict.get('city')}, {city_dict.get('country')}")
                lat = city_dict.get("latitude_coordinate")
                lon = city_dict.get("longitude_coordinate")
                resp = requests.get(
                        "http://api.openweathermap.org/data/2.5/air_pollution",
                        params={
                            "lat": lat,
                            "lon": lon,
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
                    table=f"{SOURCE_SCHEMA_NAME}.{default_args["params"]["air_quality_staging_table_name"]}", 
                    rows=insert_rows_with_city_data, 
                    column_names=RAW_TABLE_COLUMNS
                )

        write_air_pollution_data = SQLExecuteQueryOperator(
        task_id='write_air_pollution_data',
        sql = f"sql/write_air_pollution_data.sql",
        conn_id=CLICKHOUSE_CONN,
    )
        write_city_data_task = write_city_data(city_coordinates)
        ingest_air_pollution_data_task = ingest_air_pollution_data(city_coordinates)

        truncate_city_coordinates >> write_city_data_task >> ingest_air_pollution_data_task >> write_air_pollution_data
    
    @task()
    def create_bucket_if_not_exists():
        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        if not hook.check_for_bucket(MINIO_BUCKET_NAME):
            hook.create_bucket(bucket_name=MINIO_BUCKET_NAME)
            logger.info(f"Bucket {MINIO_BUCKET_NAME} created successfully.")
        else:
            logger.info(f"Bucket {MINIO_BUCKET_NAME} already exists. Skipping creation.")
    
    dbt_models = DbtTaskGroup( # TODO: separate staging models target schema/database from snapshots and marts, if possible to maintain logical separation of silver/gold layers
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

    @task_group
    def refresh_online_table():
        model = None
        MODEL_INFO_JSON_KEY = MODEL_KEY.rsplit("/", 1)[0] + "/" + "model_info.json"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN)
        clickhouse_client = clickhouse_hook.get_client()

        create_live_data_view_if_not_exists = SQLExecuteQueryOperator(
            task_id='create_live_data_view_if_not_exists',
            sql = f"""create view if not exists {FEATURES_SCHEMA_NAME}.feature_air_pollution_latest
            as
            SELECT 
                *
            FROM {FEATURES_SCHEMA_NAME}.feature_air_pollution
            where time_of_measurement_utc >= (now('UTC') - (INTERVAL 6 HOUR + INTERVAL 30 MINUTE))
            and aqi_6_hour_ahead is null
            COMMENT 'Gets the most recent rows for which the target column of aqi_6_hours_ahead is not known from {FEATURES_SCHEMA_NAME}.feature_air_pollution';
            """,
            conn_id=CLICKHOUSE_CONN,
        )

        @task
        def extract_model_and_make_prediction():
            import hmac, hashlib, json, pickle

            if not s3_hook.check_for_key(MODEL_INFO_JSON_KEY, MINIO_BUCKET_NAME):
                raise FileNotFoundError(f"Could not find the json metadata file: {MODEL_INFO_JSON_KEY}")
            
            if not s3_hook.check_for_key(MODEL_KEY, MINIO_BUCKET_NAME):
                raise FileNotFoundError(f"Could not find the chosen model: {MODEL_KEY}")
            
            json_metadata_dict = json.loads(s3_hook.get_key(MODEL_INFO_JSON_KEY, MINIO_BUCKET_NAME).get()["Body"].read())
            model_s3_object = s3_hook.get_key(MODEL_KEY, MINIO_BUCKET_NAME)
            model_obj = model_s3_object.get()['Body'].read()

            model_name = MODEL_KEY.rsplit("/", 1)[-1]

            received_signature = json_metadata_dict.get(model_name, {}).get("signature", "")
            expected_signature = hmac.new(SECRET_SIGNING_KEY.encode('utf-8'), model_obj, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(received_signature, expected_signature):
                raise ValueError(f"Signature mismatch on {MODEL_KEY}")
            
            model = pickle.loads(model_obj)

            logger.info(f"model object type: {type(model)}, making predictions")
            
            air_pollution_features_df = clickhouse_client.query_df("SELECT * FROM weather_features.feature_air_pollution order by time_of_measurement")
            air_pollution_features_df = air_pollution_features_df.dropna()
            features,_ = transform_feature_rows_for_model_readiness(air_pollution_features_df)
            predictions=model.predict(features)
            
            # use these values to recreate online table
            pass

        create_live_data_view_if_not_exists >> extract_model_and_make_prediction()

        pass

    # TODO: when fetching hourly data, also write current aqi to five hours ago for the respective columns.
    '''
    for writing target to five hours ago, first get all current city rows from weather_features.city_coordinates snapshot to get their respective lat_and_long_encoding_id
    
    map cities to their lat and long encoding id in python.
    use a dynamic task to expand the params and run a lightweight update for each of the cities/lat_and_long_encoding_ids
    '''

    city_coordinates = get_city_coordinates()
    wcarapt = write_city_and_raw_air_pollution_data(city_coordinates)
    cbine = create_bucket_if_not_exists()
    rot = refresh_online_table()

    create_schema_if_not_exists >> create_tables 
    
    create_tables >> city_coordinates

    city_coordinates >> wcarapt >> dbt_models >> rot

    cbine >> dbt_models >> generate_dbt_docs_to_minio_bucket 
    
    
air_quality_live_dag = air_quality_live()