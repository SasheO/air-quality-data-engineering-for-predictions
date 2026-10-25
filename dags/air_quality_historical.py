'''
plan:
    get json data from API monthly for the past five years
    load the cities from csv into table if not exists
    get data in python operator function for the entirety of the previous month, parse data into fomat that can be inserted into clickhouse, do bulk inserts not n+1
    create incremental dbt models that transform the data into desired features in clickhouse
        - [ ] decide how to normalise features, what features to get

'''
from datetime import (
    datetime, 
    timedelta,
    timezone,
    )
from airflow.decorators import (
    task_group, 
    task, 
    dag,
    )
import logging
import requests
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import get_current_context
from airflow.models import Variable

logger = logging.getLogger(__name__)

CLICKHOUSE_CONN = "clickhouse_conn"
MINIO_CONN = "minio_conn"
INCLUDE_PATH = '/opt/airflow/include'

SCHEMA_NAME = 'air_quality_datasets'
RAW_TABLE_NAME = 'raw'
CITY_COORDINATES_TABLE = 'city_coordinates'

RAW_TABLE_COLUMNS = [
    "city_coordinates_id",
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
    "datetime",
    "inserted_by",
    # created_at not included
]

OPENWEATHER_API_SECRET_KEY = Variable.get("OPENWEATHER_API_SECRET_KEY") # TODO: get this from airflow variables

with open(f"{INCLUDE_PATH}/doc_md/air_quality_historical.md", "r", encoding="utf-8") as file:
    DOC_MD = file.read() # TODO: complete the MD file

default_args = {
    "depends_on_past": True, # so if a relationship data test fails for example (they are not fail-fast but run after all berka tasks), data quality issues don't compound. The DE gets a notification and can resolve the issue before future runs.
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=30),
    'trigger_rule': 'all_success',
    'do_xcom_push': False, 
    "params":{
        "schema_name": SCHEMA_NAME,
        "raw_table_name": RAW_TABLE_NAME,
        "city_coordinates_table_name": CITY_COORDINATES_TABLE,
        }
}

def read_sql_script(relative_include_path:str) -> str:
    '''
    pass in the path here the same way you 
    '''
    with open(f"{INCLUDE_PATH}/{relative_include_path}", "r") as f:
        sql = f.read()
    return sql

def build_rows(city_dict: dict[str, str], payload: dict, dag_run_id: str, output_rows: list[tuple]) -> None:
    '''
    modifies output_rows in place to contain insert ready rows with data from city_dict and json payload
    based on fields in RAW_TABLE_COLUMNS
    '''
    city = city_dict["city"]
    country = city_dict["country"]
    city_coordinates_id = city_dict["city_coordinates_id"]
    lat = payload.get("coord", {}).get("lat") or float(city_dict["latitude_coordinate"])
    lon = payload.get("coord", {}).get("lon")  or float(city_dict["longitude_coordinate"])

    row_count = 0

    for entry in payload.get("list", []):
        comp = entry.get("components", {}) or {}
        if comp:
            row_count += 1
            output_rows.append((
                city_coordinates_id,
                lon,                                       # longitude Decimal(10,7)
                lat,                                       # latitude  Decimal(10,7)
                comp.get("co"),                            # Decimal(8,3)
                comp.get("no"),
                comp.get("no2"),
                comp.get("o3"),
                comp.get("so2"),
                comp.get("pm2_5"),
                comp.get("pm10"),
                comp.get("nh3"),
                (entry.get("main") or {}).get("aqi", 0),      # UInt8; 0 signified invalid values because valid is 1-5
                entry["dt"],                               # unix seconds → DateTime
                dag_run_id,
            ))
    logger.info(f"{row_count} queued for insertion")

@dag(
    dag_id="air_quality_historical",
    start_date=datetime(2021, 2, 1),
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
    def refresh_city_coordinates_table():
        import csv

        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN)

        data = []

        with open(f"{INCLUDE_PATH}/csv/latitudes_and_longitudes.csv", mode="r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            
            # Optional: Skip or capture the header row
            column_headers = next(reader) 
            number_column_headers = []

            for indx in range(len(column_headers)):
                column_header = column_headers[indx]
                if "_coordinate" in column_header:
                    number_column_headers.append(indx)

            for row in reader:
                # Each 'row' is a list of strings
                for indx in number_column_headers:
                    row[indx] = float(row[indx])
                data.append(row)
        
        ch_hook.bulk_insert_rows(
            table=f"{SCHEMA_NAME}.{CITY_COORDINATES_TABLE}", 
            rows=data, 
            column_names=column_headers
        )

    @task
    def get_city_coordinates(**context):
        '''
        return a list of dicts mapping column name to value for each row in city coordinates table
        '''
        ch_client = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN).get_client()

        script = read_sql_script("sql/get_deduplicated_city_coordinates.sql")

        ti = context['ti']
        rendered = ti.task.render_template(script, context)

        query_result = ch_client.query(rendered)

        columns = query_result.column_names
        rows = query_result.result_rows

        outputs = []

        for row in rows:
            outputs.append(dict(zip(columns, row)))

        return outputs
    
    '''
    TODO: add task that fetches the entire month as a json,
    generates the clickhouse rows,
    wrties to clickhouse
    '''
    @task
    def write_previous_month_data(city_coordinates: list[dict[str, str]]):
        '''
        arguments:
            city_coordinates: a list of dicts with city,country,latitude_coordinate,longitude_coordinate
        '''
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id
    
        insert_rows_with_city_data = []
        
        today = datetime.now(timezone.utc)
        first_day_this_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_time = first_day_this_month - timedelta(seconds=1)
        first_day_prev_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
        start_time = first_day_prev_month

        start_time_unix = int(start_time.timestamp())
        end_time_unix = int(end_time.timestamp())

        for city_dict in city_coordinates:
            logger.info(f"Fetching historical data from {start_time} to {end_time} for {city_dict.get("city")}, {city_dict.get("country")}")
            lat = city_dict.get("latitude_coordinate")
            lon = city_dict.get("longitude_coordinate") # TODO
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
            
        # TODO: actually insert
        if not insert_rows_with_city_data:
            logger.warning("No rows fetched — skipping insert")
        else:
            ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN)
            ch_hook.bulk_insert_rows(
            table=f"{SCHEMA_NAME}.{RAW_TABLE_COLUMNS}", 
            rows=insert_rows_with_city_data, 
            column_names=RAW_TABLE_COLUMNS
        )

    
    refresh_city_coordinates_table_ =  refresh_city_coordinates_table()
    city_coordinates = get_city_coordinates()

    create_schema_if_not_exists >> create_tables >> refresh_city_coordinates_table_ >> \
    city_coordinates

    write_previous_month_data(city_coordinates)

air_quality_historical_dag = air_quality_historical()