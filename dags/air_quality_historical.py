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
    )
from airflow.decorators import (
    task_group, 
    task, 
    dag,
    )
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

CLICKHOUSE_CONN = "clickhouse_conn"
MINIO_CONN = "minio_conn"
INCLUDE_PATH = '/opt/airflow/include'

SCHEMA_NAME = 'air_quality_datasets'
RAW_TABLE_NAME = 'raw'
CITY_COORDINATES_TABLE = 'city_coordinates'

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
    "params":{
        "schema_name": SCHEMA_NAME,
        "raw_table_name": RAW_TABLE_NAME,
        "city_coordinates_table_name": CITY_COORDINATES_TABLE,
        }
}

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

    # refresh_city_coordinates_table = SQLExecuteQueryOperator(
    #     task_id='refresh_city_coordinates_table',
    #     sql = f"sql/load_city_coordinates.sql",
    #     split_statements=True,
    #     conn_id=CLICKHOUSE_CONN,
    # )

    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        sql = f"sql/create_raw_data_table.sql",
        split_statements=True,
        conn_id=CLICKHOUSE_CONN,
    )

    @task
    def load_city_coordinates():
        import csv

        hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN)

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
        
        hook.bulk_insert_rows(
            table=f"{SCHEMA_NAME}.{CITY_COORDINATES_TABLE}", 
            rows=data, 
            column_names=column_headers
        )

    load_city_coordinates_ =  load_city_coordinates()

    create_schema_if_not_exists >> create_tables >> load_city_coordinates_

air_quality_historical_dag = air_quality_historical()