'''
plan:
    get json data from API monthly for the past five years
    stage the json file in an appropriately named folder in minio. I think historical/year/{month}.json
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

CLICKHOUSE_CONN = "clickhouse_conn"
MINIO_CONN = "minio_conn"
INCLUDE_PATH = '/opt/airflow/include'

SCHEMA_NAME = 'air_quality_datasets'

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
        "schema_name": SCHEMA_NAME
        }
}

@dag(
    dag_id="air_quality_historical",
    start_date=datetime(2021, 1, 1),
    schedule="@monthly",
    template_searchpath=[INCLUDE_PATH],
    default_args=default_args,
    catchup=True,
    doc_md=DOC_MD,
    max_active_runs=1
)
def air_quality_historical():

    create_schema_if_not_exists = SQLExecuteQueryOperator(
        task_id='create_schema_if_not_exists',
        sql = f"sql/create_database.sql",
        conn_id=CLICKHOUSE_CONN,
    )

air_quality_historical_dag = air_quality_historical()