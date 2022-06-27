import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    "LocaslIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.parquet"

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        # the downloaded file is not saved on the default directory, because if
        # we do that the file would be removed after the task finishs running 
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'

    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user="root",
            password="root",
            host="pgdatabase",
            port="5432",
            db="ny_taxi",
            table_name=TABLE_NAME_TEMPLATE,
            file_name=OUTPUT_FILE_TEMPLATE
        )
    )

    wget_task >> ingest_task
