import json
import os
import pandas as pd
import pyarrow as pa
import requests
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

OPERATION_TYPES = {'exports': 1, 'imports': 2}
EN_COLUMNS = [
    'year', 'month', 'destination_country', 'trade_bloc', 'origin_city', 'origin_state',
    'section_cd', 'section_ds', 'fob', 'net_weight'
]
TABLE_SCHEMA = pa.schema(
    [
        ('year',pa.int32()),
        ('month',pa.int32()),
        ('destination_country',pa.string()),
        ('origin_city',pa.string()),
        ('origin_state',pa.string()),
        ('section_cd',pa.string()),
        ('section_ds',pa.string()),
        ('fob',pa.int64()),
        ('net_weight',pa.int64()),
    ]
)

def extract_dataset(operation_type, operation_type_id, parquet_file,
    year, month, month_name):
    query_parameters = {
        "yearStart": str(year),
        "yearEnd": str(year),
        "typeForm": 1,
        "typeOrder": operation_type_id,
        "filterList": [],
        "filterArray": [],
        "rangeFilter": [],
        "detailDatabase": [
            {
                "id": "noPaisen",
                "text": "Country"
            },
            {
                "id": "noBlocoen",
                "text": "Economic Block"
            },
            {
                "id": "noMunMin",
                "text": "City",
                "concat": "noMunMinsgUf"
            },
            {
                "id": "noUf",
                "text": "State of company"
            },
            {
                "id": "noSecen",
                "text": "Section",
                "parentId": "coNcmSecrom",
                "parent": "Section Code"
            }
        ],
        "monthDetail": True,
        "metricFOB": True,
        "metricKG": True,
        "monthStart": "01",
        "monthEnd": "01",
        "formQueue": "city",
        "langDefault": "en",
        "monthStartName": month_name,
        "monthEndName": month_name
    }
    query_parameters = json.dumps(query_parameters)
    url = f'http://api.comexstat.mdic.gov.br/cities?filter={query_parameters}'
    payload = ""
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'Origin': 'http://comexstat.mdic.gov.br',
        'Referer': 'http://comexstat.mdic.gov.br/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }
    start = time.perf_counter()
    response = requests.request("GET", url, headers=headers, data=payload)
    request_time = time.perf_counter() - start
    if (response.ok):
        print(f"Request {year}-{month} data completed in {request_time:.2f}ms")
        response_data = response.json()
    else:
        response.raise_for_status()
    df = pd.DataFrame.from_dict(response_data["data"]["list"])
    df.columns = EN_COLUMNS
    df = df.astype({"year": int, "month": int, "fob": int, "net_weight": int})
    df.to_parquet(f"{PATH_TO_LOCAL_HOME}/{parquet_file}", schema=TABLE_SCHEMA)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


with DAG(
    dag_id="download_upload_to_gcs_dag",
    schedule_interval="@monthly",
    start_date=datetime(1997, 1, 1),
    end_date=datetime(2022, 12, 1),
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:
    for operation_type, operation_type_id in OPERATION_TYPES.items():
        year = "{{ execution_date.strftime('%Y') }}"
        month_name = "{{ execution_date.strftime('%B') }}"
        month = "{{ execution_date.strftime('%m') }}"
        parquet_file = f"{operation_type}_{year}-{month}.parquet"
        gcs_path_template = f"raw/{operation_type}/" + "{{ execution_date.strftime('%Y') }}/" + parquet_file

        extract_dataset_task = PythonOperator(
            task_id=f"{operation_type}_extract_dataset_task",
            python_callable=extract_dataset,
            op_kwargs={
                "operation_type": operation_type,
                "operation_type_id": operation_type_id,
                "parquet_file": parquet_file,
                "year": year,
                "month_name": month_name,
                "month": month
            }
        )
        
        local_to_gcs_task = PythonOperator(
            task_id=f"{operation_type}_local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,   
                "object_name": gcs_path_template,
                "local_file": f"{PATH_TO_LOCAL_HOME}/{parquet_file}",
            },
            trigger_rule='one_success'
        )

        delete_downloaded_files_task = BashOperator(
            task_id=f"{operation_type}_delete_downloaded_files_task",
            bash_command=f"rm {PATH_TO_LOCAL_HOME}/{parquet_file}"
        )

        extract_dataset_task >> local_to_gcs_task >> delete_downloaded_files_task