import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage
from datetime import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

DATASET_FILE = "fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
DATASET_URL = f"https://nyc-tlc.s3.amazonaws.com/trip+data/{DATASET_FILE}"
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
BIGQUERY_TABLE_NAME_TEMPLATE = "fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}"


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_fhv_homework",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 1),
    max_active_runs=2,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # -s: silent, -S: show error, -l: list-only, -f: fail
        # curl Ref: https://curl.se/docs/manpage.html
        bash_command=f"echo {DATASET_FILE} & curl -sSLf {DATASET_URL} > {PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{DATASET_FILE}",
            "local_file": f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": BIGQUERY_TABLE_NAME_TEMPLATE,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{DATASET_FILE}"],
            },
        },
    )

    delete_downloaded_files_task = BashOperator(
        task_id="delete_downloaded_files_task",
        bash_command=f"rm {PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
    )

    download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task >> delete_downloaded_files_task
