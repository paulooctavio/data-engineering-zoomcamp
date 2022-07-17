import logging
import os
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from google.cloud import storage
from datetime import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file, parquet_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, parquet_file)
    print(f"{src_file} sucessfully formated to {parquet_file}")


def set_table_schema(src_file, table_schema):
    df = pd.read_parquet(src_file)
    df.to_parquet(src_file, schema=table_schema)
    print(f"Successfully defined table schema")


def upload_to_gcs(ti, bucket, object_name, local_file):
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
    
    fetched_command = ti.xcom_pull(key='return_value', task_ids=['download_dataset_task'])
    print(f'Downloaded dataset: {fetched_command}')


def download_upload_to_gcs(
    dag,
    dataset_file_template,
    dataset_url_template,
    bigquery_table_name_template,
    gcs_path_template,
    table_schema=None,
    parquetize=False
):
    with dag:

        if parquetize:
            parquet_file = dataset_file_template.replace('.csv', '.parquet')
        else:
            parquet_file = dataset_file_template

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            # -s: silent, -S: show error, -l: list-only, -f: fail
            # curl Ref: https://curl.se/docs/manpage.html
            bash_command=f"echo {dataset_file_template} & curl -sSLf {dataset_url_template} > {PATH_TO_LOCAL_HOME}/{dataset_file_template}",
            do_xcom_push=True
        )

        branch = BranchPythonOperator(
            task_id="branch",
            python_callable=lambda parquetize: "format_to_parquet_task" if parquetize == True else "set_table_schema_task",
            op_kwargs={"parquetize": parquetize},
            dag=dag)

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{PATH_TO_LOCAL_HOME}/{dataset_file_template}",
                "parquet_file": parquet_file
            },
        )

        set_table_schema_task = PythonOperator(
            task_id="set_table_schema_task",
            python_callable=set_table_schema,
            op_kwargs={
                "src_file": f"{PATH_TO_LOCAL_HOME}/{parquet_file}",
                "table_schema": table_schema
            },
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        # XComs ( “cross-communications”) are a mechanism that let Tasks talk to each other
        # Ref: https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,   
                "object_name": gcs_path_template,
                "local_file": f"{PATH_TO_LOCAL_HOME}/{parquet_file}",
            },
            trigger_rule='one_success'
        )

        delete_downloaded_files_task = BashOperator(
            task_id="delete_downloaded_files_task",
            bash_command=f"rm {PATH_TO_LOCAL_HOME}/{parquet_file}"
        )

        download_dataset_task >> branch >> format_to_parquet_task >> local_to_gcs_task >> delete_downloaded_files_task
        download_dataset_task >> branch >> set_table_schema_task >> local_to_gcs_task >> delete_downloaded_files_task


yellow_tripdata_dag = DAG(
    dag_id="yellow_tripdata_dag",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 1),
    max_active_runs=3,
    tags=['dtc-de'],
)

green_tripdata_dag = DAG(
    dag_id="green_tripdata_dag",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 1),
    max_active_runs=3,
    tags=['dtc-de'],
)

fhv_data_dag = DAG(
    dag_id="fhv_data_dag",
    schedule_interval="@monthly",
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 12, 1),
    max_active_runs=3,
    tags=['dtc-de'],
)

zones_data_dag = DAG(
    dag_id="zones_data_dag",
    schedule_interval='@once',
    start_date= days_ago(1),
    tags=['dtc-de'],
)

YELLOW_TRIPDATA_DATASET_FILE_TEMPLATE = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
YELLOW_TRIPDATA_DATASET_URL_TEMPLATE = f"https://s3.amazonaws.com/nyc-tlc/trip+data/" + YELLOW_TRIPDATA_DATASET_FILE_TEMPLATE
YELLOW_TRIPDATA_BIGQUERY_TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y-%m') }}"
YELLOW_TRIPDATA_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime('%Y') }}/" + YELLOW_TRIPDATA_DATASET_FILE_TEMPLATE
YELLOW_TABLE_SCHEMA = pa.schema(
    [
        ('VendorID',pa.int64()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

download_upload_to_gcs(
    dag=yellow_tripdata_dag,
    dataset_file_template=YELLOW_TRIPDATA_DATASET_FILE_TEMPLATE,
    dataset_url_template=YELLOW_TRIPDATA_DATASET_URL_TEMPLATE,
    bigquery_table_name_template=YELLOW_TRIPDATA_BIGQUERY_TABLE_NAME_TEMPLATE,
    gcs_path_template=YELLOW_TRIPDATA_GCS_PATH_TEMPLATE,
    table_schema=YELLOW_TABLE_SCHEMA
)

GREEN_TRIPDATA_DATASET_FILE_TEMPLATE = "green_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
GREEN_TRIPDATA_DATASET_URL_TEMPLATE = f"https://d37ci6vzurychx.cloudfront.net/trip-data/" + GREEN_TRIPDATA_DATASET_FILE_TEMPLATE
GREEN_TRIPDATA_BIGQUERY_TABLE_NAME_TEMPLATE = "green_taxi_{{ execution_date.strftime('%Y-%m') }}"
GREEN_TRIPDATA_GCS_PATH_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime('%Y') }}/" + GREEN_TRIPDATA_DATASET_FILE_TEMPLATE
GREEN_TABLE_SCHEMA = pa.schema(
    [
        ('VendorID',pa.int64()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

download_upload_to_gcs(
    dag=green_tripdata_dag,
    dataset_file_template=GREEN_TRIPDATA_DATASET_FILE_TEMPLATE,
    dataset_url_template=GREEN_TRIPDATA_DATASET_URL_TEMPLATE,
    bigquery_table_name_template=GREEN_TRIPDATA_BIGQUERY_TABLE_NAME_TEMPLATE,
    gcs_path_template=GREEN_TRIPDATA_GCS_PATH_TEMPLATE,
    table_schema=GREEN_TABLE_SCHEMA
)

FHV_DATASET_FILE_TEMPLATE = "fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
FHV_DATASET_URL_TEMPLATE = f"https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/fhv/" + FHV_DATASET_FILE_TEMPLATE
FHV_BIGQUERY_TABLE_NAME_TEMPLATE = "fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}"
FHV_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime('%Y') }}/" + FHV_DATASET_FILE_TEMPLATE
FHV_TABLE_SCHEMA = pa.schema(
    [
        ('dispatching_base_num',pa.string()),
        ('pickup_datetime',pa.timestamp('s')),
        ('dropOff_datetime',pa.timestamp('s')),
        ('PUlocationID',pa.int64()),
        ('DOlocationID',pa.int64()),
        ('SR_Flag',pa.int64()),
        ('Affiliated_base_number',pa.string()),
    ]
)

download_upload_to_gcs(
    dag=fhv_data_dag,
    dataset_file_template=FHV_DATASET_FILE_TEMPLATE,
    dataset_url_template=FHV_DATASET_URL_TEMPLATE,
    bigquery_table_name_template=FHV_BIGQUERY_TABLE_NAME_TEMPLATE,
    gcs_path_template=FHV_GCS_PATH_TEMPLATE,
    table_schema=FHV_TABLE_SCHEMA
)

ZONES_DATASET_FILE_TEMPLATE = "taxi_zone_lookup.csv"
ZONES_DATASET_URL_TEMPLATE = f"https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
ZONES_BIGQUERY_TABLE_NAME_TEMPLATE = "taxi_zone_lookup"
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/" + ZONES_DATASET_FILE_TEMPLATE.replace(".csv", ".parquet")

download_upload_to_gcs(
    dag=zones_data_dag,
    dataset_file_template=ZONES_DATASET_FILE_TEMPLATE,
    dataset_url_template=ZONES_DATASET_URL_TEMPLATE,
    bigquery_table_name_template=ZONES_BIGQUERY_TABLE_NAME_TEMPLATE,
    gcs_path_template=ZONES_GCS_PATH_TEMPLATE,
    parquetize=True
)
