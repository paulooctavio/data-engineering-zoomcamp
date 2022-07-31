import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
    )
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASETS = {'yellow': 'tpep_pickup_datetime', 'green': 'tpep_pickup_datetime','fhv':'pickup_datetime'}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for dataset, partition_col in DATASETS.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f"{dataset}_ds_move_files_gcs_task",
            source_bucket=BUCKET,
            source_object=f'raw/{dataset}_tripdata*.parquet',
            destination_bucket=BUCKET,
            destination_object=f"{dataset}",
            move_object=True,
        )

        gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f"{dataset}_ds_gcs_to_bq_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{dataset}_tripdata_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{dataset}/*"],
                },
            },
        )

        CREATE_PARTITION_TABLE_QUERY = f"""
        CREATE OR REPLACE TABLE `{BIGQUERY_DATASET}.{dataset}_tripdata_partitioned`
        PARTITION BY
            DATE({partition_col}) AS
        SELECT
            *
        FROM
            `{BIGQUERY_DATASET}.{dataset}_tripdata_external_table`;
        """

        bq_ext_to_part_task = BigQueryInsertJobOperator(
            task_id=f"{dataset}_ds_bq_ext_to_part_task",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_TABLE_QUERY,
                    "useLegacySql": False,
                }
            },
        )

        move_files_gcs_task >> gcs_to_bq_ext_task >> bq_ext_to_part_task
