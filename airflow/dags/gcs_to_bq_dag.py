import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)


PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')

DATASETS = ['exports', 'imports']
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'comex_stat')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for dataset in DATASETS:
        gcs_to_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f"{dataset}_ds_gcs_to_bq_ext_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{dataset}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{dataset}/*"],
                },
            },
        )

        CREATE_PARTITION_TABLE_QUERY = f"""
            CREATE OR REPLACE TABLE `{BIGQUERY_DATASET}.{dataset}_partitioned`
            PARTITION BY
                trade_date AS
            SELECT
                *
                ,PARSE_DATE('%Y%m%d', CAST(year * 10000 + month * 100 + 1 AS STRING)) as trade_date
            FROM
                `{BIGQUERY_DATASET}.{dataset}_external_table`;
        """

        bq_ext_to_part_task = BigQueryInsertJobOperator(
            task_id=f"{dataset}_insert_query_job",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_TABLE_QUERY,
                    "useLegacySql": False,
                }
            },
        )
        gcs_to_bq_ext_task >> bq_ext_to_part_task
