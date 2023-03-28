import pyspark

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

credentials_location = '~/.google/credentials/google_credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

def test(test_arg):
    df = spark.read.parquet('gs://dtc_data_lake_dtc-de-352419/pq/green/*/*')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="batch_processing_dag",
    schedule_interval="@daily",
    default_args=default_args,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:
    test_task = PythonOperator(
        task_id=f"test_task",
        python_callable=test,
        op_kwargs={
            "test_arg": "test_arg",
        }
    )

    test_task