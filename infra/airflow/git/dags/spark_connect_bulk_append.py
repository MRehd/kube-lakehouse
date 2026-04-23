'''
Sample DAG demonstrating a Spark Connect task against the live infrastructure:
reads Parquet files from MinIO at s3a://bronze/btc_parquet and appends them
to the bronze.crypto.btc Iceberg table registered in the Polaris REST catalog.

No per-task configuration is needed — the Spark Connect server already has:
  - the `bronze` Iceberg catalog pre-registered via spark-defaults.conf
  - S3A filesystem + creds + hadoop-aws baked into the image

Returned XCom is the row count appended, so you can spot-check from the task's
XComs view.
'''

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id='spark_connect_bulk_append',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'spark-connect', 'iceberg'],
)
def spark_bulk_append():

    @task.pyspark(conn_id='spark_default')
    def btc_to_bronze(spark) -> int:
        df = spark.read.parquet('s3a://raw/btc_parquet')
        df.writeTo('bronze.crypto.btc').append()
        return df.count()
    
    @task.pyspark(conn_id='spark_default')
    def eth_to_bronze(spark) -> int:
        df = spark.read.parquet('s3a://raw/eth_parquet')
        df.writeTo('bronze.crypto.eth').append()
        return df.count()

    [btc_to_bronze(), eth_to_bronze()]


spark_bulk_append()
