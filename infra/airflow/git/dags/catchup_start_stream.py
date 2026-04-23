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

import os
import requests as r
from pyspark.sql import functions as f
from datetime import datetime
from airflow.sdk import dag, task

PRODUCER_URL = os.getenv('PRODUCER_URL')
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAFKA_PORT = os.getenv('KAFKA_PORT')
KAFKA_BTC_TOPIC = os.getenv('KAFKA_BTC_TOPIC')
KAFKA_ETH_TOPIC = os.getenv('KAFKA_ETH_TOPIC')

def get_max_timestamp(spark, table: str) -> str:
    max_timestamp = spark.read.format('iceberg').load(table).select(f.max(f.col('Timestamp'))).collect()[0][0]
    if not max_timestamp:
        max_timestamp = '2014-01-01 00:00:00'
    else:
        max_timestamp = max_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    return max_timestamp

@dag(
    dag_id='spark_connect_bulk_append',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'spark-connect', 'iceberg'],
)
def catch_up_start_stream():

    @task.pyspark(conn_id='spark_default')
    def catch_up_btc_to_bronze(spark) -> str:
        
        max_timestamp = get_max_timestamp(spark, 'bronze.crypto.btc')
        df = spark.read.parquet('s3a://raw/btc_parquet').select('Timestamp', 'Low', 'High', 'Open', 'Close', 'Volume').where(f"Timestamp > '{max_timestamp}'")
        df.writeTo('bronze.crypto.btc').append()

        return get_max_timestamp(spark, 'bronze.crypto.btc')
    
    @task.pyspark(conn_id='spark_default')
    def catch_up_eth_to_bronze(spark) -> str:

        max_timestamp = get_max_timestamp(spark, 'bronze.crypto.eth')
        df = spark.read.parquet('s3a://raw/eth_parquet').select('Timestamp', 'Low', 'High', 'Open', 'Close', 'Volume').where(f"Timestamp > '{max_timestamp}'")
        df.writeTo('bronze.crypto.eth').append()

        return get_max_timestamp(spark, 'bronze.crypto.eth')

    @task.pyspark(conn_id='spark_default')
    def start_btc_stream(spark):

        start_time = spark.sql("select coalesce(max(Timestamp), '2017-01-01 00:00:00') from bronze.crypto.btc").collect()[0][0]

        headers = {
        'Content-Type': 'application/json'
        }

        data = {
            'kafka_server': f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
            'kafka_topic': os.getenv('KAFKA_BTC_TOPIC'),
            'symbol': 'BTC-USD',
            'start_time': start_time,
            'mode': 'async'
        }

        producer_url = f"{os.getenv('PRODUCER_URL')}/start-stream"
        response = r.post(producer_url, headers=headers, json=data)

        return response.json()
    
    @task.pyspark(conn_id='spark_default')
    def start_eth_stream(spark):

        start_time = spark.sql("select coalesce(max(Timestamp), '2017-01-01 00:00:00') from bronze.crypto.eth").collect()[0][0]

        headers = {
        'Content-Type': 'application/json'
        }

        data = {
            'kafka_server': f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
            'kafka_topic': os.getenv('KAFKA_ETH_TOPIC'),
            'symbol': 'ETH-USD',
            'start_time': start_time,
            'mode': 'async'
        }

        producer_url = f"{os.getenv('PRODUCER_URL')}/start-stream"
        response = r.post(producer_url, headers=headers, json=data)

        return response.json()
    

    [catch_up_btc_to_bronze(), catch_up_eth_to_bronze()] >> [start_btc_stream(), start_eth_stream()]


catch_up_start_stream()
