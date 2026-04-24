'''
Backfill-then-stream DAG.

For each symbol (BTC, ETH):
  1. Read the historical Parquet dump from s3a://raw/<sym>_parquet and append
     any rows newer than the current max Timestamp in bronze.crypto.<sym>.
  2. Once the backfill is done, POST to the producer to start a Kafka stream
     from that max Timestamp onwards — Flink consumes the topic and keeps
     bronze.crypto.<sym> current from there.

Env vars used (all injected by Pulumi into the Airflow pods):
  KAFKA_BOOTSTRAP_SERVERS  "host:port" — passed verbatim as producer kafka_server
  PRODUCER_BASE_URL        http URL for the producer service
'''

import os
from datetime import datetime

import requests as r
from pyspark.sql import functions as f

from airflow.sdk import dag, task


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
PRODUCER_BASE_URL       = os.getenv('PRODUCER_BASE_URL')


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
        df = (
            spark.read.parquet('s3a://raw/btc_parquet')
                 .select('Timestamp', 'Low', 'High', 'Open', 'Close', 'Volume')
                 .where(f"Timestamp > '{max_timestamp}'")
        )
        df.writeTo('bronze.crypto.btc').append()
        return get_max_timestamp(spark, 'bronze.crypto.btc')

    @task.pyspark(conn_id='spark_default')
    def catch_up_eth_to_bronze(spark) -> str:
        max_timestamp = get_max_timestamp(spark, 'bronze.crypto.eth')
        df = (
            spark.read.parquet('s3a://raw/eth_parquet')
                 .select('Timestamp', 'Low', 'High', 'Open', 'Close', 'Volume')
                 .where(f"Timestamp > '{max_timestamp}'")
        )
        df.writeTo('bronze.crypto.eth').append()
        return get_max_timestamp(spark, 'bronze.crypto.eth')

    @task.pyspark(conn_id='spark_default')
    def start_btc_stream(spark):
        start_time = spark.sql(
            "select coalesce(max(Timestamp), '2017-01-01 00:00:00') from bronze.crypto.btc"
        ).collect()[0][0]

        response = r.post(
            f'{PRODUCER_BASE_URL}/start-stream',
            headers={'Content-Type': 'application/json'},
            json={
                'kafka_server': KAFKA_BOOTSTRAP_SERVERS,
                'kafka_topic':  'btc',
                'symbol':       'BTC-USD',
                'start_time':   start_time,
                'mode':         'async',
            },
        )
        return response.json()

    @task.pyspark(conn_id='spark_default')
    def start_eth_stream(spark):
        start_time = spark.sql(
            "select coalesce(max(Timestamp), '2017-01-01 00:00:00') from bronze.crypto.eth"
        ).collect()[0][0]

        response = r.post(
            f'{PRODUCER_BASE_URL}/start-stream',
            headers={'Content-Type': 'application/json'},
            json={
                'kafka_server': KAFKA_BOOTSTRAP_SERVERS,
                'kafka_topic':  'eth',
                'symbol':       'ETH-USD',
                'start_time':   start_time,
                'mode':         'async',
            },
        )
        return response.json()

    [catch_up_btc_to_bronze(), catch_up_eth_to_bronze()] >> [start_btc_stream(), start_eth_stream()]


catch_up_start_stream()
