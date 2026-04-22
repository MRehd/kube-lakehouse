'''
Sample DAG demonstrating the @task.pyspark decorator against Spark Connect.

The task runs inside an Airflow worker pod and connects remotely to the Spark
Connect server (port 15002) over gRPC — no spark-submit, no driver pod.

The `spark_default` connection is auto-registered from spark.connect_server_url
(sc://<release>-connect.<namespace>.svc.cluster.local:15002) via the
AIRFLOW_CONN_SPARK_DEFAULT env var in the Airflow Helm values.

Requires `apache-airflow-providers-apache-spark` and `pyspark` in pip_packages.
'''

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id='spark_connect_example',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example', 'spark'],
)
def spark_connect_example():

    @task.pyspark(conn_id='spark_default')
    def compute_summary(spark, sc):
        rows = [
            ('BTC', 'buy',  65000.0,  0.10),
            ('BTC', 'sell', 66500.0,  0.05),
            ('ETH', 'buy',   3200.0,  2.00),
            ('ETH', 'buy',   3250.0,  1.50),
            ('ETH', 'sell',  3400.0,  0.75),
        ]
        df = spark.createDataFrame(rows, ['symbol', 'action', 'price', 'qty'])
        df = df.withColumn('notional', df.price * df.qty)

        summary = (
            df.groupBy('symbol', 'action')
              .sum('notional')
              .withColumnRenamed('sum(notional)', 'total_notional')
              .orderBy('symbol', 'action')
        )
        return summary

    compute_summary()


spark_connect_example()
