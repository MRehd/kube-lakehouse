'''
Sample DAG demonstrating the @task.pyspark decorator against Spark Connect.

The task runs inside an Airflow worker pod and connects remotely to the Spark
Connect server over gRPC — no spark-submit, no driver pod, no local JVM.

The `spark_default` connection is auto-registered by Pulumi with URI scheme
spark-connect:// so Airflow classifies it as conn_type=spark_connect; the
decorator then builds the SparkSession via SparkConnectHook and injects
`spark` / `sc` as arguments.
'''

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id='spark_connect_example',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example', 'spark', 'spark-connect'],
)
def spark_connect_example():

    @task.pyspark(conn_id='spark_default')
    def compute_summary(spark):
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
        return summary.toPandas()

    compute_summary()


spark_connect_example()
