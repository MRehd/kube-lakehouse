'''
Monte Carlo Pi estimation — demonstrates a self-contained batch job submitted
to the kubeflow spark-operator via Airflow's SparkKubernetesOperator.

Referenced from the SparkApplication spec as:
    mainApplicationFile: s3a://spark-jobs/pi.py

The driver pod fetches this file at startup using the hadoop-aws S3A
filesystem baked into the custom Spark image.
'''

import sys
from operator import add
from random import random

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName('PythonPi').getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100_000 * partitions

    def sample(_: int) -> int:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x * x + y * y <= 1 else 0

    hits = spark.sparkContext.parallelize(range(n), partitions).map(sample).reduce(add)
    print(f'Pi is roughly {4.0 * hits / n}')

    spark.stop()


if __name__ == '__main__':
    main()
