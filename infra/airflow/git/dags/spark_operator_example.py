'''
Sample DAG demonstrating SparkKubernetesOperator — submits a SparkApplication
CR to the cluster so the kubeflow spark-operator launches a dedicated driver +
executor pods for the job.

Compare to spark_connect_example.py: this pattern is for batch-style jobs that
benefit from per-job isolation, retries, and event-log capture. The code runs
entirely inside its own Spark pods — no shared driver.

The example runs the bundled Spark Pi job at
/opt/spark/examples/src/main/python/pi.py — present in any apache/spark image.
For real workloads, bake the job script into the custom Spark image at
/opt/spark/jobs/<name>.py and reference it as local:///opt/spark/jobs/<name>.py.

SparkKubernetesOperator is a classic operator (not a TaskFlow function) — it
has to stay an explicit operator instance. The TaskFlow @task decorator builds
the SparkApplication spec; its return value is passed as an XComArg into the
operator's template_spec, which makes the build step show as its own task in
the UI and keeps the spec inspectable in XCom.

Required:
  - apache-airflow-providers-cncf-kubernetes (bundled with KubernetesExecutor)
  - Worker pod ServiceAccount needs CRUD on sparkapplications.sparkoperator.k8s.io
  - SPARK_IMAGE env var on Airflow workers — add to the env block in __main__.py:
        env={..., 'SPARK_IMAGE': spark.image}
  - NAMESPACE env var (or a default below) — namespace where the CR is created
'''

import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)


NAMESPACE   = os.environ.get('NAMESPACE',   'ns-k8lh-dev')
SPARK_IMAGE = os.environ.get('SPARK_IMAGE', 'localhost:5000/spark:4.0.0')


@dag(
    dag_id='spark_operator_example',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example', 'spark', 'kubernetes-operator'],
)
def spark_operator_example():

    @task
    def build_spec(job_name: str) -> dict:
        '''Render the SparkApplication CR spec from worker-pod env vars.'''
        return {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind':       'SparkApplication',
            'metadata':   {'name': job_name, 'namespace': NAMESPACE},
            'spec': {
                'type':                'Python',
                'pythonVersion':       '3',
                'mode':                'cluster',
                'image':               SPARK_IMAGE,
                'imagePullPolicy':     'Always',
                'mainApplicationFile': 'local:///opt/spark/examples/src/main/python/pi.py',
                'sparkVersion':        '4.0.0',
                'restartPolicy':       {'type': 'Never'},
                'driver':   {'cores': 1, 'memory': '1g', 'serviceAccount': 'spark'},
                'executor': {'cores': 1, 'memory': '1g', 'instances': 2},
                # Event logs land in the same bucket SparkHistory reads from,
                # so this run shows up in the UI alongside Connect-driven runs.
                'sparkConf': {
                    'spark.hadoop.fs.s3a.endpoint':          os.environ.get('S3_ENDPOINT',   ''),
                    'spark.hadoop.fs.s3a.access.key':        os.environ.get('S3_ACCESS_KEY', ''),
                    'spark.hadoop.fs.s3a.secret.key':        os.environ.get('S3_SECRET_KEY', ''),
                    'spark.hadoop.fs.s3a.path.style.access': 'true',
                    'spark.hadoop.fs.s3a.impl':              'org.apache.hadoop.fs.s3a.S3AFileSystem',
                    'spark.eventLog.enabled':                'true',
                    'spark.eventLog.dir':                    's3a://spark-logs/',
                    'spark.eventLog.rolling.enabled':        'true',
                    'spark.eventLog.rolling.maxFileSize':    '64m',
                },
            },
        }

    @task
    def report(app_name: str) -> str:
        '''Trivial downstream TaskFlow task — runs after the Spark job finishes.'''
        return f'SparkApplication {app_name} completed; check the SparkHistory UI.'

    spec = build_spec('spark-pi')

    submit = SparkKubernetesOperator(
        task_id='submit_spark_pi',
        namespace=NAMESPACE,
        kubernetes_conn_id='kubernetes_default',
        # Keep the CR around after the run so the SparkHistory UI can still
        # link logs/state for it. Set True if you want auto-cleanup.
        delete_on_termination=True,
        get_logs=True,
        template_spec=spec,
    )

    submit >> report('spark-pi')


spark_operator_example()