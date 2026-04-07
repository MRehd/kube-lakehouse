'''Apache Spark on Kubernetes — Connect server + History Server.'''

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import json
import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apps.v1 import Deployment
from pulumi_kubernetes.core.v1 import Service
from pulumi_kubernetes.networking.v1 import Ingress

CONFIG_DIR = Path(__file__).parent.parent / 'config'

CONNECT_SERVER_PORT = 15002
HISTORY_SERVER_PORT = 18080


@dataclass
class SparkArgs:
    namespace: Input[str] = 'default'
    release_name: Optional[str] = None

    image: str = 'apache/spark'
    image_tag: str = '4.0.0'

    service_account_name: Input[str] = 'spark'
    '''
    Name of the ServiceAccount used by the Connect server pod.
    Create via the ServiceAccounts module and pass the output here.
    '''

    # Connect Server
    connect_master: str = 'k8s://https://kubernetes.default.svc:443'
    '''
    Spark master URL for the Connect server.
    - "k8s://https://kubernetes.default.svc:443"    — spawn executor pods via K8s native scheduler (default)
    - "local[*]"                                    — all computation inside the Connect server pod (dev only)
    When using k8s://, executor pods are created in the same namespace using the same image and
    service account. The spark ServiceAccount must have pod/service/configmap CRUD (already set).
    '''
    executor_instances: int = 1
    '''Number of executor pods when connect_master uses k8s://. Ignored for local[*].'''

    # History Server
    event_log_bucket: str = 'spark-logs'
    '''MinIO bucket where the Connect server writes event logs.'''

    s3_endpoint: Input[str] = ''
    s3_access_key: Input[str] = ''
    s3_secret_key: Input[str] = ''

    ingress_enabled: bool = False
    ingress_domain: str = ''
    ingress_class_name: str = 'nginx'


class Spark(pulumi.ComponentResource):
    '''
    Deploys a Spark Connect server and a Spark History Server on Kubernetes.

    Spark Connect server:
        A persistent gRPC endpoint (port 15002) that Airflow workers connect to
        via the @task.pyspark decorator. The decorator injects a remote SparkSession
        into the decorated function — no spark-submit, no custom Docker images needed
        for jobs.

        Airflow integration:
            1. Install apache-airflow-providers-apache-spark in Airflow.
            2. Create a Spark connection in Airflow:
                   conn_id   = "spark_default"
                   conn_type = "spark"
                   host      = "sc://<connect-svc>.<namespace>.svc.cluster.local"
                   port      = 15002
            3. Use the decorator in your DAG:

               @task.pyspark(conn_id="spark_default")
               def my_transform(spark: SparkSession) -> None:
                   df = spark.read.parquet("s3a://bronze/...")
                   df.write.format("iceberg").save("silver.my_table")

        The Connect server is pre-configured with S3A (MinIO) and event logging.
        To access Iceberg/Polaris catalogs, pass the catalog conf via the SparkSession
        builder inside the task, or extend extra_spark_conf via SparkArgs.

    History Server:
        A passive log reader that scans the spark-logs MinIO bucket and presents
        completed/running job history at spark.<domain>. It has no connection to
        the Connect server — jobs appear here automatically because the Connect server
        writes event logs to s3a://spark-logs/.
    '''

    def __init__(
        self,
        name: str,
        args: SparkArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:spark:Spark', name, {}, opts)

        args = args or SparkArgs()
        self._namespace = Output.from_input(args.namespace)
        release = args.release_name or name
        image = f'{args.image}:{args.image_tag}'

        # ── Spark Connect Server ───────────────────────────────────────────────

        cs_name = f'{release}-connect'
        cs_app_label = {'app': cs_name}

        connect_args = Output.all(
            ep=args.s3_endpoint,
            key=args.s3_access_key,
            sec=args.s3_secret_key,
            ns=self._namespace,
        ).apply(lambda v: [
            '--class', 'org.apache.spark.sql.connect.service.SparkConnectServer',
            '--master', args.connect_master,
            '--conf', f'spark.eventLog.enabled=true',
            '--conf', f'spark.eventLog.dir=s3a://{args.event_log_bucket}/',
            '--conf', f'spark.hadoop.fs.s3a.endpoint={v["ep"]}',
            '--conf', f'spark.hadoop.fs.s3a.access.key={v["key"]}',
            '--conf', f'spark.hadoop.fs.s3a.secret.key={v["sec"]}',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--conf', f'spark.connect.grpc.binding.port={CONNECT_SERVER_PORT}',
            # K8s executor scheduling — only meaningful when connect_master is k8s://
            '--conf', f'spark.kubernetes.namespace={v["ns"]}',
            '--conf', f'spark.kubernetes.authenticate.driver.serviceAccountName={args.service_account_name}',
            '--conf', f'spark.kubernetes.container.image={image}',
            '--conf', f'spark.executor.instances={args.executor_instances}',
        ])

        cs_spec = json.loads((CONFIG_DIR / 'resources/spark_connect_spec.json').read_text())
        cs_spec['selector']['matchLabels']                               = cs_app_label
        cs_spec['template']['metadata']['labels']                        = cs_app_label
        cs_spec['template']['spec']['serviceAccountName']                = args.service_account_name
        cs_spec['template']['spec']['initContainers'][0]['image']        = image
        cs_spec['template']['spec']['containers'][0]['image']            = image
        cs_spec['template']['spec']['containers'][0]['args']             = connect_args

        Deployment(
            f'{name}-connect-server',
            metadata={'name': cs_name, 'namespace': self._namespace},
            spec=cs_spec,
            opts=pulumi.ResourceOptions(parent=self),
        )

        Service(
            f'{name}-connect-server-svc',
            metadata={'name': cs_name, 'namespace': self._namespace},
            spec={
                'selector': cs_app_label,
                'ports': [
                    {'name': 'grpc', 'port': CONNECT_SERVER_PORT, 'targetPort': CONNECT_SERVER_PORT},
                    {'name': 'ui',   'port': 4040,                'targetPort': 4040},
                ],
            },
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.connect_server_url = Output.concat(
            'sc://', cs_name, '.', self._namespace,
            '.svc.cluster.local:', str(CONNECT_SERVER_PORT),
        )

        # ── History Server ────────────────────────────────────────────────────

        hs_name = f'{release}-history-server'
        hs_app_label = {'app': hs_name}

        spark_history_opts = Output.all(
            ep=args.s3_endpoint,
            key=args.s3_access_key,
            sec=args.s3_secret_key,
        ).apply(lambda v: (
            f'-Dspark.history.fs.logDirectory=s3a://{args.event_log_bucket}/ '
            f'-Dspark.hadoop.fs.s3a.endpoint={v["ep"]} '
            f'-Dspark.hadoop.fs.s3a.access.key={v["key"]} '
            f'-Dspark.hadoop.fs.s3a.secret.key={v["sec"]} '
            f'-Dspark.hadoop.fs.s3a.path.style.access=true '
            f'-Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'
        ))

        hs_spec = json.loads((CONFIG_DIR / 'resources/history_server_spec.json').read_text())
        hs_spec['selector']['matchLabels']                               = hs_app_label
        hs_spec['template']['metadata']['labels']                        = hs_app_label
        hs_spec['template']['spec']['initContainers'][0]['image']        = image
        hs_spec['template']['spec']['containers'][0]['image']            = image
        hs_spec['template']['spec']['containers'][0]['env'][0]['value']  = spark_history_opts

        Deployment(
            f'{name}-history-server',
            metadata={'name': hs_name, 'namespace': self._namespace},
            spec=hs_spec,
            opts=pulumi.ResourceOptions(parent=self),
        )

        Service(
            f'{name}-history-server-svc',
            metadata={'name': hs_name, 'namespace': self._namespace},
            spec={
                'selector': hs_app_label,
                'ports': [{'port': HISTORY_SERVER_PORT, 'targetPort': HISTORY_SERVER_PORT}],
            },
            opts=pulumi.ResourceOptions(parent=self),
        )

        if args.ingress_enabled and args.ingress_domain:
            host = f'spark.{args.ingress_domain}'

            ingress_spec = json.loads(
                (CONFIG_DIR / 'resources/ingress_spec.json').read_text()
            )
            ingress_spec['ingressClassName'] = args.ingress_class_name
            ingress_spec['rules'][0]['host'] = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend'] = {
                'service': {
                    'name': hs_name,
                    'port': {'number': HISTORY_SERVER_PORT},
                },
            }

            Ingress(
                f'{name}-history-server-ingress',
                metadata={
                    'name': f'{hs_name}-ingress',
                    'namespace': self._namespace,
                    'annotations': {'kubernetes.io/ingress.class': args.ingress_class_name},
                },
                spec=ingress_spec,
                opts=pulumi.ResourceOptions(parent=self),
            )
            self.history_server_url = Output.from_input(f'http://{host}')
        else:
            self.history_server_url = Output.concat(
                'http://', hs_name, '.', self._namespace,
                '.svc.cluster.local:', str(HISTORY_SERVER_PORT),
            )

        self.namespace = self._namespace
        self.register_outputs({
            'namespace': self.namespace,
            'connect_server_url': self.connect_server_url,
            'history_server_url': self.history_server_url,
        })
