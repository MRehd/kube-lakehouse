'''
Apache Spark on Kubernetes — Connect server + History Server.

Deploys two components:

  Spark Connect Server (port 15002)
    A persistent gRPC endpoint that Airflow workers connect to via the
    @task.pyspark decorator or directly with pyspark.sql.SparkSession.
    Runs as a standalone Deployment; executor pods are created on demand
    by the Spark K8s scheduler (k8s:// master) and destroyed when idle.
    Uses dynamic allocation: minExecutors=0, maxExecutors=N.

  Spark History Server (port 18080)
    A passive log reader that scans the spark-logs MinIO bucket and presents
    completed/running job history. Jobs appear here automatically because the
    Connect server writes event logs to s3a://spark-logs/.

Airflow integration:
    Pass spark.connect_server_url as a connection URI to Airflow:

        AirflowConnectionArgs(
            conn_id='spark_default',
            uri=spark.connect_server_url,
        )

Example:
    spark = Spark('spark', SparkArgs(
        namespace=ns.metadata.name,
        service_account_name=sas.provision(...).metadata.name,
        s3_endpoint=minio.endpoint,
        s3_access_key='minioadmin',
        s3_secret_key=minio_password,
        executor_instances=4,
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))
'''

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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
    '''Configuration arguments for the Spark Connect server and History Server.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Resource name prefix for K8s resources. Defaults to the Pulumi resource name.'''

    image: str = 'apache/spark'
    '''Docker image for both the Connect server and History Server.'''

    image_tag: str = '4.0.0'
    '''Image tag.'''

    service_account_name: Input[str] = 'spark'
    '''
    Name of the ServiceAccount used by the Connect server pod.
    This SA must have pod/service/configmap CRUD permissions so the K8s
    scheduler can create and manage executor pods. Create via ServiceAccounts
    and pass the output here.
    '''

    connect_master: str = 'k8s://https://kubernetes.default.svc:443'
    '''
    Spark master URL for the Connect server.
    - "k8s://https://kubernetes.default.svc:443" — spawn executor pods via K8s (default)
    - "local[*]"                                 — all computation inside the Connect pod (dev only)
    When using k8s://, executors are created in the same namespace using the same
    image and ServiceAccount. Dynamic allocation tears them down when idle.
    '''

    executor_instances: int = 4
    '''Maximum executor pods for dynamic allocation (maxExecutors). minExecutors is always 0.'''

    event_log_bucket: str = 'spark-logs'
    '''MinIO bucket where the Connect server writes Spark event logs.'''

    s3_endpoint: Input[str] = ''
    '''S3/MinIO endpoint URL. Accepts a Pulumi Output.'''

    s3_access_key: Input[str] = ''
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    ingress_enabled: bool = False
    '''Create an Ingress for the History Server UI.'''

    ingress_domain: str = ''
    '''Base domain. Creates spark.<domain> → History Server.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''


class Spark(pulumi.ComponentResource):
    '''
    Deploys a Spark Connect server and a Spark History Server on Kubernetes.

    The Connect server acts as the Spark master for interactive/programmatic
    sessions. It spawns executor pods on demand via the K8s scheduler and
    releases them when idle (dynamic allocation). Zero idle worker cost.

    The History Server is a passive read-only UI that reads event logs from
    the spark-logs MinIO bucket — no connection to the Connect server needed.

    Outputs:
        namespace           — Kubernetes namespace
        connect_server_url  — sc://<host>:<port> URI for Spark Connect clients
        history_server_url  — http://<host>:<port> URL for the History Server UI

    Example:
        spark = Spark('spark', SparkArgs(
            namespace=ns.metadata.name,
            service_account_name=spark_sa.metadata.name,
            s3_endpoint=minio.endpoint,
            s3_access_key='minioadmin',
            s3_secret_key=minio_password,
        ))
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
        cs_name      = f'{release}-connect'
        cs_app_label = {'app': cs_name}

        # Resolve all Input[str] values together before building the args list.
        # service_account_name is included because it may be a Pulumi Output
        # (e.g. sa.metadata.name) and cannot be str()-coerced inside the lambda.
        connect_args = Output.all(
            ep=args.s3_endpoint,
            key=args.s3_access_key,
            sec=args.s3_secret_key,
            ns=self._namespace,
            sa=args.service_account_name,
        ).apply(lambda v: [
            '--class', 'org.apache.spark.sql.connect.service.SparkConnectServer',
            '--master', args.connect_master,
            '--conf', 'spark.eventLog.enabled=true',
            '--conf', f'spark.eventLog.dir=s3a://{args.event_log_bucket}/',
            '--conf', f'spark.hadoop.fs.s3a.endpoint={v["ep"]}',
            '--conf', f'spark.hadoop.fs.s3a.access.key={v["key"]}',
            '--conf', f'spark.hadoop.fs.s3a.secret.key={v["sec"]}',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--conf', f'spark.connect.grpc.binding.port={CONNECT_SERVER_PORT}',
            # K8s executor scheduling
            '--conf', f'spark.kubernetes.namespace={v["ns"]}',
            '--conf', f'spark.kubernetes.authenticate.driver.serviceAccountName={v["sa"]}',
            '--conf', f'spark.kubernetes.container.image={image}',
            # Dynamic allocation: executors created on demand, released after 60 s idle
            '--conf', 'spark.dynamicAllocation.enabled=true',
            '--conf', 'spark.dynamicAllocation.shuffleTracking.enabled=true',
            '--conf', 'spark.dynamicAllocation.minExecutors=0',
            '--conf', f'spark.dynamicAllocation.maxExecutors={args.executor_instances}',
            '--conf', 'spark.dynamicAllocation.executorIdleTimeout=60s',
        ])

        cs_spec = json.loads((CONFIG_DIR / 'resources/spark_connect_spec.json').read_text())
        cs_spec['selector']['matchLabels']                    = cs_app_label
        cs_spec['template']['metadata']['labels']             = cs_app_label
        cs_spec['template']['spec']['serviceAccountName']     = args.service_account_name
        cs_spec['template']['spec']['initContainers'][0]['image'] = image
        cs_spec['template']['spec']['containers'][0]['image']     = image
        cs_spec['template']['spec']['containers'][0]['args']      = connect_args

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
        hs_name      = f'{release}-history-server'
        hs_app_label = {'app': hs_name}

        # SPARK_HISTORY_OPTS env var — built from S3 credentials
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
        hs_spec['selector']['matchLabels']                    = hs_app_label
        hs_spec['template']['metadata']['labels']             = hs_app_label
        hs_spec['template']['spec']['initContainers'][0]['image'] = image
        hs_spec['template']['spec']['containers'][0]['image']     = image
        hs_spec['template']['spec']['containers'][0]['env'][0]['value'] = spark_history_opts

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

        # ── History Server Ingress (optional) ─────────────────────────────────
        if args.ingress_enabled and args.ingress_domain:
            host = f'spark.{args.ingress_domain}'

            ingress_spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            ingress_spec['ingressClassName']                                                       = args.ingress_class_name
            ingress_spec['rules'][0]['host']                                                       = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['name']             = hs_name
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number']   = HISTORY_SERVER_PORT

            Ingress(
                f'{name}-history-server-ingress',
                metadata={
                    'name':        f'{hs_name}-ingress',
                    'namespace':   self._namespace,
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
            'namespace':          self.namespace,
            'connect_server_url': self.connect_server_url,
            'history_server_url': self.history_server_url,
        })
