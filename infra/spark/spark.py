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
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apps.v1 import Deployment
from pulumi_kubernetes.core.v1 import Service
from pulumi_kubernetes.networking.v1 import Ingress

CONFIG_DIR = Path(__file__).parent.parent / 'config'

CONNECT_SERVER_PORT = 15002
HISTORY_SERVER_PORT = 18080


@dataclass
class SparkIcebergCatalogArgs:
    '''An Iceberg REST catalog (via Polaris) to register in the Spark Connect server.'''

    name: str
    '''Catalog name as it appears in Spark SQL (e.g. "bronze").'''

    polaris_endpoint: Input[str]
    '''Base URL of the Polaris service. Accepts a Pulumi Output.'''

    warehouse: str
    '''Catalog name in Polaris used as the Iceberg warehouse.'''

    credentials_secret: str
    '''
    K8s Secret name containing CLIENT_ID and CLIENT_SECRET for this catalog's
    Polaris principal. Mounted as env vars with a per-catalog prefix
    POLARIS_<CATALOG>_ on the Connect server pod.
    '''

    s3_path_style_access: bool = True
    '''Use path-style S3 access (required for MinIO).'''


@dataclass
class SparkArgs:
    '''Configuration arguments for the Spark Connect server and History Server.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Resource name prefix for K8s resources. Defaults to the Pulumi resource name.'''

    image: Input[str] = 'apache/spark'
    '''Docker image for both the Connect server and History Server.'''

    image_tag: Input[str] = '4.0.0'
    '''Image tag.'''

    service_account_name: Input[str] = 'spark'
    '''
    Name of the ServiceAccount used by the Connect server pod.
    This SA must have pod/service/configmap CRUD permissions so the K8s
    scheduler can create and manage executor pods. Create via ServiceAccounts
    and pass the output here.
    '''

    connect_master: Input[str] = 'k8s://https://kubernetes.default.svc:443'
    '''
    Spark master URL for the Connect server.
    - "k8s://https://kubernetes.default.svc:443" — spawn executor pods via K8s (default)
    - "local[*]"                                 — all computation inside the Connect pod (dev only)
    When using k8s://, executors are created in the same namespace using the same
    image and ServiceAccount. Dynamic allocation tears them down when idle.
    '''

    executor_instances: int = 4
    '''Maximum executor pods for dynamic allocation (maxExecutors). minExecutors is always 0.'''

    event_log_bucket: Input[str] = 'spark-logs'
    '''MinIO bucket where the Connect server writes Spark event logs.'''

    s3_endpoint: Input[str] = ''
    '''S3/MinIO endpoint URL. Accepts a Pulumi Output.'''

    s3_access_key: Input[str] = ''
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    s3_region: Input[str] = 'us-east-1'
    '''S3/MinIO region.'''

    ingress_enabled: bool = False
    '''Create an Ingress for the History Server UI.'''

    ingress_domain: Input[str] = ''
    '''Base domain. Creates spark.<domain> → History Server.'''

    ingress_class_name: Input[str] = 'nginx'
    '''Ingress class name.'''

    iceberg_catalogs: List[SparkIcebergCatalogArgs] = field(default_factory=list)
    '''
    Iceberg REST catalogs to register in the Spark Connect server.
    Configuration is injected via spark-defaults.conf generated at pod startup.
    Credentials are read from K8s Secrets mounted as env vars — never embedded
    in the pod spec.
    '''


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
        self._namespace      = Output.from_input(args.namespace)
        s3_endpoint          = Output.from_input(args.s3_endpoint)
        s3_access_key        = Output.from_input(args.s3_access_key)
        s3_secret_key        = Output.from_input(args.s3_secret_key)
        s3_region            = Output.from_input(args.s3_region)
        service_account_name = Output.from_input(args.service_account_name)
        connect_master       = Output.from_input(args.connect_master)
        event_log_bucket     = Output.from_input(args.event_log_bucket)
        ingress_domain       = Output.from_input(args.ingress_domain)
        release              = args.release_name or name
        image                = Output.concat(Output.from_input(args.image), ':', Output.from_input(args.image_tag))
        cat_endpoints        = [Output.from_input(c.polaris_endpoint) for c in args.iceberg_catalogs]

        # ── Spark Connect Server ───────────────────────────────────────────────
        cs_name      = f'{release}-connect'
        cs_app_label = {'app': cs_name}

        connect_args = [
            '--class', 'org.apache.spark.sql.connect.service.SparkConnectServer',
            '--master', connect_master,
            '--conf', 'spark.eventLog.enabled=true',
            '--conf', Output.concat('spark.eventLog.dir=s3a://', event_log_bucket, '/'),
            '--conf', Output.concat('spark.hadoop.fs.s3a.endpoint=', s3_endpoint),
            '--conf', Output.concat('spark.hadoop.fs.s3a.access.key=', s3_access_key),
            '--conf', Output.concat('spark.hadoop.fs.s3a.secret.key=', s3_secret_key),
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', Output.concat('spark.hadoop.fs.s3a.endpoint.region=', s3_region),
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--conf', f'spark.connect.grpc.binding.port={CONNECT_SERVER_PORT}',
            '--conf', Output.concat('spark.kubernetes.namespace=', self._namespace),
            '--conf', Output.concat('spark.kubernetes.authenticate.driver.serviceAccountName=', service_account_name),
            '--conf', Output.concat('spark.kubernetes.container.image=', image),
            '--conf', 'spark.dynamicAllocation.enabled=true',
            '--conf', 'spark.dynamicAllocation.shuffleTracking.enabled=true',
            '--conf', 'spark.dynamicAllocation.minExecutors=0',
            '--conf', f'spark.dynamicAllocation.maxExecutors={args.executor_instances}',
            '--conf', 'spark.dynamicAllocation.executorIdleTimeout=60s',
        ]

        cs_spec = json.loads((CONFIG_DIR / 'resources/spark_connect_spec.json').read_text())
        cs_spec['selector']['matchLabels']                        = cs_app_label
        cs_spec['template']['metadata']['labels']                 = cs_app_label
        cs_spec['template']['spec']['serviceAccountName']         = service_account_name
        cs_spec['template']['spec']['initContainers'][0]['image'] = image
        cs_spec['template']['spec']['containers'][0]['image']     = image
        cs_spec['template']['spec']['containers'][0]['args']      = connect_args

        # ── Iceberg catalog setup (optional) ──────────────────────────────────
        if args.iceberg_catalogs:
            # Append Iceberg JARs to the existing jar-download init container
            iceberg_jar_downloads = (
                ' && curl -fL -o /extra-jars/iceberg-spark-runtime.jar'
                ' https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar'
                ' && curl -fL -o /extra-jars/iceberg-aws-bundle.jar'
                ' https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar'
            )
            cs_spec['template']['spec']['initContainers'][0]['command'][2] += iceberg_jar_downloads

            # Deduped envFrom entries for credential secrets (prefixed per catalog)
            seen_secrets: set = set()
            conf_env_from = []
            for cat in args.iceberg_catalogs:
                if cat.credentials_secret not in seen_secrets:
                    conf_env_from.append({
                        'secretRef': {'name': cat.credentials_secret},
                        'prefix':    f'POLARIS_{cat.name.upper()}_',
                    })
                    seen_secrets.add(cat.credentials_secret)

            # Build the conf script as a plain string — catalog names/warehouses are
            # ordinary Python values. Output values (endpoint, S3 config, credentials)
            # are referenced as shell variables substituted at container runtime.
            script_lines = [
                'set -e', '{',
                'echo "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"',
            ]
            for cat in args.iceberg_catalogs:
                n      = cat.name
                prefix = f'POLARIS_{n.upper()}'
                ps     = str(cat.s3_path_style_access).lower()
                script_lines += [
                    f'echo "spark.sql.catalog.{n}=org.apache.iceberg.spark.SparkCatalog"',
                    f'echo "spark.sql.catalog.{n}.catalog-impl=org.apache.iceberg.rest.RESTCatalog"',
                    f'echo "spark.sql.catalog.{n}.uri=${{POLARIS_ENDPOINT}}/api/catalog"',
                    f'echo "spark.sql.catalog.{n}.warehouse={cat.warehouse}"',
                    f'echo "spark.sql.catalog.{n}.credential=${{{prefix}_CLIENT_ID}}:${{{prefix}_CLIENT_SECRET}}"',
                    f'echo "spark.sql.catalog.{n}.scope=PRINCIPAL_ROLE:ALL"',
                    f'echo "spark.sql.catalog.{n}.oauth2-server-uri=${{POLARIS_ENDPOINT}}/api/catalog/v1/oauth/tokens"',
                    f'echo "spark.sql.catalog.{n}.s3.endpoint=${{S3_ENDPOINT}}"',
                    f'echo "spark.sql.catalog.{n}.s3.access-key=${{S3_ACCESS_KEY}}"',
                    f'echo "spark.sql.catalog.{n}.s3.secret-key=${{S3_SECRET_KEY}}"',
                    f'echo "spark.sql.catalog.{n}.s3.path-style-access={ps}"',
                    f'echo "spark.sql.catalog.{n}.s3.region=${{S3_REGION}}"',
                    f'echo "spark.sql.catalog.{n}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"',
                ]
            script_lines.append('} > /spark-conf/spark-defaults.conf')
            conf_script = '\n'.join(script_lines)

            # Output.all() is only needed to inject the Output-typed values as env vars.
            # All catalogs share one Polaris instance, so a single POLARIS_ENDPOINT suffices.
            setup_conf_container = Output.all(
                ep=cat_endpoints[0], s3ep=s3_endpoint, s3key=s3_access_key, s3sec=s3_secret_key, s3reg=s3_region,
            ).apply(lambda r: {
                'name':    'setup-conf',
                'image':   'busybox:1.36',
                'command': ['sh', '-c', conf_script],
                'env': [
                    {'name': 'POLARIS_ENDPOINT', 'value': r['ep']},
                    {'name': 'S3_ENDPOINT',      'value': r['s3ep']},
                    {'name': 'S3_ACCESS_KEY',    'value': r['s3key']},
                    {'name': 'S3_SECRET_KEY',    'value': r['s3sec']},
                    {'name': 'S3_REGION',        'value': r['s3reg']},
                ],
                'envFrom':      conf_env_from,
                'volumeMounts': [{'name': 'spark-conf', 'mountPath': '/spark-conf'}],
            })
            cs_spec['template']['spec']['initContainers'].append(setup_conf_container)

            # spark-conf volume shared between setup-conf init container and main container
            cs_spec['template']['spec']['volumes'].append({'name': 'spark-conf', 'emptyDir': {}})

            # Mount spark-conf on the main container and point SPARK_CONF_DIR at it.
            # Also mount the credential secrets so the Connect server can log/reload them.
            main_container = cs_spec['template']['spec']['containers'][0]
            main_container.setdefault('volumeMounts', []).append(
                {'name': 'spark-conf', 'mountPath': '/spark-conf'}
            )
            main_container.setdefault('env', []).append(
                {'name': 'SPARK_CONF_DIR', 'value': '/spark-conf'}
            )
            main_container.setdefault('envFrom', []).extend(conf_env_from)

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

        spark_history_opts = Output.concat(
            '-Dspark.history.fs.logDirectory=s3a://', event_log_bucket, '/ ',
            '-Dspark.hadoop.fs.s3a.endpoint=', s3_endpoint, ' ',
            '-Dspark.hadoop.fs.s3a.access.key=', s3_access_key, ' ',
            '-Dspark.hadoop.fs.s3a.secret.key=', s3_secret_key, ' ',
            '-Dspark.hadoop.fs.s3a.path.style.access=true ',
            '-Dspark.hadoop.fs.s3a.endpoint.region=', s3_region, ' ',
            '-Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
        )

        hs_spec = json.loads((CONFIG_DIR / 'resources/history_server_spec.json').read_text())
        hs_spec['selector']['matchLabels']                        = hs_app_label
        hs_spec['template']['metadata']['labels']                 = hs_app_label
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
            host = Output.concat('spark.', ingress_domain)

            ingress_spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            ingress_spec['ingressClassName']                                                     = args.ingress_class_name
            ingress_spec['rules'][0]['host']                                                     = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['name']           = hs_name
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'] = HISTORY_SERVER_PORT

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
            self.history_server_url = Output.concat('http://', host)
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
