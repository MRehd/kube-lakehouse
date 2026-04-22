'''
Apache Spark Connect on Kubernetes.

Deploys the Spark Connect server (port 15002) — a persistent gRPC endpoint
that Airflow workers connect to via the @task.pyspark decorator or directly
with pyspark.sql.SparkSession. Runs as a standalone Deployment; executor pods
are created on demand by the Spark K8s scheduler (k8s:// master) and destroyed
when idle. Uses dynamic allocation: minExecutors=0, maxExecutors=N.

The Spark History Server lives in a sibling component (SparkHistory) so it can
be shared with the SparkOperator. Both Connect and operator-managed jobs write
event logs to the same s3a://<event_log_bucket>/ path, and the History Server
reads from there. Rolling event logs are enabled so in-flight runs surface in
the UI without waiting for job completion.

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
import pulumi_docker as docker
from pulumi import Input, Output
from pulumi_kubernetes.apps.v1 import Deployment
from pulumi_kubernetes.core.v1 import Service

CONFIG_DIR    = Path(__file__).parent.parent / 'config'
BUILD_CONTEXT = str(Path(__file__).parent / 'image')

CONNECT_SERVER_PORT = 15002


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

    image_name: Input[str] = ''
    '''
    Full image name without tag for the custom Spark image, e.g. "docker.io/myuser/spark".
    Built from infra/spark/image/ (FROM apache/spark with extra Python deps baked in).
    Used everywhere — Connect server, History Server, and executor pods.
    '''

    image_tag: Input[str] = '4.0.0'
    '''Image tag — also passed as the SPARK_VERSION build arg (FROM apache/spark:<tag>).'''

    registry_username: Input[str] = ''
    '''Docker registry username.'''

    registry_password: Input[str] = ''
    '''Docker registry password or access token. Accepts a Pulumi secret Output.'''

    registry_server: Input[str] = 'https://index.docker.io/v1/'
    '''Docker registry server URL.'''

    service_account_name: Input[str] = 'spark'
    '''
    Name of the ServiceAccount used by the Connect server pod.
    This SA must have pod/service/configmap CRUD permissions so the K8s
    scheduler can create and manage executor pods. Create via ServiceAccounts
    and pass the output here.
    '''

    connect_master: Input[str] = 'k8s://kubernetes.default.svc:443'
    '''
    Spark master URL for the Connect server.
    - "k8s://kubernetes.default.svc:443" — spawn executor pods via K8s (default)
    - "local[*]"                                 — all computation inside the Connect pod (dev only)
    When using k8s://, executors are created in the same namespace using the same
    image and ServiceAccount. Dynamic allocation tears them down when idle.
    '''

    executor_instances: int = 4
    '''Maximum executor pods for dynamic allocation (maxExecutors). minExecutors is always 0.'''

    executor_memory: Input[str] = '1g'
    '''JVM heap per executor (spark.executor.memory). Pod memory request adds ~10% overhead.'''

    executor_cores: int = 1
    '''Task slots per executor (spark.executor.cores).'''

    executor_request_cores: Input[str] = '500m'
    '''K8s CPU request per executor pod. Lower than limit for bursty workloads.'''

    executor_limit_cores: Input[str] = '1'
    '''K8s CPU limit per executor pod. Should be >= executor_cores.'''

    driver_memory: Input[str] = '1g'
    '''JVM heap for the Connect server (spark.driver.memory).'''

    driver_cores: int = 1
    '''CPU cores for the Connect server driver (spark.driver.cores).'''

    event_log_bucket: Input[str] = 'spark-logs'
    '''
    MinIO bucket where the Connect server writes Spark event logs.
    Point the SparkHistory component at the same bucket so its UI surfaces
    every Connect-driven run.
    '''

    s3_endpoint: Input[str] = ''
    '''S3/MinIO endpoint URL. Accepts a Pulumi Output.'''

    s3_access_key: Input[str] = ''
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    s3_region: Input[str] = 'us-east-1'
    '''S3/MinIO region.'''

    iceberg_catalogs: List[SparkIcebergCatalogArgs] = field(default_factory=list)
    '''
    Iceberg REST catalogs to register in the Spark Connect server.
    Configuration is injected via spark-defaults.conf generated at pod startup.
    Credentials are read from K8s Secrets mounted as env vars — never embedded
    in the pod spec.
    '''


class Spark(pulumi.ComponentResource):
    '''
    Deploys a Spark Connect server on Kubernetes.

    The Connect server acts as the Spark master for interactive/programmatic
    sessions. It spawns executor pods on demand via the K8s scheduler and
    releases them when idle (dynamic allocation). Zero idle worker cost.

    Event logs are written to s3a://<event_log_bucket>/ with rolling enabled
    so in-flight runs are visible in the SparkHistory UI within seconds.

    Outputs:
        namespace          — Kubernetes namespace
        image              — Full image ref (used by SparkHistory and SparkOperator jobs)
        connect_server_url — sc://<host>:<port> URI for Spark Connect clients
        connect_ui_url     — http://<host>[:port] URL for the Connect driver UI

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
        release              = args.release_name or name
        self.spark_version   = Output.from_input(args.image_tag)
        image_name           = Output.from_input(args.image_name)
        image_tag            = Output.from_input(args.image_tag)
        full_image           = Output.concat(image_name, ':', image_tag)
        cat_endpoints        = [Output.from_input(c.polaris_endpoint) for c in args.iceberg_catalogs]

        # Build a custom Spark image from infra/spark/image/ — bakes Python deps
        # (numpy, polars, mlflow, ...) on top of apache/spark:<tag> so they're
        # available on both the Connect server and the executor pods it spawns.
        image_obj = docker.Image(
            f'{name}-image',
            image_name=full_image,
            build=docker.DockerBuildArgs(
                context=BUILD_CONTEXT,
                dockerfile=f'{BUILD_CONTEXT}/dockerfile',
                args={'SPARK_VERSION': image_tag},
                platform='linux/amd64',
            ),
            registry=docker.RegistryArgs(
                server=Output.from_input(args.registry_server),
                username=Output.from_input(args.registry_username),
                password=Output.from_input(args.registry_password),
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )
        image      = image_obj.image_name
        self.image = image

        # ── Spark Connect Server ───────────────────────────────────────────────
        cs_name      = f'{release}-connect'
        cs_app_label = {'app': cs_name}

        connect_args = [
            '--class', 'org.apache.spark.sql.connect.service.SparkConnectServer',
            '--master', connect_master,
            '--conf', 'spark.eventLog.enabled=true',
            '--conf', Output.concat('spark.eventLog.dir=s3a://', event_log_bucket, '/'),
            '--conf', 'spark.eventLog.rolling.enabled=true',
            '--conf', 'spark.eventLog.rolling.maxFileSize=64m',
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
            '--conf', 'spark.sql.adaptive.enabled=true',
            '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
            '--conf', 'spark.dynamicAllocation.enabled=true',
            '--conf', 'spark.dynamicAllocation.shuffleTracking.enabled=true',
            '--conf', 'spark.dynamicAllocation.minExecutors=0',
            '--conf', f'spark.dynamicAllocation.maxExecutors={args.executor_instances}',
            '--conf', 'spark.dynamicAllocation.executorIdleTimeout=60s',
            '--conf', Output.concat('spark.executor.memory=',                  Output.from_input(args.executor_memory)),
            '--conf', f'spark.executor.cores={args.executor_cores}',
            '--conf', Output.concat('spark.kubernetes.executor.request.cores=', Output.from_input(args.executor_request_cores)),
            '--conf', Output.concat('spark.kubernetes.executor.limit.cores=',   Output.from_input(args.executor_limit_cores)),
            '--conf', Output.concat('spark.driver.memory=',                    Output.from_input(args.driver_memory)),
            '--conf', f'spark.driver.cores={args.driver_cores}',
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

            # Single envFrom entry — all catalogs share one principal's credentials.
            conf_env_from = [{
                'secretRef': {'name': args.iceberg_catalogs[0].credentials_secret},
                'prefix':    'POLARIS_',
            }]

            # Build the conf script as a plain string — catalog names/warehouses are
            # ordinary Python values. Output values (endpoint, S3 config, credentials)
            # are referenced as shell variables substituted at container runtime.
            script_lines = [
                'set -e', '{',
                'echo "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"',
            ]
            for cat in args.iceberg_catalogs:
                n  = cat.name
                ps = str(cat.s3_path_style_access).lower()
                script_lines += [
                    f'echo "spark.sql.catalog.{n}=org.apache.iceberg.spark.SparkCatalog"',
                    f'echo "spark.sql.catalog.{n}.catalog-impl=org.apache.iceberg.rest.RESTCatalog"',
                    f'echo "spark.sql.catalog.{n}.uri=${{POLARIS_ENDPOINT}}/api/catalog"',
                    f'echo "spark.sql.catalog.{n}.warehouse={cat.warehouse}"',
                    f'echo "spark.sql.catalog.{n}.credential=${{POLARIS_CLIENT_ID}}:${{POLARIS_CLIENT_SECRET}}"',
                    f'echo "spark.sql.catalog.{n}.scope=PRINCIPAL_ROLE:ALL"',
                    f'echo "spark.sql.catalog.{n}.oauth2-server-uri=${{POLARIS_ENDPOINT}}/api/catalog/v1/oauth/tokens"',
                    f'echo "spark.sql.catalog.{n}.s3.endpoint=${{S3_ENDPOINT}}"',
                    f'echo "spark.sql.catalog.{n}.s3.access-key-id=${{S3_ACCESS_KEY}}"',
                    f'echo "spark.sql.catalog.{n}.s3.secret-access-key=${{S3_SECRET_KEY}}"',
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

        # The live Connect driver UI (port 4040) is not exposed via Ingress —
        # use the SparkHistory UI for run inspection (rolling event logs surface
        # in-flight runs within seconds), or port-forward for real-time:
        #   kubectl port-forward deployment/<cs_name> 4040:4040
        self.connect_ui_url = Output.concat(
            'http://', cs_name, '.', self._namespace, '.svc.cluster.local:4040',
        )

        self.namespace = self._namespace
        self.register_outputs({
            'namespace':          self.namespace,
            'image':              self.image,
            'connect_server_url': self.connect_server_url,
            'connect_ui_url':     self.connect_ui_url,
            'spark_version':      self.spark_version,
        })
