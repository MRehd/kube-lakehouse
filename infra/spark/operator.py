'''
Spark Kubernetes Operator — kubeflow spark-operator Helm chart.

Deploys the spark-operator controller plus CRDs (SparkApplication,
ScheduledSparkApplication). Each batch job is its own SparkApplication CR with
its own driver pod, isolated from other jobs and from the Spark Connect server.

This is a separate Pulumi ComponentResource from Spark — they share the same
custom image (built by Spark) but are independent at the Kubernetes layer.

Per-job UIs are not exposed via Ingress. Inspect runs in the SparkHistory UI
instead — every SparkApplication writes rolling event logs to the same S3
bucket the History Server reads from, so completed and in-flight runs both
appear there. For ad-hoc debugging of a specific driver, port-forward:

    kubectl port-forward sparkapplication/<job_name>-driver 4040:4040

Job code distribution:
    Bake `.py` files into the custom Spark image at /opt/spark/jobs/, then
    reference them as local:///opt/spark/jobs/<file>.py via main_application_file.
    Mirrors the Flink pattern.

Airflow integration:
    Use SparkKubernetesOperator in DAGs to create SparkApplication CRs at
    runtime (recommended). For always-on / infrastructure-managed jobs, call
    submit_application() from Pulumi instead.

Example:
    operator = SparkOperator('spark-operator', SparkOperatorArgs(
        namespace=ns.metadata.name,
        watch_namespaces=[ns_name],
    ))

    operator.submit_application('daily-etl', SparkApplicationArgs(
        job_name='daily-etl',
        image=spark_image_ref,
        main_application_file='local:///opt/spark/jobs/daily_etl.py',
        executor_instances=4,
        iceberg_catalogs=[bronze_catalog],
        credentials_secret='spark-credentials',
    ))
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apiextensions import CustomResource
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

from config.utils.utils import _deep_merge

from .spark import SparkIcebergCatalogArgs

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class SparkApplicationArgs:
    '''Configuration arguments for a single SparkApplication CR (one batch job).'''

    job_name: str
    '''Name of the SparkApplication CR. Used as the Pulumi resource name suffix too.'''

    image: Input[str]
    '''Full image ref including tag (e.g. spark.image_name output from Spark component).'''

    main_application_file: Input[str]
    '''
    Path to the Python entrypoint inside the image, e.g.
    local:///opt/spark/jobs/daily_etl.py
    '''

    spark_version: str = '4.0.0'
    '''Spark version reported in the SparkApplication spec.'''

    service_account_name: Input[str] = 'spark'
    '''ServiceAccount the driver pod uses to spawn executor pods.'''

    driver_cores: int = 1
    '''CPU cores for the driver pod.'''

    driver_memory: Input[str] = '1g'
    '''JVM heap for the driver.'''

    executor_cores: int = 1
    '''CPU cores per executor (spark.executor.cores → task slots).'''

    executor_memory: Input[str] = '1g'
    '''JVM heap per executor.'''

    executor_instances: int = 1
    '''Number of executor pods (static — no dynamic allocation by default for batch).'''

    s3_endpoint: Optional[Input[str]] = None
    '''S3/MinIO endpoint URL. Defaults to the operator's s3_endpoint.'''

    s3_access_key: Optional[Input[str]] = None
    '''S3 access key. Defaults to the operator's s3_access_key.'''

    s3_secret_key: Optional[Input[str]] = None
    '''S3 secret key. Defaults to the operator's s3_secret_key.'''

    s3_region: Optional[Input[str]] = None
    '''S3/MinIO region. Defaults to the operator's s3_region.'''

    event_log_bucket: Optional[Input[str]] = None
    '''
    S3 bucket the driver writes Spark event logs to. Defaults to the operator's
    event_log_bucket — override only if this job needs a different bucket from
    the cluster-wide one the SparkHistory UI reads from.
    '''

    iceberg_catalogs: List[SparkIcebergCatalogArgs] = field(default_factory=list)
    '''
    Iceberg REST catalogs (Polaris) to register in this job's Spark session.
    Translated into spark.sql.catalog.<name>.* entries in sparkConf.
    Credentials are read from K8s secrets mounted as env vars on the driver pod
    and substituted at runtime via ${...} placeholders in sparkConf.
    '''

    env: Dict[str, Input[str]] = field(default_factory=dict)
    '''Plain key/value env vars injected into both driver and executor pods.'''

    extra_spark_conf: Dict[str, Input[str]] = field(default_factory=dict)
    '''Extra Spark configuration entries (deep-merged over the generated config).'''

    restart_policy: str = 'OnFailure'
    '''SparkApplication restart policy: Never, Always, or OnFailure.'''


@dataclass
class SparkOperatorArgs:
    '''Configuration arguments for the spark-operator Helm release.'''

    namespace: Input[str] = 'default'
    '''Namespace to install the operator into.'''

    chart_version: str = '2.0.2'
    '''Version of the kubeflow spark-operator Helm chart.'''

    watch_namespaces: List[str] = field(default_factory=list)
    '''Namespaces the operator watches for SparkApplication CRs. Empty = cluster-wide.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''

    # ── Cluster-wide defaults applied to every submit_application() call ─────
    # Per-job overrides on SparkApplicationArgs win when set explicitly.

    event_log_bucket: Input[str] = 'spark-logs'
    '''Default S3 bucket for Spark event logs. Match the SparkHistory bucket.'''

    s3_endpoint: Input[str] = ''
    '''Default S3/MinIO endpoint URL for all submitted jobs.'''

    s3_access_key: Input[str] = ''
    '''Default S3 access key for all submitted jobs.'''

    s3_secret_key: Input[str] = ''
    '''Default S3 secret key for all submitted jobs. Accepts a Pulumi secret Output.'''

    s3_region: Input[str] = 'us-east-1'
    '''Default S3/MinIO region for all submitted jobs.'''


class SparkOperator(pulumi.ComponentResource):
    '''
    Deploys the kubeflow spark-operator Helm chart and exposes a `submit_application`
    method to create SparkApplication CRs for batch jobs.

    Each SparkApplication gets its own driver + executor pods, isolated from other
    jobs and from the Spark Connect server (which runs as a separate Deployment
    managed by the Spark component). Both can coexist in the same namespace.

    Outputs:
        namespace — Namespace where the operator runs

    Example:
        operator = SparkOperator('spark-operator', SparkOperatorArgs(
            namespace=ns.metadata.name,
            watch_namespaces=[ns_name],
        ))
    '''

    def __init__(
        self,
        name: str,
        args: SparkOperatorArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:spark:SparkOperator', name, {}, opts)

        args = args or SparkOperatorArgs()
        self._namespace        = Output.from_input(args.namespace)
        self._event_log_bucket = args.event_log_bucket
        self._s3_endpoint      = args.s3_endpoint
        self._s3_access_key    = args.s3_access_key
        self._s3_secret_key    = args.s3_secret_key
        self._s3_region        = args.s3_region

        values = json.loads((CONFIG_DIR / 'helm/helm_values_spark.json').read_text())
        values['spark']['jobNamespaces'] = args.watch_namespaces
        values = _deep_merge(values, args.extra_values)

        # CRDs are managed by Helm — ignore changes to prevent spurious diffs
        # when the operator upgrades CRD schema versions.
        def ignore_crd_changes(t: pulumi.ResourceTransformArgs) -> pulumi.ResourceTransformResult:
            if t.type_ == 'kubernetes:apiextensions.k8s.io/v1:CustomResourceDefinition':
                t.opts.ignore_changes = ['spec']
            return pulumi.ResourceTransformResult(props=t.props, opts=t.opts)

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='spark-operator',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://kubeflow.github.io/spark-operator'),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self, transforms=[ignore_crd_changes]),
        )

        self.namespace = self._namespace
        self.register_outputs({'namespace': self.namespace})

    def submit_application(
        self,
        name: str,
        args: SparkApplicationArgs,
        opts: pulumi.ResourceOptions = None,
    ) -> CustomResource:
        '''
        Create a SparkApplication CR for a single batch job.

        The operator launches an isolated driver + executor pods for this job,
        runs it to completion, and (per restart_policy) deletes or retries them.

        Iceberg catalog properties are baked into the SparkApplication's sparkConf.
        Polaris/S3 credentials are mounted as env vars on the driver pod via
        envFrom, and referenced from sparkConf using ${...} placeholders so values
        are resolved at job runtime, not at Pulumi-deploy time.

        Returns the SparkApplication CustomResource.
        '''
        image                = Output.from_input(args.image)
        main_application     = Output.from_input(args.main_application_file)
        service_account_name = Output.from_input(args.service_account_name)
        s3_endpoint          = Output.from_input(args.s3_endpoint      if args.s3_endpoint      is not None else self._s3_endpoint)
        s3_access_key        = Output.from_input(args.s3_access_key    if args.s3_access_key    is not None else self._s3_access_key)
        s3_secret_key        = Output.from_input(args.s3_secret_key    if args.s3_secret_key    is not None else self._s3_secret_key)
        s3_region            = Output.from_input(args.s3_region        if args.s3_region        is not None else self._s3_region)
        event_log_bucket     = Output.from_input(args.event_log_bucket if args.event_log_bucket is not None else self._event_log_bucket)

        spec = json.loads((CONFIG_DIR / 'resources/spark_application_spec.json').read_text())
        spec['image']                            = image
        spec['mainApplicationFile']              = main_application
        spec['sparkVersion']                     = args.spark_version
        spec['restartPolicy']['type']            = args.restart_policy
        spec['driver']['cores']                  = args.driver_cores
        spec['driver']['memory']                 = args.driver_memory
        spec['driver']['serviceAccount']         = service_account_name
        spec['executor']['cores']                = args.executor_cores
        spec['executor']['memory']               = args.executor_memory
        spec['executor']['instances']            = args.executor_instances

        # ── Spark configuration ──────────────────────────────────────────────
        # Rolling event logs make in-flight runs visible in the SparkHistory UI
        # without waiting for job completion (combined with a low
        # spark.history.fs.update.interval on the History Server side).
        spark_conf: Dict[str, Input[str]] = {
            'spark.hadoop.fs.s3a.endpoint':            s3_endpoint,
            'spark.hadoop.fs.s3a.access.key':          s3_access_key,
            'spark.hadoop.fs.s3a.secret.key':          s3_secret_key,
            'spark.hadoop.fs.s3a.endpoint.region':     s3_region,
            'spark.hadoop.fs.s3a.path.style.access':   'true',
            'spark.hadoop.fs.s3a.impl':                'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.eventLog.enabled':                  'true',
            'spark.eventLog.dir':                      Output.concat('s3a://', event_log_bucket, '/'),
            'spark.eventLog.rolling.enabled':          'true',
            'spark.eventLog.rolling.maxFileSize':      '64m',
        }

        if args.iceberg_catalogs:
            spark_conf['spark.sql.extensions'] = (
                'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
            )
            for cat in args.iceberg_catalogs:
                n  = cat.name
                ps = str(cat.s3_path_style_access).lower()
                spark_conf.update({
                    f'spark.sql.catalog.{n}':                  'org.apache.iceberg.spark.SparkCatalog',
                    f'spark.sql.catalog.{n}.catalog-impl':     'org.apache.iceberg.rest.RESTCatalog',
                    f'spark.sql.catalog.{n}.uri':              Output.concat(Output.from_input(cat.polaris_endpoint), '/api/catalog'),
                    f'spark.sql.catalog.{n}.warehouse':        cat.warehouse,
                    f'spark.sql.catalog.{n}.credential':       '${POLARIS_CLIENT_ID}:${POLARIS_CLIENT_SECRET}',
                    f'spark.sql.catalog.{n}.scope':            'PRINCIPAL_ROLE:ALL',
                    f'spark.sql.catalog.{n}.oauth2-server-uri': Output.concat(Output.from_input(cat.polaris_endpoint), '/api/catalog/v1/oauth/tokens'),
                    f'spark.sql.catalog.{n}.s3.endpoint':      s3_endpoint,
                    f'spark.sql.catalog.{n}.s3.access-key':    s3_access_key,
                    f'spark.sql.catalog.{n}.s3.secret-key':    s3_secret_key,
                    f'spark.sql.catalog.{n}.s3.path-style-access': ps,
                    f'spark.sql.catalog.{n}.s3.region':        s3_region,
                    f'spark.sql.catalog.{n}.io-impl':          'org.apache.iceberg.aws.s3.S3FileIO',
                })

        spark_conf.update(args.extra_spark_conf)
        spec['sparkConf'] = spark_conf

        # ── Pod env: shared between driver and executors ─────────────────────
        env_vars = [{'name': k, 'value': v} for k, v in args.env.items()]

        # Mount catalog credentials as env vars on the driver. All catalogs share
        # one principal's credentials secret (matches the Connect server pattern),
        # so a single secretRef is enough — sparkConf above references the env vars
        # via ${POLARIS_CLIENT_ID} / ${POLARIS_CLIENT_SECRET}.
        env_from = []
        if args.iceberg_catalogs:
            env_from.append({'secretRef': {'name': args.iceberg_catalogs[0].credentials_secret}})

        spec['driver']['env']     = env_vars
        spec['driver']['envFrom'] = env_from
        spec['executor']['env']   = env_vars

        resource_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            resource_opts = pulumi.ResourceOptions.merge(resource_opts, opts)

        return CustomResource(
            f'{name}-sparkapplication',
            api_version='sparkoperator.k8s.io/v1beta2',
            kind='SparkApplication',
            metadata={'name': args.job_name, 'namespace': self._namespace},
            spec=spec,
            opts=resource_opts,
        )
