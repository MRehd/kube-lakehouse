'''
Apache Flink on Kubernetes — flink-kubernetes-operator Helm chart.

Deploys the Flink Kubernetes Operator and exposes submit_job() to create
FlinkDeployment CRs for PyFlink Application Mode jobs. Each submitted job
gets its own isolated JobManager + TaskManagers managed by the operator.

Iceberg catalog configuration is injected into the FlinkDeployment spec via
flink-conf properties. Polaris credentials come from K8s Secrets mounted as
env vars — they never appear in the spec or Pulumi state.

Example:
    flink = Flink('flink', FlinkArgs(namespace=ns.metadata.name))

    flink.submit_job('btc-ingest', FlinkJobArgs(
        image='my-registry/flink-btc-job',
        python_script='/opt/flink/jobs/ingest.py',
        parallelism=2,
        credentials_secret='polaris-flink-credentials',
        iceberg_catalogs=[
            FlinkIcebergCatalogArgs(
                name='bronze',
                polaris_endpoint=polaris.endpoint,
                warehouse='bronze',
                s3_endpoint=minio.endpoint,
                s3_access_key='minioadmin',
                s3_secret_key=minio_password,
                credentials_secret='polaris-flink-credentials',
            ),
        ],
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
from pulumi_kubernetes.networking.v1 import Ingress

from config.utils.utils import _deep_merge

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class FlinkIcebergCatalogArgs:
    '''An Iceberg REST catalog (via Polaris) to register in a Flink job.'''

    name: str
    '''Catalog name as it appears in Flink SQL (e.g. "bronze").'''

    polaris_endpoint: Input[str]
    '''Base URL of the Polaris service. Accepts a Pulumi Output.'''

    warehouse: Input[str]
    '''Catalog name in Polaris used as the Iceberg warehouse.'''

    s3_endpoint: Input[str]
    '''S3-compatible storage endpoint. Accepts a Pulumi Output.'''

    s3_access_key: Input[str]
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str]
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    credentials_secret: Input[str] = 'polaris-flink-credentials'
    '''
    K8s Secret name containing CLIENT_ID and CLIENT_SECRET for this catalog's
    Polaris principal. Mounted as env vars with a per-catalog prefix.
    Flink resolves them via ${ENV:POLARIS_<CATALOG>_CLIENT_ID} at runtime.
    '''

    s3_path_style_access: bool = True
    '''Use path-style S3 access (required for MinIO).'''

    s3_region: Input[str] = 'us-east-1'
    '''S3 region (any value works for MinIO).'''


@dataclass
class FlinkJobArgs:
    '''Configuration for a PyFlink Application Mode job.'''

    image: Input[str]
    '''Docker image containing the PyFlink job script and its dependencies. Accepts a Pulumi Output (e.g. docker.Image.image_name).'''

    python_script: Input[str]
    '''Path to the Python entry-point inside the container (e.g. "/opt/flink/jobs/ingest.py").'''

    job_name: str = 'flink-job'
    '''Human-readable job name shown in the Flink UI. Also used as the K8s resource name.'''

    image_tag: Input[str] = 'latest'
    '''Image tag.'''

    flink_version: Input[str] = 'v2_0'
    '''Flink version string for the FlinkDeployment spec (e.g. "v2_0", "v1_19").'''

    parallelism: int = 1
    '''Default job parallelism (number of parallel task slots).'''

    jobmanager_cpu: float = 0.5
    '''CPU cores for the JobManager pod.'''

    jobmanager_memory: Input[str] = '1024m'
    '''Memory for the JobManager pod.'''

    taskmanager_cpu: float = 1.0
    '''CPU cores per TaskManager pod.'''

    taskmanager_memory: Input[str] = '2048m'
    '''Memory per TaskManager pod.'''

    taskmanager_replicas: int = 1
    '''Number of TaskManager replicas.'''

    env: Dict[str, Input[str]] = field(default_factory=dict)
    '''Env vars to inject into job pods. Values may be Pulumi Outputs (e.g. kafka.bootstrap_servers).'''

    credentials_secret: Optional[Input[str]] = None
    '''
    A top-level K8s Secret (CLIENT_ID / CLIENT_SECRET) to mount as env vars.
    Required when any iceberg_catalog entry needs Polaris auth without a per-catalog secret.
    '''

    iceberg_catalogs: List[FlinkIcebergCatalogArgs] = field(default_factory=list)
    '''Iceberg catalogs to register in the job via flink-conf pipeline properties.'''

    autoscaling_enabled: bool = False
    '''
    Enable the Flink operator's built-in reactive autoscaler.
    Scales parallelism based on throughput and backpressure.
    '''

    autoscaling_target_utilization: float = 0.75
    '''Target task slot utilization (0.0–1.0) before the autoscaler scales up.'''

    autoscaling_metrics_window: Input[str] = '5m'
    '''Time window for metric aggregation.'''

    autoscaling_stabilization_interval: Input[str] = '1m'
    '''Minimum time between consecutive scaling decisions.'''

    extra_flink_config: Dict[str, Input[str]] = field(default_factory=dict)
    '''Additional key-value entries merged into the FlinkDeployment flinkConfiguration.'''

    ingress_enabled: bool = False
    '''Expose the JobManager web UI via an Ingress.'''

    ingress_domain: Optional[Input[str]] = None
    '''Base domain. Creates <job_name>.<domain> → JobManager UI (port 8081).'''

    ingress_class_name: Input[str] = 'nginx'
    '''Ingress class name.'''


@dataclass
class FlinkArgs:
    '''Configuration arguments for the Flink operator deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name. Defaults to the Pulumi resource name.'''

    chart_version: str = '1.14.0'
    '''Version of the flink-kubernetes-operator Helm chart.'''

    watch_namespaces: List[str] = field(default_factory=list)
    '''Namespaces the operator watches. Empty list = cluster-wide.'''

    operator_resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '100m', 'memory': '256Mi'},
        'limits':   {'cpu': '500m', 'memory': '512Mi'},
    })
    '''CPU and memory requests/limits for the operator pod.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Flink(pulumi.ComponentResource):
    '''
    Deploys the Apache Flink Kubernetes Operator and exposes submit_job() to
    create FlinkDeployment CRs for PyFlink Application Mode jobs.

    Each submit_job() call creates an isolated job cluster (JobManager +
    TaskManagers). Credentials and catalog config are passed via K8s Secrets
    mounted as env vars — never embedded in the FlinkDeployment spec.

    Outputs:
        namespace — Kubernetes namespace

    Example:
        flink = Flink('flink', FlinkArgs(namespace=ns.metadata.name))

        flink.submit_job('btc-ingest', FlinkJobArgs(
            image='my-registry/flink-btc-job',
            python_script='/opt/flink/jobs/ingest.py',
            parallelism=2,
            iceberg_catalogs=[
                FlinkIcebergCatalogArgs(
                    name='bronze',
                    polaris_endpoint=polaris.endpoint,
                    warehouse='bronze',
                    s3_endpoint=minio.endpoint,
                    s3_access_key='minioadmin',
                    s3_secret_key=minio_password,
                    credentials_secret='polaris-flink-credentials',
                ),
            ],
        ))
    '''

    def __init__(
        self,
        name: str,
        args: FlinkArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:flink:Flink', name, {}, opts)

        args = args or FlinkArgs()
        self._namespace = Output.from_input(args.namespace)

        # ── Helm values ───────────────────────────────────────────────────────
        values = json.loads((CONFIG_DIR / 'helm/helm_values_flink.json').read_text())
        values['watchNamespaces']            = args.watch_namespaces
        values['operatorPod']['resources']   = args.operator_resources
        values = _deep_merge(values, args.extra_values)

        # ── Chart ─────────────────────────────────────────────────────────────
        # CRD specs are managed by Helm — ignore changes to prevent spurious diffs
        # when the operator upgrades CRD schema versions.
        def ignore_crd_changes(t: pulumi.ResourceTransformArgs) -> pulumi.ResourceTransformResult:
            if t.type_ == 'kubernetes:apiextensions.k8s.io/v1:CustomResourceDefinition':
                t.opts.ignore_changes = ['spec']
            return pulumi.ResourceTransformResult(props=t.props, opts=t.opts)

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='flink-kubernetes-operator',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(
                    repo=f'https://archive.apache.org/dist/flink/flink-kubernetes-operator-{args.chart_version}/',
                ),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self, transforms=[ignore_crd_changes]),
        )

        self.namespace = self._namespace
        self.register_outputs({'namespace': self.namespace})

    def submit_job(
        self,
        name: str,
        args: FlinkJobArgs,
        opts: pulumi.ResourceOptions = None,
    ) -> CustomResource:
        '''
        Create a FlinkDeployment CR for a PyFlink Application Mode job.

        The operator launches an isolated JobManager + TaskManagers for this job.
        Iceberg catalog properties are injected into flinkConfiguration so PyFlink
        can register them via the Table API. Polaris credentials are read from K8s
        Secrets mounted as env vars with a per-catalog prefix (POLARIS_<CATALOG>_).

        Args:
            name: Pulumi resource name prefix.
            args: Job configuration including image, catalogs, and resources.
            opts: Optional extra resource options.

        Returns:
            The FlinkDeployment CustomResource.

        Example:
            flink.submit_job('my-job', FlinkJobArgs(
                image='my-registry/flink-job',
                python_script='/opt/flink/jobs/job.py',
                parallelism=2,
                iceberg_catalogs=[
                    FlinkIcebergCatalogArgs(
                        name='bronze',
                        polaris_endpoint=polaris.endpoint,
                        warehouse='bronze',
                        s3_endpoint=minio.endpoint,
                        s3_access_key='minioadmin',
                        s3_secret_key=minio_password,
                        credentials_secret='polaris-flink-credentials',
                    ),
                ],
            ))
        '''
        image         = Output.from_input(args.image)
        cat_endpoints = [Output.from_input(c.polaris_endpoint) for c in args.iceberg_catalogs]
        cat_s3eps     = [Output.from_input(c.s3_endpoint)      for c in args.iceberg_catalogs]
        cat_s3keys    = [Output.from_input(c.s3_access_key)    for c in args.iceberg_catalogs]
        cat_s3secs    = [Output.from_input(c.s3_secret_key)    for c in args.iceberg_catalogs]

        spec = json.loads((CONFIG_DIR / 'resources/flink_deployment_spec.json').read_text())

        spec['flinkVersion']                        = args.flink_version
        spec['image']                               = Output.concat(image, ':', args.image_tag)
        spec['jobManager']['resource']['cpu']       = args.jobmanager_cpu
        spec['jobManager']['resource']['memory']    = args.jobmanager_memory
        spec['taskManager']['replicas']             = args.taskmanager_replicas
        spec['taskManager']['resource']['cpu']      = args.taskmanager_cpu
        spec['taskManager']['resource']['memory']   = args.taskmanager_memory
        spec['job']['args']                         = ['-py', args.python_script]
        spec['job']['parallelism']                  = args.parallelism

        # Catalogs are registered at runtime by the Python job script via CREATE CATALOG SQL.
        # We intentionally do NOT add table.catalog.* to flinkConfiguration because the JVM
        # pre-registers them before the Python process starts — when Python then does
        # CREATE CATALOG IF NOT EXISTS, it's a no-op and the JVM-side catalog (which may have
        # unresolved credentials) takes precedence, causing 401s on table operations.
        # Credentials are still mounted as POLARIS_<CATALOG>_CLIENT_ID/SECRET env vars so
        # the Python CREATE CATALOG SQL can read them via os.getenv().
        flink_config: Dict[str, Input[str]] = {'pipeline.name': args.job_name}

        if args.autoscaling_enabled:
            flink_config['job.autoscaler.enabled']                = 'true'
            flink_config['job.autoscaler.target.utilization']     = str(args.autoscaling_target_utilization)
            flink_config['job.autoscaler.metrics.window']         = args.autoscaling_metrics_window
            flink_config['job.autoscaler.stabilization.interval'] = args.autoscaling_stabilization_interval

        flink_config.update(args.extra_flink_config)
        spec['flinkConfiguration'] = flink_config

        # Pod template: static env vars + per-catalog secret mounts (deduped)
        env_vars = [{'name': k, 'value': v} for k, v in args.env.items()]
        env_from = []
        seen_secrets: set = set()
        for cat in args.iceberg_catalogs:
            if cat.credentials_secret not in seen_secrets:
                env_from.append({
                    'secretRef': {'name': cat.credentials_secret},
                    'prefix':    f'POLARIS_{cat.name.upper()}_',
                })
                seen_secrets.add(cat.credentials_secret)
        if args.credentials_secret and args.credentials_secret not in seen_secrets:
            env_from.append({'secretRef': {'name': args.credentials_secret}})

        pod_template = {'spec': {'containers': [
            {'name': 'flink-main-container', 'env': env_vars, 'envFrom': env_from}
        ]}}
        spec['jobManager']['podTemplate']  = pod_template
        spec['taskManager']['podTemplate'] = pod_template

        resource_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            resource_opts = pulumi.ResourceOptions.merge(resource_opts, opts)

        cr = CustomResource(
            f'{name}-flinkdeployment',
            api_version='flink.apache.org/v1beta1',
            kind='FlinkDeployment',
            metadata={'name': args.job_name, 'namespace': self._namespace},
            spec=spec,
            opts=resource_opts,
        )

        if args.ingress_enabled and args.ingress_domain:
            ingress_spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            ingress_spec['ingressClassName']                                                     = args.ingress_class_name
            ingress_spec['rules'][0]['host']                                                     = Output.concat(args.job_name, '.', Output.from_input(args.ingress_domain))
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['name']           = f'{args.job_name}-rest'
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'] = 8081

            Ingress(
                f'{name}-ingress',
                metadata={'name': f'{args.job_name}-ingress', 'namespace': self._namespace},
                spec=ingress_spec,
                opts=pulumi.ResourceOptions(parent=self, depends_on=[cr]),
            )

        return cr
