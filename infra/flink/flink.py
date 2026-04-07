'''Reusable Apache Flink component for Kubernetes using the flink-kubernetes-operator Helm chart.'''

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import json
import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apiextensions import CustomResource
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class FlinkIcebergCatalogArgs:
    '''Iceberg REST catalog (via Polaris) to register in a Flink job.'''

    name: str
    '''Catalog name as it will appear in Flink SQL (e.g. "bronze").'''

    polaris_endpoint: Input[str]
    '''Base URL of the Polaris service. Accepts a Pulumi Output.'''

    warehouse: str
    '''Catalog name in Polaris to use as the Iceberg warehouse.'''

    s3_endpoint: Input[str]
    '''S3-compatible storage endpoint. Accepts a Pulumi Output.'''

    s3_access_key: Input[str]
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str]
    '''S3 secret key. Accepts a Pulumi Output.'''

    credentials_secret: str = 'polaris-flink-credentials'
    '''
    K8s Secret name containing CLIENT_ID and CLIENT_SECRET for this catalog's
    Polaris principal. Created by polaris.create_principals() and mounted as
    env vars on the job pods. Flink resolves them via ${ENV:CLIENT_ID} at runtime.
    '''

    s3_path_style_access: bool = True
    s3_region: str = 'us-east-1'


@dataclass
class FlinkJobArgs:
    '''Configuration for a PyFlink Application Mode job.'''

    image: str
    '''Docker image containing the PyFlink job script and dependencies.'''

    python_script: str
    '''Path to the Python entry-point script inside the container (e.g. "/opt/flink/jobs/ingest.py").'''

    job_name: str = 'flink-job'
    '''Human-readable name for the Flink job (shown in the UI).'''

    image_tag: str = 'latest'
    '''Image tag.'''

    flink_version: str = 'v2_0'
    '''Flink version string for the FlinkDeployment spec (e.g. "v2_0", "v1_19").'''

    parallelism: int = 1
    '''Default job parallelism.'''

    jobmanager_cpu: float = 0.5
    jobmanager_memory: str = '1024m'

    taskmanager_cpu: float = 1.0
    taskmanager_memory: str = '2048m'
    taskmanager_replicas: int = 1

    env: Dict[str, str] = field(default_factory=dict)
    '''Extra static env vars to set on the job pods.'''

    credentials_secret: Optional[str] = None
    '''
    K8s Secret name (CLIENT_ID / CLIENT_SECRET) to mount as env vars.
    Required when any iceberg_catalogs entry needs Polaris auth.
    '''

    iceberg_catalogs: List[FlinkIcebergCatalogArgs] = field(default_factory=list)
    '''Iceberg catalogs to register in the job via flink-conf.yaml pipeline properties.'''

    autoscaling_enabled: bool = False
    '''
    Enable the Flink operator's built-in autoscaler. Scales parallelism based on
    actual throughput and backpressure — more Flink-aware than KEDA lag thresholds.
    '''

    autoscaling_target_utilization: float = 0.75
    '''Target utilization (0.0–1.0) before the autoscaler scales up.'''

    autoscaling_metrics_window: str = '5m'
    '''Time window for autoscaler metric aggregation.'''

    autoscaling_stabilization_interval: str = '1m'
    '''Minimum time between consecutive scaling decisions.'''

    extra_flink_config: Dict[str, str] = field(default_factory=dict)
    '''Additional key-value pairs merged into the FlinkDeployment flinkConfiguration.'''


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
    '''Namespaces the operator watches. Empty list means cluster-wide.'''

    operator_resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '100m', 'memory': '256Mi'},
        'limits':   {'cpu': '500m', 'memory': '512Mi'},
    })
    '''Resource requests/limits for the operator pod.'''

    extra_values: dict = field(default_factory=dict)


class Flink(pulumi.ComponentResource):
    '''
    Deploys the Apache Flink Kubernetes Operator and exposes submit_job() to
    create FlinkDeployment CRs for PyFlink Application Mode jobs.

    Each submit_job() call creates an isolated job cluster (JobManager + TaskManagers)
    managed by the operator. Credentials and catalog config are passed via K8s Secrets
    mounted as env vars — never embedded in the spec.

    Example:
        ```python
        flink = Flink("flink", FlinkArgs(namespace=ns.metadata.name))

        flink.submit_job("btc-ingest", FlinkJobArgs(
            image="my-registry/flink-btc-job",
            python_script="/opt/flink/jobs/ingest.py",
            parallelism=2,
            credentials_secret=flink_credentials_secret,
            iceberg_catalogs=[
                FlinkIcebergCatalogArgs(
                    name="bronze",
                    polaris_endpoint=polaris.endpoint,
                    warehouse="bronze",
                    s3_endpoint=minio.endpoint,
                    s3_access_key=minio_user,
                    s3_secret_key=minio_password,
                ),
            ],
        ))
        ```
    '''

    def __init__(
        self,
        name: str,
        args: FlinkArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:flink:Flink', name, {}, opts)

        args = args or FlinkArgs()
        self._name = name
        self._namespace = Output.from_input(args.namespace)

        values = self._build_values(args)

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

    def _build_values(self, args: FlinkArgs) -> dict:
        values = json.loads((CONFIG_DIR / 'helm/helm_values_flink.json').read_text())

        values['watchNamespaces'] = args.watch_namespaces
        values['operatorPod']['resources'] = args.operator_resources

        return self._deep_merge(values, args.extra_values)

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
        can register them via the Table API at runtime. Polaris credentials are
        read from a K8s Secret mounted as env vars — never embedded in the spec.

        Args:
            name: Pulumi resource name.
            args: Job configuration.
            opts: Optional resource options.

        Returns:
            The FlinkDeployment CustomResource.
        '''
        # Collect all Input[str] values from catalog configs that need resolution
        all_inputs: Dict[str, Input[str]] = {}
        for i, cat in enumerate(args.iceberg_catalogs):
            all_inputs[f'c{i}_endpoint'] = cat.polaris_endpoint
            all_inputs[f'c{i}_s3ep']     = cat.s3_endpoint
            all_inputs[f'c{i}_s3key']    = cat.s3_access_key
            all_inputs[f'c{i}_s3sec']    = cat.s3_secret_key

        def build_spec(resolved: dict) -> dict:
            spec = json.loads((CONFIG_DIR / 'resources/flink_deployment_spec.json').read_text())

            # Top-level overrides
            spec['flinkVersion'] = args.flink_version
            spec['image']        = f'{args.image}:{args.image_tag}'

            # JobManager resources
            spec['jobManager']['resource']['cpu']    = args.jobmanager_cpu
            spec['jobManager']['resource']['memory'] = args.jobmanager_memory

            # TaskManager resources and replica count
            spec['taskManager']['replicas']           = args.taskmanager_replicas
            spec['taskManager']['resource']['cpu']    = args.taskmanager_cpu
            spec['taskManager']['resource']['memory'] = args.taskmanager_memory

            # Job entrypoint
            spec['job']['args']        = ['-py', args.python_script]
            spec['job']['parallelism'] = args.parallelism

            # Flink configuration
            flink_config: Dict[str, str] = {'pipeline.name': args.job_name}

            # Iceberg/Polaris catalog properties — one block per catalog.
            # Each catalog's Polaris principal credentials are mounted from its
            # credentials_secret as env vars with a per-catalog prefix, then
            # referenced via Flink's ${ENV:VAR} interpolation at runtime.
            for i, cat in enumerate(args.iceberg_catalogs):
                prefix      = f'table.catalog.{cat.name}'
                env_prefix  = f'POLARIS_{cat.name.upper()}_'
                flink_config[f'{prefix}.type']                 = 'iceberg'
                flink_config[f'{prefix}.catalog-type']         = 'rest'
                flink_config[f'{prefix}.uri']                  = f'{resolved[f"c{i}_endpoint"]}/api/catalog'
                flink_config[f'{prefix}.warehouse']            = cat.warehouse
                flink_config[f'{prefix}.credential']           = f'${{ENV:{env_prefix}CLIENT_ID}}:${{ENV:{env_prefix}CLIENT_SECRET}}'
                flink_config[f'{prefix}.s3.endpoint']          = resolved[f'c{i}_s3ep']
                flink_config[f'{prefix}.s3.access-key']        = resolved[f'c{i}_s3key']
                flink_config[f'{prefix}.s3.secret-key']        = resolved[f'c{i}_s3sec']
                flink_config[f'{prefix}.s3.path-style-access'] = str(cat.s3_path_style_access).lower()

            # Operator autoscaler
            if args.autoscaling_enabled:
                flink_config['job.autoscaler.enabled']                 = 'true'
                flink_config['job.autoscaler.target.utilization']      = str(args.autoscaling_target_utilization)
                flink_config['job.autoscaler.metrics.window']          = args.autoscaling_metrics_window
                flink_config['job.autoscaler.stabilization.interval']  = args.autoscaling_stabilization_interval

            flink_config.update(args.extra_flink_config)
            spec['flinkConfiguration'] = flink_config

            # Pod template: env vars + secret mounts applied to both JM and TM.
            # Each catalog's credentials_secret is mounted with a per-catalog prefix
            # so multiple catalogs can coexist without key collisions.
            env_vars = [{'name': k, 'value': v} for k, v in args.env.items()]
            env_from = []
            seen_secrets = set()
            for cat in args.iceberg_catalogs:
                if cat.credentials_secret not in seen_secrets:
                    env_from.append({
                        'secretRef': {'name': cat.credentials_secret},
                        'prefix': f'POLARIS_{cat.name.upper()}_',
                    })
                    seen_secrets.add(cat.credentials_secret)
            # Any extra top-level secret (e.g. for non-catalog auth)
            if args.credentials_secret and args.credentials_secret not in seen_secrets:
                env_from.append({'secretRef': {'name': args.credentials_secret}})

            container = {'name': 'flink-main-container', 'env': env_vars, 'envFrom': env_from}
            pod_template = {'spec': {'containers': [container]}}
            spec['jobManager']['podTemplate']  = pod_template
            spec['taskManager']['podTemplate'] = pod_template

            return spec

        spec = pulumi.Output.all(**all_inputs).apply(build_spec) if all_inputs else pulumi.Output.from_input(build_spec({}))

        resource_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            resource_opts = pulumi.ResourceOptions.merge(resource_opts, opts)

        return CustomResource(
            f'{name}-flinkdeployment',
            api_version='flink.apache.org/v1beta1',
            kind='FlinkDeployment',
            metadata={
                'name': args.job_name,
                'namespace': self._namespace,
            },
            spec=spec,
            opts=resource_opts,
        )

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Flink._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
