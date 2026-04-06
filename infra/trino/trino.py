'''Reusable Trino component for Kubernetes using the trinodb/trino Helm chart.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class TrinoIcebergCatalogArgs:
    '''Configuration for a Trino Iceberg catalog backed by a Polaris REST endpoint.'''

    name: str
    '''Catalog name as it will appear in Trino (e.g. 'bronze').'''

    polaris_endpoint: Input[str]
    '''Base URL of the Polaris service (e.g. http://polaris:8181). Accepts a Pulumi Output.'''

    warehouse: str
    '''Catalog name in Polaris to use as the Iceberg warehouse.'''

    client_id: Input[str] = 'root'
    '''
    OAuth2 client ID for authenticating with Polaris. Accepts a Pulumi Output.
    Ignored when credentials_secret is set.
    '''

    client_secret: Input[str] = 'root'
    '''
    OAuth2 client secret for authenticating with Polaris. Accepts a Pulumi Output (secret).
    Ignored when credentials_secret is set.
    '''

    credentials_secret: Optional[str] = None
    '''
    Name of a Kubernetes Secret containing 'client-id' and 'client-secret' keys.
    When set, credentials are read from env vars at pod startup (via ${ENV:VAR} in catalog
    properties) instead of being embedded inline in the Helm values. Use this with
    Polaris principals whose credentials were stored via PrincipalArgs.credentials_secret_name.
    '''

    s3_endpoint: Input[str] = ''
    '''S3-compatible storage endpoint (e.g. http://minio:9000). Accepts a Pulumi Output.'''

    s3_access_key: Input[str] = ''
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi Output (secret).'''

    s3_path_style_access: bool = True
    '''Use path-style S3 access (required for MinIO).'''

    s3_region: str = 'us-east-1'
    '''S3 region (can be any value for MinIO).'''


@dataclass
class TrinoAutoscalingArgs:
    '''HPA configuration for Trino worker pods.'''

    enabled: bool = True
    '''Enable the Horizontal Pod Autoscaler for workers.'''

    min_replicas: int = 1
    '''Minimum number of worker replicas.'''

    max_replicas: int = 10
    '''Maximum number of worker replicas.'''

    target_cpu_utilization: Optional[int] = 70
    '''Target CPU utilization percentage. Set to None to omit.'''

    target_memory_utilization: Optional[int] = None
    '''Target memory utilization percentage. Set to None to omit.'''


@dataclass
class TrinoArgs:
    '''Configuration arguments for Trino deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy Trino into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    chart_version: str = '1.42.1'
    '''Version of the trinodb/trino Helm chart.'''

    workers: int = 2
    '''Number of Trino worker replicas.'''

    coordinator_heap: str = '1G'
    '''JVM max heap size for the coordinator (e.g. '2G').'''

    worker_heap: str = '1G'
    '''JVM max heap size for each worker (e.g. '2G').'''

    coordinator_resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '500m',  'memory': '1Gi'},
        'limits':   {'cpu': '1000m', 'memory': '2Gi'},
    })
    '''Resource requests and limits for the Trino coordinator pod.'''

    worker_resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '500m',  'memory': '1Gi'},
        'limits':   {'cpu': '1000m', 'memory': '2Gi'},
    })
    '''Resource requests and limits for Trino worker pods.'''

    ingress_enabled: bool = False
    '''Enable Ingress for external access to the Trino UI and JDBC endpoint.'''

    ingress_domain: Optional[str] = None
    '''Domain for Ingress (e.g. 'k8lh.local'). Creates trino.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name (e.g. 'nginx', 'traefik').'''

    ingress_annotations: Optional[dict] = None
    '''Additional annotations for the Ingress resource.'''

    autoscaling: Optional['TrinoAutoscalingArgs'] = None
    '''HPA configuration for worker pods. None disables autoscaling.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''


class Trino(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying Trino to Kubernetes using Helm.

    Trino is a distributed SQL query engine. This component deploys the coordinator
    and worker pods and exposes a create_catalogs() method for registering Iceberg
    catalogs backed by a Polaris REST endpoint.

    Catalogs are registered via create_catalogs() and compiled into the Helm chart
    values before deployment. Call create_catalogs() immediately after constructing
    the Trino instance, before pulumi up runs.

    Example:
        ```python
        from trino import Trino, TrinoArgs, TrinoIcebergCatalogArgs

        trino = Trino('my-trino', TrinoArgs(
            namespace='data',
            workers=2,
            ingress_enabled=True,
            ingress_domain='k8lh.local',
        ))

        trino.create_catalogs('trino-catalogs', [
            TrinoIcebergCatalogArgs(
                name='bronze',
                polaris_endpoint=polaris.endpoint,
                warehouse='bronze',
                client_id='root',
                client_secret='root',
                s3_endpoint=minio.endpoint,
                s3_access_key='minioadmin',
                s3_secret_key=minio_password,
            ),
        ])
        ```
    '''

    def __init__(
        self,
        name: str,
        args: TrinoArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:trino:Trino', name, {}, opts)

        args = args or TrinoArgs()
        self._args = args
        self._name = name
        self._release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)

        # Accumulator for catalogs registered via create_catalogs().
        # Populated before Pulumi resolves the values Output (all Python runs first).
        self._catalog_list: List[TrinoIcebergCatalogArgs] = []

        # Values are built lazily so that create_catalogs() calls made after __init__
        # are captured before Pulumi resolves the Output.
        values = Output.from_input(True).apply(
            lambda _: self._resolve_values(args)
        )

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='trino',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(
                    repo='https://trinodb.github.io/charts',
                ),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace
        self.endpoint = pulumi.Output.concat(
            'http://', self._release_name, '.', self._namespace,
            '.svc.cluster.local:8080'
        )

        if args.ingress_enabled and args.ingress_domain:
            self.host = f'trino.{args.ingress_domain}'
            self.ui_url = Output.from_input(f'http://{self.host}')
        else:
            self.ui_url = self.endpoint

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint': self.endpoint,
            'ui_url': self.ui_url,
        })

    def create_catalogs(
        self,
        name: str,
        catalogs: List[TrinoIcebergCatalogArgs],
        opts: pulumi.ResourceOptions = None,
    ) -> 'Trino':
        '''
        Register one or more Iceberg REST catalogs backed by Polaris.

        Must be called before pulumi up — catalogs are compiled into the Helm
        chart values at deployment time, not injected post-deployment.

        Args:
            name: Logical name for this set of catalogs (for documentation only).
            catalogs: List of catalog configurations.
            opts: Unused — present for API consistency with other components.

        Returns:
            self, for method chaining.

        Example:
            ```python
            trino.create_catalogs('lakehouse-catalogs', [
                TrinoIcebergCatalogArgs(
                    name='bronze',
                    polaris_endpoint=polaris.endpoint,
                    warehouse='bronze',
                    client_id='root',
                    client_secret='root',
                    s3_endpoint=minio.endpoint,
                    s3_access_key=minio_user,
                    s3_secret_key=minio_password,
                ),
            ])
            ```
        '''
        self._catalog_list.extend(catalogs)
        return self

    def _resolve_values(self, args: TrinoArgs) -> Output:
        '''
        Build an Output[dict] of Helm values with all registered catalogs resolved.

        Called lazily by Pulumi after all Python code has run, so self._catalog_list
        is fully populated with any create_catalogs() calls that followed __init__.
        '''
        if not self._catalog_list:
            return Output.from_input(self._build_values(args, {}))

        # Collect every Input[str] from every catalog so they can be resolved together.
        all_inputs = {}
        for i, cat in enumerate(self._catalog_list):
            all_inputs[f'c{i}_endpoint'] = cat.polaris_endpoint
            all_inputs[f'c{i}_s3ep']     = cat.s3_endpoint
            all_inputs[f'c{i}_s3key']    = cat.s3_access_key
            all_inputs[f'c{i}_s3sec']    = cat.s3_secret_key
            # Only resolve inline credentials when not sourcing from a K8s secret
            if not cat.credentials_secret:
                all_inputs[f'c{i}_id']     = Output.from_input(cat.client_id)
                all_inputs[f'c{i}_secret'] = Output.from_input(cat.client_secret)

        return pulumi.Output.all(**all_inputs).apply(
            lambda r: self._build_values(args, r)
        )

    def _build_values(self, args: TrinoArgs, resolved: dict) -> dict:
        '''Build Helm chart values from TrinoArgs and the resolved catalog inputs.'''
        values = json.loads((CONFIG_DIR / 'helm/helm_values_trino.json').read_text())

        values['fullnameOverride'] = self._release_name
        values['server']['workers'] = args.workers
        values['coordinator']['jvm']['maxHeapSize'] = args.coordinator_heap
        values['coordinator']['resources'] = args.coordinator_resources
        values['worker']['jvm']['maxHeapSize'] = args.worker_heap
        values['worker']['resources'] = args.worker_resources

        # Build catalog properties strings
        catalog_configs = {}
        # Collect env var sources from K8s secrets (one entry per catalog that uses a secret)
        env_from_secrets = []

        for i, cat in enumerate(self._catalog_list):
            endpoint = resolved[f'c{i}_endpoint']
            s3ep     = resolved[f'c{i}_s3ep']
            s3key    = resolved[f'c{i}_s3key']
            s3sec    = resolved[f'c{i}_s3sec']

            if cat.credentials_secret:
                # Use Trino's ${ENV:VAR} interpolation — credentials stay out of Helm values
                env_prefix = f'TRINO_{cat.name.upper()}'
                credential_line = f'${{ENV:{env_prefix}_CLIENT_ID}}:${{ENV:{env_prefix}_CLIENT_SECRET}}'
                env_from_secrets.append({
                    'secretRef': {'name': cat.credentials_secret},
                    'prefix': f'{env_prefix}_',
                })
            else:
                credential_line = f'{resolved[f"c{i}_id"]}:{resolved[f"c{i}_secret"]}'

            catalog_configs[cat.name] = '\n'.join([
                'connector.name=iceberg',
                'iceberg.catalog.type=rest',
                f'iceberg.rest-catalog.uri={endpoint}/api/catalog',
                f'iceberg.rest-catalog.warehouse={cat.warehouse}',
                'iceberg.rest-catalog.security=OAUTH2',
                f'iceberg.rest-catalog.oauth2.credential={credential_line}',
                'fs.native-s3.enabled=true',
                f's3.endpoint={s3ep}',
                f's3.path-style-access={str(cat.s3_path_style_access).lower()}',
                f's3.aws-access-key={s3key}',
                f's3.aws-secret-key={s3sec}',
                f's3.region={cat.s3_region}',
            ]) + '\n'

        values['catalogs'] = catalog_configs

        # Mount K8s secrets as env vars on all pods (coordinator + worker)
        if env_from_secrets:
            values.setdefault('envFrom', [])
            values['envFrom'].extend(env_from_secrets)

        if args.autoscaling:
            a = args.autoscaling
            hpa: dict = {
                'enabled': a.enabled,
                'minReplicas': a.min_replicas,
                'maxReplicas': a.max_replicas,
                'targetCPUUtilizationPercentage': a.target_cpu_utilization,
            }
            if a.target_memory_utilization is not None:
                hpa['targetMemoryUtilizationPercentage'] = a.target_memory_utilization
            values['worker']['autoscaling'] = hpa

        if args.ingress_enabled and args.ingress_domain:
            values['ingress'] = {
                'enabled': True,
                'className': args.ingress_class_name,
                'annotations': args.ingress_annotations or {},
                'hosts': [{
                    'host': f'trino.{args.ingress_domain}',
                    'paths': [{'path': '/', 'pathType': 'ImplementationSpecific'}],
                }],
                'tls': [],
            }

        values = self._deep_merge(values, args.extra_values)
        return values

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        '''Deep merge two dictionaries, with override taking precedence.'''
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Trino._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
