'''
Trino on Kubernetes — trinodb/trino Helm chart.

Deploys a Trino coordinator + workers with Iceberg REST catalogs backed by a Polaris
endpoint. Credentials (CLIENT_ID / CLIENT_SECRET) are sourced from K8s Secrets via
Trino's ${ENV:VAR} interpolation — they never appear in Helm values or Pulumi state.

Example:
    trino = Trino('trino', TrinoArgs(
        namespace=ns.metadata.name,
        workers=2,
        ingress_enabled=True,
        ingress_domain='k8lh.local',
        catalogs=[
            TrinoIcebergCatalogArgs(
                name='bronze',
                polaris_endpoint=polaris.endpoint,
                warehouse='bronze',
                credentials_secret='polaris-trino-credentials',
                s3_endpoint=minio.endpoint,
                s3_access_key='minioadmin',
                s3_secret_key=minio_password,
            ),
        ],
    ))
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

from config.utils.utils import _deep_merge

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class TrinoIcebergCatalogArgs:
    '''Configuration for a Trino Iceberg catalog backed by a Polaris REST endpoint.'''

    name: str
    '''Catalog name as it appears in Trino SQL (e.g. "bronze").'''

    polaris_endpoint: Input[str]
    '''Base URL of the Polaris service (e.g. http://polaris:8181). Accepts a Pulumi Output.'''

    warehouse: str
    '''Catalog name in Polaris used as the Iceberg warehouse.'''

    s3_endpoint: Input[str] = ''
    '''S3-compatible storage endpoint. Accepts a Pulumi Output.'''

    s3_access_key: Input[str] = ''
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    s3_path_style_access: bool = True
    '''Use path-style S3 access (required for MinIO).'''

    s3_region: str = 'us-east-1'
    '''S3 region (any value works for MinIO).'''

    client_id: Input[str] = 'root'
    '''
    OAuth2 client ID for Polaris authentication. Accepts a Pulumi Output.
    Ignored when credentials_secret is set — use that instead for production.
    '''

    client_secret: Input[str] = 'root'
    '''
    OAuth2 client secret. Accepts a Pulumi secret Output.
    Ignored when credentials_secret is set.
    '''

    credentials_secret: Optional[str] = None
    '''
    Name of a K8s Secret containing CLIENT_ID and CLIENT_SECRET keys.
    When set, credentials are read from env vars at pod startup via Trino's
    ${ENV:VAR} interpolation — they never appear in Helm values or state.
    Use this with secrets created by polaris.create_principals().
    '''


@dataclass
class TrinoAutoscalingArgs:
    '''HPA configuration for Trino worker pods.'''

    enabled: bool = True
    '''Enable the Horizontal Pod Autoscaler for workers.'''

    min_replicas: int = 1
    '''Minimum worker replicas.'''

    max_replicas: int = 10
    '''Maximum worker replicas.'''

    target_cpu_utilization: Optional[int] = 70
    '''Target CPU utilization percentage. None omits this metric.'''

    target_memory_utilization: Optional[int] = None
    '''Target memory utilization percentage. None omits this metric.'''


@dataclass
class TrinoArgs:
    '''Configuration arguments for Trino deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy Trino into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '1.42.1'
    '''Version of the trinodb/trino Helm chart.'''

    workers: int = 2
    '''Number of Trino worker replicas.'''

    coordinator_heap: str = '1G'
    '''JVM max heap size for the coordinator (e.g. "2G").'''

    worker_heap: str = '1G'
    '''JVM max heap size for each worker.'''

    coordinator_resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '500m',  'memory': '1Gi'},
        'limits':   {'cpu': '1000m', 'memory': '2Gi'},
    })
    '''CPU and memory requests/limits for the coordinator pod.'''

    worker_resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '500m',  'memory': '1Gi'},
        'limits':   {'cpu': '1000m', 'memory': '2Gi'},
    })
    '''CPU and memory requests/limits for worker pods.'''

    ingress_enabled: bool = False
    '''Create an Ingress for external access to the Trino UI and JDBC endpoint.'''

    ingress_domain: Optional[str] = None
    '''Base domain. Creates trino.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''

    ingress_annotations: Optional[dict] = None
    '''Extra Ingress annotations.'''

    autoscaling: Optional[TrinoAutoscalingArgs] = None
    '''HPA configuration for workers. None disables autoscaling.'''

    catalogs: List[TrinoIcebergCatalogArgs] = field(default_factory=list)
    '''Iceberg REST catalogs to register, each backed by a Polaris endpoint.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Trino(pulumi.ComponentResource):
    '''
    Deploys Trino to Kubernetes using the official trinodb/trino Helm chart.

    Pass catalogs directly via TrinoArgs.catalogs — credentials are sourced from
    K8s Secrets via Trino's ${ENV:VAR} syntax, never embedded in chart values.

    Outputs:
        namespace — Kubernetes namespace
        endpoint  — Internal cluster URL (http://<release>.<ns>.svc.cluster.local:8080)
        ui_url    — External URL if ingress enabled, else same as endpoint

    Example:
        trino = Trino('trino', TrinoArgs(
            namespace=ns.metadata.name,
            workers=2,
            catalogs=[
                TrinoIcebergCatalogArgs(
                    name='bronze',
                    polaris_endpoint=polaris.endpoint,
                    warehouse='bronze',
                    credentials_secret='polaris-trino-credentials',
                    s3_endpoint=minio.endpoint,
                    s3_access_key='minioadmin',
                    s3_secret_key=minio_password,
                ),
            ],
        ))
    '''

    def __init__(
        self,
        name: str,
        args: TrinoArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:trino:Trino', name, {}, opts)

        args = args or TrinoArgs()
        self._release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)

        # Build the static portion of the Helm values synchronously.
        v = json.loads((CONFIG_DIR / 'helm/helm_values_trino.json').read_text())
        v['fullnameOverride']                  = self._release_name
        v['server']['workers']                 = args.workers
        v['coordinator']['jvm']['maxHeapSize'] = args.coordinator_heap
        v['coordinator']['resources']          = args.coordinator_resources
        v['worker']['jvm']['maxHeapSize']      = args.worker_heap
        v['worker']['resources']               = args.worker_resources

        if args.autoscaling:
            a = args.autoscaling
            hpa = {
                'enabled':                        a.enabled,
                'minReplicas':                    a.min_replicas,
                'maxReplicas':                    a.max_replicas,
                'targetCPUUtilizationPercentage': a.target_cpu_utilization,
            }
            if a.target_memory_utilization is not None:
                hpa['targetMemoryUtilizationPercentage'] = a.target_memory_utilization
            v['worker']['autoscaling'] = hpa

        if args.ingress_enabled and args.ingress_domain:
            v['ingress'] = {
                'enabled':     True,
                'className':   args.ingress_class_name,
                'annotations': args.ingress_annotations or {},
                'hosts': [{'host': f'trino.{args.ingress_domain}', 'paths': [{'path': '/', 'pathType': 'ImplementationSpecific'}]}],
                'tls': [],
            }

        v = _deep_merge(v, args.extra_values)

        if not args.catalogs:
            values = Output.from_input(v)
        else:
            # Build each catalog's .properties content as an Output[str], resolving
            # any Pulumi Outputs (endpoints, keys) per catalog independently.
            env_from_secrets = []
            catalog_outputs  = {}
            for cat in args.catalogs:
                if cat.credentials_secret:
                    pfx  = f'TRINO_{cat.name.upper()}'
                    cred = Output.from_input(f'${{ENV:{pfx}_CLIENT_ID}}:${{ENV:{pfx}_CLIENT_SECRET}}')
                    env_from_secrets.append({'secretRef': {'name': cat.credentials_secret}, 'prefix': f'{pfx}_'})
                else:
                    cred = Output.all(
                        cid=Output.from_input(cat.client_id),
                        sec=Output.from_input(cat.client_secret),
                    ).apply(lambda r: f'{r["cid"]}:{r["sec"]}')

                catalog_outputs[cat.name] = Output.all(
                    ep=cat.polaris_endpoint,
                    s3ep=cat.s3_endpoint,
                    s3k=cat.s3_access_key,
                    s3s=cat.s3_secret_key,
                    cred=cred,
                ).apply(lambda r, cat=cat: '\n'.join([
                    'connector.name=iceberg',
                    'iceberg.catalog.type=rest',
                    f'iceberg.rest-catalog.uri={r["ep"]}/api/catalog',
                    f'iceberg.rest-catalog.warehouse={cat.warehouse}',
                    'iceberg.rest-catalog.security=OAUTH2',
                    f'iceberg.rest-catalog.oauth2.credential={r["cred"]}',
                    'fs.native-s3.enabled=true',
                    f's3.endpoint={r["s3ep"]}',
                    f's3.path-style-access={str(cat.s3_path_style_access).lower()}',
                    f's3.aws-access-key={r["s3k"]}',
                    f's3.aws-secret-key={r["s3s"]}',
                    f's3.region={cat.s3_region}',
                ]) + '\n')

            # Merge resolved catalog configs into the values dict.
            values = Output.all(**catalog_outputs).apply(lambda catalogs: {
                **v,
                'catalogs': catalogs,
                **({'envFrom': (v.get('envFrom') or []) + env_from_secrets} if env_from_secrets else {}),
            })

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='trino',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://trinodb.github.io/charts'),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace
        self.endpoint = Output.concat(
            'http://', self._release_name, '.', self._namespace,
            '.svc.cluster.local:8080',
        )
        self.ui_url = (
            Output.from_input(f'http://trino.{args.ingress_domain}')
            if args.ingress_enabled and args.ingress_domain
            else self.endpoint
        )

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint':  self.endpoint,
            'ui_url':    self.ui_url,
        })
