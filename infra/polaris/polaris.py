'''Reusable Apache Polaris component for Kubernetes using Helm charts.'''

import json
from dataclasses import dataclass, field
from typing import List, Optional

import pulumi
from pulumi_kubernetes.batch.v1 import Job
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts
from pulumi_kubernetes.networking.v1 import Ingress


@dataclass
class CatalogArgs:
    '''Configuration for an Apache Polaris catalog backed by S3/MinIO storage.'''

    name: str
    '''Name of the catalog to create.'''

    s3_endpoint: str
    '''S3/MinIO endpoint URL (e.g., 'http://minio.namespace.svc.cluster.local:9000').'''

    s3_bucket: str
    '''S3/MinIO bucket name for the catalog's warehouse location.'''

    s3_access_key: str
    '''S3/MinIO access key ID.'''

    s3_secret_key: str
    '''S3/MinIO secret access key.'''

    s3_path_style_access: bool = True
    '''Use path-style access (required for MinIO).'''

    s3_region: str = 'us-east-1'
    '''S3 region (can be any value for MinIO).'''

    realm: str = 'POLARIS'
    '''Polaris realm to create the catalog in.'''

    catalog_type: str = 'INTERNAL'
    '''Catalog type: INTERNAL or EXTERNAL.'''

    default_base_location: Optional[str] = None
    '''Default base location for tables. If not set, uses s3://<bucket>/.'''


@dataclass
class PolarisArgs:
    '''Configuration arguments for Apache Polaris deployment.'''

    namespace: str = 'polaris'
    '''Kubernetes namespace to deploy Polaris into (must already exist).'''

    persistence_type: str = 'relational-jdbc'
    '''Persistence type: 'in-memory' or 'relational-jdbc'.'''

    persistence_secret_name: Optional[str] = None
    '''Name of the Kubernetes secret containing database credentials.'''

    persistence_secret_username_key: str = 'username'
    '''Key in the secret for the database username.'''

    persistence_secret_password_key: str = 'password'
    '''Key in the secret for the database password.'''

    persistence_secret_jdbc_url_key: str = 'jdbcUrl'
    '''Key in the secret for the JDBC URL.'''

    realms: List[str] = field(default_factory=lambda: ['POLARIS'])
    '''List of valid realms. The first realm is the default.'''

    replica_count: int = 1
    '''Number of Polaris replicas to deploy.'''

    service_type: str = 'LoadBalancer'
    '''Kubernetes service type: ClusterIP, NodePort, or LoadBalancer.'''

    service_port: int = 8181
    '''Port for the Polaris API service.'''

    management_port: int = 8182
    '''Port for the Polaris management service (health checks, metrics).'''

    image_repository: str = 'apache/polaris'
    '''Docker image repository for Polaris.'''

    image_tag: str = 'latest'
    '''Docker image tag for Polaris.'''

    image_pull_policy: str = 'IfNotPresent'
    '''Image pull policy: Always, IfNotPresent, or Never.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '512Mi', 'cpu': '250m'},
        'limits': {'memory': '1Gi', 'cpu': '500m'},
    })
    '''Resource requests and limits for Polaris pods.'''

    chart_version: str = '1.3.0-incubating'
    '''Version of the Polaris Helm chart to deploy.'''

    chart_repo: str = 'https://downloads.apache.org/polaris/helm-chart/'
    '''Helm chart repository URL.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix (usually 'cluster.local').'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    ingress_enabled: bool = True
    '''Enable Ingress for external access.'''

    ingress_domain: Optional[str] = None
    '''Domain for Ingress (e.g., 'k8lh.local'). Creates polaris.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name (e.g., 'nginx', 'traefik').'''

    ingress_annotations: Optional[dict] = None
    '''Additional annotations for the Ingress resource.'''

    metrics_enabled: bool = True
    '''Enable metrics collection for Polaris.'''

    logging_level: str = 'INFO'
    '''Root logging level for Polaris.'''

    logging_console_json: bool = False
    '''Enable JSON format for console logs.'''

    autoscaling_enabled: bool = True
    '''Enable horizontal pod autoscaler.'''

    autoscaling_min_replicas: int = 1
    '''Minimum replicas for autoscaling.'''

    autoscaling_max_replicas: int = 2
    '''Maximum replicas for autoscaling.'''


class Polaris(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying Apache Polaris to Kubernetes using Helm.

    Apache Polaris is an open-source, fully-featured catalog for Apache Iceberg.
    It implements the Iceberg REST catalog API and provides a centralized place 
    to manage Iceberg tables across multiple query engines.

    Example:
        ```python
        from polaris import Polaris, PolarisArgs

        polaris = Polaris('my-polaris', PolarisArgs(
            namespace='data',
            persistence_type='relational-jdbc',
            persistence_secret_name='polaris-db-creds',
            persistence_jdbc_url='jdbc:postgresql://postgres:5432/polaris',
        ))
        ```
    '''

    def __init__(
        self,
        name: str,
        args: PolarisArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:polaris:Polaris', name, {}, opts)

        args = args or PolarisArgs()
        self._args = args
        self._name = name
        # Helm release name determines K8s resource names
        self._release_name = args.release_name or name
        # Bootstrap credentials (set by create_bootstrap, used by create_catalogs)
        self._root_client_id: str = 'root'
        self._root_client_secret: pulumi.Input[str] = 'root'

        # Build Helm values from args
        values = self._build_values(args)

        # Deploy Polaris using Helm chart
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='polaris',
                version=args.chart_version,
                namespace=args.namespace,
                fetch_opts=FetchOpts(
                    repo=args.chart_repo,
                ),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        # Export useful outputs
        self.namespace = pulumi.Output.from_input(args.namespace)
        self.host = pulumi.Output.concat(
            self._release_name, '.', args.namespace,
            '.svc.', args.cluster_domain
        )
        self.endpoint = pulumi.Output.concat(
            'http://', self.host, ':', str(args.service_port)
        )
        self.management_endpoint = pulumi.Output.concat(
            'http://', self._release_name, '.', args.namespace,
            '.svc.', args.cluster_domain, ':', str(args.management_port)
        )

        # Create Ingress if enabled
        self.ingress = None
        if args.ingress_enabled and args.ingress_domain:
            self.api_host = f'polaris.{args.ingress_domain}'
            self.ingress = self._create_ingress(args)
            self.api_url = pulumi.Output.from_input(f'http://{self.api_host}')
        else:
            self.api_url = self.endpoint

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint': self.endpoint,
            'management_endpoint': self.management_endpoint,
            'api_url': self.api_url,
            'host': self.host,
        })

    def _create_ingress(self, args: PolarisArgs) -> Ingress:
        '''Create Ingress for Polaris API access.'''
        annotations = {
            'nginx.ingress.kubernetes.io/proxy-body-size': '0',
            'nginx.ingress.kubernetes.io/proxy-read-timeout': '600',
            'nginx.ingress.kubernetes.io/proxy-send-timeout': '600',
        }
        if args.ingress_annotations:
            annotations.update(args.ingress_annotations)

        return Ingress(
            f'{self._release_name}-ingress',
            metadata={
                'namespace': args.namespace,
                'annotations': annotations,
            },
            spec={
                'ingressClassName': args.ingress_class_name,
                'rules': [
                    {
                        'host': self.api_host,
                        'http': {
                            'paths': [{
                                'path': '/',
                                'pathType': 'Prefix',
                                'backend': {
                                    'service': {
                                        'name': self._release_name,
                                        'port': {'number': args.service_port},
                                    },
                                },
                            }],
                        },
                    },
                ],
            },
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.chart]),
        )

    def _build_values(self, args: PolarisArgs) -> dict:
        '''Build Helm chart values from PolarisArgs.'''
        values = {
            'fullnameOverride': self._release_name,
            'replicaCount': args.replica_count,
            'image': {
                'repository': args.image_repository,
                'tag': args.image_tag,
                'pullPolicy': args.image_pull_policy,
            },
            'service': {
                'type': args.service_type,
                'ports': [{
                    'name': 'polaris-http',
                    'port': args.service_port,
                }],
            },
            'managementService': {
                'ports': [{
                    'name': 'polaris-mgmt',
                    'port': args.management_port,
                }],
            },
            'resources': args.resources,
            'realmContext': {
                'type': 'default',
                'realms': args.realms,
            },
            'metrics': {
                'enabled': args.metrics_enabled,
            },
            'logging': {
                'level': args.logging_level,
                'console': {
                    'json': args.logging_console_json,
                },
            },
            'autoscaling': {
                'enabled': args.autoscaling_enabled,
                'minReplicas': args.autoscaling_min_replicas,
                'maxReplicas': args.autoscaling_max_replicas,
            },
        }

        # Configure persistence
        if args.persistence_type == 'relational-jdbc':
            persistence_config = {
                'type': 'relational-jdbc',
                'relationalJdbc': {
                    'secret': {
                        'name': args.persistence_secret_name,
                        'username': args.persistence_secret_username_key,
                        'password': args.persistence_secret_password_key,
                        'jdbcUrl': args.persistence_secret_jdbc_url_key,
                    },
                },
            }
            values['persistence'] = persistence_config
        else:
            values['persistence'] = {'type': 'in-memory'}

        # Merge extra values (allowing overrides)
        values = self._deep_merge(values, args.extra_values)

        return values

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        '''Deep merge two dictionaries, with override taking precedence.'''
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Polaris._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def create_bootstrap(
        self,
        name: str,
        root_client_id: str = 'root',
        root_client_secret: pulumi.Input[str] = 'root',
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Bootstrap Polaris by creating the database schema and initial principal credentials.

        This must be run before any catalogs can be created. The bootstrap job runs
        the polaris-admin-tool to:
        1. Create the polaris_schema in the database
        2. Create the root principal with the specified credentials

        Args:
            name: Unique name for the Pulumi resource.
            root_client_id: Client ID for the root principal (default: 'root').
            root_client_secret: Client secret for the root principal (default: 'root').
            opts: Optional Pulumi resource options.

        Returns:
            The Kubernetes Job resource that bootstraps Polaris.

        Example:
            ```python
            polaris = Polaris('my-polaris', PolarisArgs(
                namespace='data',
                persistence_type='relational-jdbc',
                persistence_secret_name='polaris-db-creds',
            ))
            
            # Bootstrap must run before creating catalogs
            bootstrap = polaris.create_bootstrap('polaris-bootstrap')
            
            polaris.create_catalogs('catalogs', [...],
                opts=pulumi.ResourceOptions(depends_on=[bootstrap]))
            ```
        '''
        args = self._args

        # Store credentials for use by create_catalogs
        self._root_client_id = root_client_id
        self._root_client_secret = root_client_secret

        # Build the bootstrap arguments
        realm = args.realms[0] if args.realms else 'POLARIS'

        # Create credential argument by resolving the secret
        credential_arg = pulumi.Output.from_input(root_client_secret).apply(
            lambda secret: f'{realm},{root_client_id},{secret}'
        )

        job_opts = pulumi.ResourceOptions(
            parent=self,
            depends_on=[self.chart],
        )
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-bootstrap-job',
            metadata={
                'namespace': args.namespace,
                'labels': {'app': 'polaris-bootstrap'},
            },
            spec={
                # Auto-delete the Job 5 minutes after it completes
                'ttlSecondsAfterFinished': 300,
                # Retry up to 3 times if the Job fails
                'backoffLimit': 3,
                'template': {
                    'spec': {
                        'restartPolicy': 'OnFailure',
                        'containers': [
                            {
                                'name': 'polaris-bootstrap',
                                'image': f'apache/polaris-admin-tool:{args.image_tag}',
                                # The admin-tool image has an entrypoint, just pass args
                                'args': [
                                    'bootstrap',
                                    '-r', realm,
                                    '-c', credential_arg,
                                    '-p',
                                ],
                                'env': [
                                    # Persistence type
                                    {
                                        'name': 'POLARIS_PERSISTENCE_TYPE',
                                        'value': args.persistence_type,
                                    },
                                    # Database credentials from secret
                                    {
                                        'name': 'QUARKUS_DATASOURCE_USERNAME',
                                        'valueFrom': {
                                            'secretKeyRef': {
                                                'name': args.persistence_secret_name,
                                                'key': args.persistence_secret_username_key,
                                            },
                                        },
                                    },
                                    {
                                        'name': 'QUARKUS_DATASOURCE_PASSWORD',
                                        'valueFrom': {
                                            'secretKeyRef': {
                                                'name': args.persistence_secret_name,
                                                'key': args.persistence_secret_password_key,
                                            },
                                        },
                                    },
                                    {
                                        'name': 'QUARKUS_DATASOURCE_JDBC_URL',
                                        'valueFrom': {
                                            'secretKeyRef': {
                                                'name': args.persistence_secret_name,
                                                'key': args.persistence_secret_jdbc_url_key,
                                            },
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                },
            },
            opts=job_opts,
        )