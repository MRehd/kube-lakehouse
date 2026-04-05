'''Reusable MinIO component for Kubernetes using Helm charts.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'
from pulumi_kubernetes.batch.v1 import Job
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts
from pulumi_kubernetes.networking.v1 import Ingress


@dataclass
class BucketArgs:
    '''Configuration for a MinIO bucket.'''

    name: str
    '''Name of the bucket to create.'''

    versioning: bool = False
    '''Enable versioning on the bucket.'''

    object_locking: bool = False
    '''Enable object locking on the bucket.'''

    quota: Optional[str] = None
    '''Bucket quota (e.g., '10GB', '1TB').'''

    policy: Optional[str] = None
    '''Bucket policy: 'none', 'download', 'upload', or 'public'.'''


@dataclass
class MinioArgs:
    '''Configuration arguments for MinIO deployment.'''

    namespace: str = 'minio'
    '''Kubernetes namespace to deploy MinIO into (must already exist).'''

    mode: str = 'standalone'
    '''Deployment mode: 'standalone' or 'distributed'.'''

    replicas: int = 1
    '''Number of MinIO replicas (only used in distributed mode).'''

    root_user: str = 'admin'
    '''MinIO root/admin username.'''

    root_password: Optional[str] = None
    '''MinIO root/admin password. If not provided, a random one is generated.'''

    persistence_enabled: bool = True
    '''Enable persistent storage for MinIO data.'''

    persistence_size: str = '10Gi'
    '''Size of the persistent volume for MinIO data.'''

    storage_class: Optional[str] = None
    '''Kubernetes storage class to use for persistence.'''

    service_type: str = 'ClusterIP'
    '''Kubernetes service type: ClusterIP, NodePort, or LoadBalancer.'''

    console_enabled: bool = True
    '''Enable MinIO Console (web UI).'''

    console_service_type: str = 'ClusterIP'
    '''Service type for the MinIO Console.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '512Mi', 'cpu': '250m'},
        'limits': {'memory': '1Gi', 'cpu': '500m'},
    })
    '''Resource requests and limits for MinIO pods.'''

    chart_version: str = '5.2.0'
    '''Version of the MinIO Helm chart to deploy.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix (usually 'cluster.local').'''

    api_port: int = 9000
    '''MinIO API port.'''

    console_port: int = 9001
    '''MinIO Console port.'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    ingress_enabled: bool = False
    '''Enable Ingress for external access.'''

    ingress_domain: Optional[str] = None
    '''Domain for Ingress (e.g., 'k8lh.local'). Creates minio.<domain> and minio-console.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name (e.g., 'nginx', 'traefik').'''

    ingress_annotations: Optional[dict] = None
    '''Additional annotations for the Ingress resource.'''


class Minio(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying MinIO to Kubernetes using Helm.

    Example:
        ```python
        from minio.minio import Minio, MinioArgs

        minio = Minio('my-minio', MinioArgs(
            namespace='data',
            persistence_size='50Gi',
            mode='distributed',
            replicas=4,
        ))
        ```
    '''

    def __init__(
        self,
        name: str,
        args: MinioArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:minio:Minio', name, {}, opts)

        args = args or MinioArgs()
        self._args = args
        self._name = name
        # Helm release name determines K8s resource names
        self._release_name = args.release_name or name

        # Build Helm values from args
        values = self._build_values(args)

        # Deploy MinIO using Helm chart
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='minio',
                version=args.chart_version,
                namespace=args.namespace,
                fetch_opts=FetchOpts(
                    repo='https://charts.min.io/',
                ),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        # Export useful outputs
        self.namespace = pulumi.Output.from_input(args.namespace)
        self.endpoint = pulumi.Output.concat(
            'http://', self._release_name, '.', args.namespace,
            '.svc.', args.cluster_domain, ':', str(args.api_port)
        )
        self.console_endpoint = pulumi.Output.concat(
            'http://', self._release_name, '-console.', args.namespace,
            '.svc.', args.cluster_domain, ':', str(args.console_port)
        )

        # Create Ingress if enabled
        self.ingress = None
        if args.ingress_enabled and args.ingress_domain:

            self.api_host = f'minio.{args.ingress_domain}'
            self.console_host = f'minio-console.{args.ingress_domain}'

            self.ingress = self._create_ingress(args)

            self.api_url = pulumi.Output.from_input(f'http://{self.api_host}')
            self.console_url = pulumi.Output.from_input(f'http://{self.console_host}')
        else:
            self.api_url = self.endpoint
            self.console_url = self.console_endpoint

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint': self.endpoint,
            'console_endpoint': self.console_endpoint,
            'api_url': self.api_url,
            'console_url': self.console_url,
        })

    def _create_ingress(self, args: MinioArgs) -> Ingress:
        '''Create Ingress for MinIO API and Console.'''
        annotations = {
            'nginx.ingress.kubernetes.io/proxy-body-size': '0',  # Unlimited upload size
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
                                        'port': {'number': args.api_port},
                                    },
                                },
                            }],
                        },
                    },
                    {
                        'host': self.console_host,
                        'http': {
                            'paths': [{
                                'path': '/',
                                'pathType': 'Prefix',
                                'backend': {
                                    'service': {
                                        'name': f'{self._release_name}-console',
                                        'port': {'number': args.console_port},
                                    },
                                },
                            }],
                        },
                    },
                ],
            },
            opts=pulumi.ResourceOptions(parent=self, depends_on=[self.chart]),
        )

    def _build_values(self, args: MinioArgs) -> dict:
        '''Build Helm chart values from MinioArgs.'''
        values = json.loads((CONFIG_DIR / 'helm/helm_values_minio.json').read_text())

        # Override with args
        values['fullnameOverride'] = self._release_name
        values['mode'] = args.mode
        values['rootUser'] = args.root_user
        values['replicas'] = args.replicas if args.mode == 'distributed' else 1
        values['persistence']['enabled'] = args.persistence_enabled
        values['persistence']['size'] = args.persistence_size
        values['service']['type'] = args.service_type
        values['consoleService']['type'] = args.console_service_type
        values['resources'] = args.resources

        # Add root password if provided
        if args.root_password:
            values['rootPassword'] = args.root_password

        # Add storage class if specified
        if args.storage_class:
            values['persistence']['storageClass'] = args.storage_class

        # Merge extra values (allowing overrides)
        values = self._deep_merge(values, args.extra_values)

        return values

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        '''Deep merge two dictionaries, with override taking precedence.'''
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Minio._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def create_buckets(
        self,
        name: str,
        buckets: BucketArgs | List[BucketArgs],
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create one or more MinIO buckets using a Kubernetes Job.

        Args:
            name: Unique name for the Pulumi resource.
            buckets: Single bucket or list of bucket configurations.
            opts: Optional Pulumi resource options.

        Returns:
            The Kubernetes Job resource that creates the buckets.

        Example:
            ```python
            minio = Minio('my-minio', MinioArgs(namespace='data'))
            
            # Single bucket
            minio.create_buckets('raw', BucketArgs(name='raw-data', versioning=True))
            
            # Multiple buckets
            minio.create_buckets('lakehouse-buckets', [
                BucketArgs(name='bronze', versioning=True),
                BucketArgs(name='silver'),
                BucketArgs(name='gold'),
            ])
            ```
        '''
        if isinstance(buckets, BucketArgs):
            buckets = [buckets]

        # Build the mc commands for each bucket
        commands = self._build_mc_commands(buckets)

        # Load job spec and configure
        spec = json.loads((CONFIG_DIR / 'jobs/mc_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['args'] = [commands]
        container['env'][0]['value'] = pulumi.Output.concat(
            'http://',
            self._args.root_user,
            ':',
            self._args.root_password or 'minioadmin',
            '@',
            self._release_name,
            '.',
            self._args.namespace,
            '.svc.',
            self._args.cluster_domain,
            ':',
            str(self._args.api_port),
        )

        job_opts = pulumi.ResourceOptions(
            parent=self,
            depends_on=[self.chart],
        )
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-bucket-job',
            metadata={
                'namespace': self._args.namespace,
                'labels': {'app': 'minio-bucket-provisioner'},
            },
            spec=spec,
            opts=job_opts,
        )

    def _build_mc_commands(self, buckets: List[BucketArgs]) -> str:
        '''Build the shell commands to create and configure buckets.'''
        scripts_dir = CONFIG_DIR / 'scripts'
        create_tpl = (scripts_dir / 'create_bucket.sh').read_text().strip()
        version_tpl = (scripts_dir / 'bucket_versioning.sh').read_text().strip()
        retention_tpl = (scripts_dir / 'bucket_retention.sh').read_text().strip()
        quota_tpl = (scripts_dir / 'bucket_quota.sh').read_text().strip()
        policy_tpl = (scripts_dir / 'bucket_policy.sh').read_text().strip()

        commands = ['sleep 5']  # Wait for MinIO to be ready

        for bucket in buckets:
            # Create the bucket
            commands.append(create_tpl.replace('{{NAME}}', bucket.name))

            # Enable versioning if requested
            if bucket.versioning:
                commands.append(version_tpl.replace('{{NAME}}', bucket.name))

            # Enable object locking if requested
            if bucket.object_locking:
                commands.append(retention_tpl.replace('{{NAME}}', bucket.name))

            # Set quota if specified
            if bucket.quota:
                commands.append(
                    quota_tpl.replace('{{NAME}}', bucket.name).replace('{{QUOTA}}', bucket.quota)
                )

            # Set policy if specified
            if bucket.policy:
                commands.append(
                    policy_tpl.replace('{{NAME}}', bucket.name).replace('{{POLICY}}', bucket.policy)
                )

        return ' && '.join(commands)
