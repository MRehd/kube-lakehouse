'''A Kubernetes Python Pulumi program for a data lakehouse.'''

import pulumi
from pulumi_kubernetes.core.v1 import Namespace
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

from minio import BucketArgs, Minio, MinioArgs

# Get project name from Pulumi.yaml
project_name = pulumi.get_project()
env = pulumi.get_stack()

# Load configuration from stack file
config = pulumi.Config()
minio_root_user = config.require('minio_root_user')
minio_root_password = config.require_secret('minio_root_password')
node = config.get('node')
domain = config.require('domain')

# Create namespace for the lakehouse
ns_name = f'ns-{project_name}-{env}'
ns = Namespace(ns_name, metadata={'name': ns_name})

# Deploy NGINX Ingress Controller
ingress_name = f'inginx-{project_name}-{env}'
ingress_nginx = Chart(
    ingress_name,
    ChartOpts(
        chart='ingress-nginx',
        version='4.10.0',
        namespace=ns_name,
        fetch_opts=FetchOpts(
            repo='https://kubernetes.github.io/ingress-nginx',
        ),
        values={
            'controller': {
                'service': {
                    'type': 'LoadBalancer',  # Use NodePort for local clusters without LB
                },
            },
        },
    ),
)

# Node selector for MinIO pod scheduling
node_selector = {'kubernetes.io/hostname': node}

# Deploy MinIO for object storage
minio_name = f'minio-{project_name}-{env}'
minio = Minio(minio_name, MinioArgs(
    namespace=ns_name,
    release_name=minio_name,
    persistence_size='10Gi',
    root_user=minio_root_user,
    root_password=minio_root_password,
    node_selector=node_selector,
    ingress_enabled=True,
    ingress_domain=domain,
    ingress_class_name='nginx',
), opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx]))

# Create lakehouse buckets (medallion architecture)
minio.create_buckets('lakehouse-buckets', [
    BucketArgs(name='bronze', versioning=True),
    BucketArgs(name='silver', versioning=True),
    BucketArgs(name='gold', versioning=True),
])

pulumi.export('minio_endpoint', minio.endpoint)
pulumi.export('minio_console', minio.console_endpoint)
pulumi.export('minio_url', minio.api_url)
pulumi.export('minio_console_url', minio.console_url)
pulumi.export('environment', env)
