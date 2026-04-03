'''A Kubernetes Python Pulumi program for a data lakehouse.'''

import pulumi
from pulumi_kubernetes.core.v1 import Namespace

from minio import BucketArgs, Minio, MinioArgs

# Get project name from Pulumi.yaml
project_name = pulumi.get_project()
env = pulumi.get_stack()

# Load configuration from stack file
config = pulumi.Config()
minio_root_user = config.require('minio_root_user')
minio_root_password = config.require_secret('minio_root_password')
minio_node = config.get('minio_node')

# Create namespace for the lakehouse
ns_name = f'ns-{project_name}-{env}'
ns = Namespace(ns_name, metadata={'name': ns_name})

# Node selector for MinIO pod scheduling
node_selector = {'kubernetes.io/hostname': minio_node}

# Deploy MinIO for object storage
minio = Minio('lakehouse', MinioArgs(
    namespace=ns_name,
    release_name=f'minio-{project_name}-{env}',
    persistence_size='10Gi',
    root_user=minio_root_user,
    root_password=minio_root_password,
    node_selector=node_selector,
), opts=pulumi.ResourceOptions(depends_on=[ns]))

# Create lakehouse buckets (medallion architecture)
minio.create_buckets('lakehouse-buckets', [
    BucketArgs(name='bronze', versioning=True),
    BucketArgs(name='silver', versioning=True),
    BucketArgs(name='gold', versioning=True),
])

pulumi.export('minio_endpoint', minio.endpoint)
pulumi.export('minio_console', minio.console_endpoint)
pulumi.export('environment', env)
