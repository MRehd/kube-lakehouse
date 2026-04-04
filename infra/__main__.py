'''A Kubernetes Python Pulumi program for a data lakehouse.'''

import pulumi
from pulumi_kubernetes.core.v1 import Namespace
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

from minio import BucketArgs, Minio, MinioArgs
from psql import DatabaseArgs, GrantArgs, Psql, PsqlArgs, UserArgs
from secrets import LakehouseSecrets, SecretArgs


# Get project name from Pulumi.yaml
project_name = pulumi.get_project()
env = pulumi.get_stack()

# Load configuration from stack file
config = pulumi.Config()

minio_root_user = config.require('minio_root_user')
minio_root_password = config.require_secret('minio_root_password')

postgres_admin_user = config.require('postgres_admin_user')
postgres_admin_password = config.require_secret('postgres_admin_password')

airflow_postgres_user = config.require('airflow_postgres_user')
airflow_postgres_password = config.require_secret('airflow_postgres_password')

polaris_postgres_user = config.require('polaris_postgres_user')
polaris_postgres_password = config.require_secret('polaris_postgres_password')

domain = config.require('domain')

# Create namespace for the lakehouse
ns_name = f'ns-{project_name}-{env}'
ns = Namespace(ns_name, metadata={'name': ns_name})

# Credentials dictionary for all services
credentials = {
    'minio': {
        'user': config.require('minio_root_user'),
        'password': config.require_secret('minio_root_password'),
    },
    'postgres': {
        'user': config.require('postgres_admin_user'),
        'password': config.require_secret('postgres_admin_password'),
    },
    'airflow': {
        'user': config.require('airflow_postgres_user'),
        'password': config.require_secret('airflow_postgres_password'),
    },
    'polaris': {
        'user': config.require('polaris_postgres_user'),
        'password': config.require_secret('polaris_postgres_password'),
    },
}

# Create Kubernetes secrets from credentials dictionary
lakehouse_secrets = LakehouseSecrets(
    f'secrets-{project_name}-{env}',
    ns.metadata.name,
    [
        SecretArgs(
            name=service_name,
            data={'user': creds['user'], 'password': creds['password']},
        )
        for service_name, creds in credentials.items()
    ],
    opts=pulumi.ResourceOptions(depends_on=[ns]),
)

# Deploy NGINX Ingress Controller
ingress_name = f'inginx-{project_name}-{env}'
ingress_nginx = Chart(
    ingress_name,
    ChartOpts(
        chart='ingress-nginx',
        version='4.10.0',
        namespace=ns.metadata.name,
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

# Deploy MinIO for object storage
minio_name = f'minio-{project_name}-{env}'
minio = Minio(minio_name, MinioArgs(
    namespace=ns.metadata.name,
    release_name=minio_name,
    mode='standalone',
    replicas=1,
    persistence_size='10Gi',
    root_user=minio_root_user,
    root_password=minio_root_password,
    ingress_enabled=True,
    ingress_domain=domain,
    ingress_class_name='nginx',
), opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx]))

# Create lakehouse buckets (medallion architecture)
minio.create_buckets(
    f'bucket-{project_name}-{env}-', 
    [
        BucketArgs(name='bronze', versioning=True),
        BucketArgs(name='silver', versioning=True),
        BucketArgs(name='gold', versioning=True),
    ]
)

# Create Psql instance for metadata management
psql_name = f'psql-{project_name}-{env}'
psql = Psql(
  psql_name, 
  PsqlArgs(
    namespace=ns.metadata.name,
    release_name=psql_name,
    existing_secret='postgres',
  ),
  opts=pulumi.ResourceOptions(depends_on=[lakehouse_secrets]),
)

# Create metadata database and users
airflow_db = 'airflow'
polaris_db = 'polaris'

db_specs = [
    {
        'db': DatabaseArgs(name=airflow_db, owner=airflow_postgres_user),
        'users': [
            UserArgs(
                name=airflow_postgres_user, 
                password=airflow_postgres_password, 
                login=True, 
                superuser=False,
            )
        ]
    },
    {
        'db': DatabaseArgs(name=polaris_db, owner=polaris_postgres_user),
        'users': [
            UserArgs(
                name=polaris_postgres_user, 
                password=polaris_postgres_password, 
                login=True, 
                superuser=False,
            )
        ]
    }
]

db = {}
for spec in db_specs:
    db_name = spec['db'].name
    user_list = []
    for user in spec['users']:
        user_list.append(
            psql.create_users(
                f'psqluser-{db_name}-{user.name}', 
                user
            )
        )
    db[db_name] = {
        'users': user_list,
        'instance': psql.create_databases(
            f'psqldb-{db_name}', 
            spec['db'],
            opts=pulumi.ResourceOptions(depends_on=user_list),
        )
    }


pulumi.export('minio_endpoint', minio.endpoint)
pulumi.export('minio_console', minio.console_endpoint)
pulumi.export('minio_url', minio.api_url)
pulumi.export('minio_console_url', minio.console_url)
pulumi.export('environment', env)
