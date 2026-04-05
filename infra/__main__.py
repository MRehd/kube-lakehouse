'''
Kubernetes Data Lakehouse Infrastructure
=========================================

A Pulumi program that deploys a complete data lakehouse stack on Kubernetes:

Architecture:
    - MinIO: S3-compatible object storage for data lake files
    - PostgreSQL: Metadata store for Polaris and Airflow
    - Apache Polaris: Iceberg REST catalog for table management
    - NGINX Ingress: External access to services

Data Organization (Medallion Architecture):
    - Bronze: Raw ingested data
    - Silver: Cleaned and transformed data
    - Gold: Business-ready aggregated data

Usage:
    pulumi up -s dev
'''

# =============================================================================
# IMPORTS
# =============================================================================

import pulumi
from pulumi_kubernetes.core.v1 import Namespace
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

from minio import BucketArgs, Minio, MinioArgs
from polaris import CatalogArgs, Polaris, PolarisArgs
from psql import DatabaseArgs, GrantArgs, Psql, PsqlArgs, UserArgs
from secrets import LakehouseSecrets, SecretArgs


# =============================================================================
# CONFIGURATION
# =============================================================================

# Project and environment identifiers
project_name = pulumi.get_project()
env = pulumi.get_stack()

# Load stack configuration values
config = pulumi.Config()
domain = config.require('domain')

# Service credentials loaded from Pulumi config (secrets are encrypted)
# These are used to create Kubernetes secrets for each service
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
}

# Polaris credentials are handled separately because the secret
# requires a JDBC URL that depends on the PostgreSQL host
polaris_creds = {
    'user': config.require('polaris_postgres_user'),
    'password': config.require_secret('polaris_postgres_password'),
}


# =============================================================================
# NAMESPACE & SECRETS
# =============================================================================

# Create a dedicated namespace for all lakehouse resources
ns_name = f'ns-{project_name}-{env}'
ns = Namespace(ns_name, metadata={'name': ns_name})

# Create Kubernetes secrets for service authentication
# Secrets are created upfront so Helm charts can reference them via existingSecret
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


# =============================================================================
# INGRESS CONTROLLER
# =============================================================================

# Deploy NGINX Ingress Controller for external access to services
# Services will be accessible at: <service>.<domain> (e.g., minio.k8lh.local)
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
                    'type': 'LoadBalancer',
                },
            },
        },
    ),
)


# =============================================================================
# MINIO - OBJECT STORAGE
# =============================================================================

# Deploy MinIO as S3-compatible object storage for the data lake
# Provides the storage layer for Iceberg tables managed by Polaris
minio_name = f'minio-{project_name}-{env}'
minio = Minio(
    minio_name,
    MinioArgs(
        namespace=ns.metadata.name,
        release_name=minio_name,
        mode='standalone',
        replicas=1,
        persistence_size='10Gi',
        root_user=credentials['minio']['user'],
        root_password=credentials['minio']['password'],
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx]),
)

# Create buckets for the medallion architecture data layers
# Each layer represents a different stage of data processing
minio.create_buckets(
    f'bucket-{project_name}-{env}-',
    [
        BucketArgs(name='bronze', versioning=True),   # Raw data
        BucketArgs(name='silver', versioning=True),   # Cleaned data
        BucketArgs(name='gold', versioning=True),     # Aggregated data
    ],
)


# =============================================================================
# POSTGRESQL - METADATA STORE
# =============================================================================

# Deploy PostgreSQL as the metadata store for Polaris and Airflow
# Uses Bitnami Helm chart with persistent storage
psql_name = f'psql-{project_name}-{env}'
psql = Psql(
    psql_name,
    PsqlArgs(
        namespace=ns.metadata.name,
        release_name=psql_name,
        existing_secret='postgres',
        service_type='LoadBalancer',
    ),
    opts=pulumi.ResourceOptions(depends_on=[lakehouse_secrets]),
)

# Database names for each service
airflow_db = 'airflow'
polaris_db = 'polaris'

# Database specifications: each service gets its own database and user
db_specs = [
    {
        'db': DatabaseArgs(name=airflow_db, owner=credentials['airflow']['user']),
        'users': [
            UserArgs(
                name=credentials['airflow']['user'],
                password=credentials['airflow']['password'],
                login=True,
                superuser=False,
            ),
        ],
    },
    {
        'db': DatabaseArgs(name=polaris_db, owner=polaris_creds['user']),
        'users': [
            UserArgs(
                name=polaris_creds['user'],
                password=polaris_creds['password'],
                login=True,
                superuser=False,
            ),
        ],
    },
]

# Create users and databases for each service
db = {}
for spec in db_specs:
    db_name = spec['db'].name

    # Create database users first
    user_list = [
        psql.create_users(f'psqluser-{db_name}-{user.name}', user)
        for user in spec['users']
    ]

    # Create database after users exist
    db[db_name] = {
        'users': user_list,
        'instance': psql.create_databases(
            f'psqldb-{db_name}',
            spec['db'],
            opts=pulumi.ResourceOptions(depends_on=user_list),
        ),
    }


# =============================================================================
# APACHE POLARIS - ICEBERG CATALOG
# =============================================================================

# Create Polaris secret with JDBC URL for PostgreSQL connection
# This secret is separate because it requires the dynamically resolved psql.host
polaris_jdbc_url = pulumi.Output.concat(
    'jdbc:postgresql://', psql.host, ':5432/', polaris_db
)
polaris_secret = LakehouseSecrets(
    f'secrets-polaris-{project_name}-{env}',
    ns.metadata.name,
    [
        SecretArgs(
            name='polaris',
            data={
                'username': polaris_creds['user'],
                'password': polaris_creds['password'],
                'jdbcUrl': polaris_jdbc_url,
            },
        ),
    ],
    opts=pulumi.ResourceOptions(depends_on=[ns, psql]),
)

# Deploy Apache Polaris as the Iceberg REST catalog
# Polaris manages Iceberg table metadata and provides a REST API for table operations
polaris_name = f'polaris-{project_name}-{env}'
polaris = Polaris(
    polaris_name,
    PolarisArgs(
        namespace=ns.metadata.name,
        release_name=polaris_name,
        persistence_type='relational-jdbc',
        persistence_secret_name='polaris',
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
    ),
    opts=pulumi.ResourceOptions(depends_on=[db[polaris_db]['instance'], polaris_secret]),
)

# Bootstrap Polaris: creates database schema and root principal credentials
# This is idempotent - if schema exists, it skips bootstrap
polaris_bootstrap = polaris.create_bootstrap(
    f'bootstrap-{project_name}-{env}',
    root_client_id='root',
    root_client_secret='root',  # TODO: Use a secret from Pulumi config in production
)

# Create Polaris catalogs for each medallion layer
# Each catalog is backed by a MinIO bucket for S3 storage
polaris.create_catalogs(
    f'catalogs-{project_name}-{env}',
    [
        CatalogArgs(
            name='bronze',
            s3_endpoint=minio.endpoint,
            s3_bucket='bronze',
            s3_access_key=credentials['minio']['user'],
            s3_secret_key=credentials['minio']['password'],
        ),
        CatalogArgs(
            name='silver',
            s3_endpoint=minio.endpoint,
            s3_bucket='silver',
            s3_access_key=credentials['minio']['user'],
            s3_secret_key=credentials['minio']['password'],
        ),
        CatalogArgs(
            name='gold',
            s3_endpoint=minio.endpoint,
            s3_bucket='gold',
            s3_access_key=credentials['minio']['user'],
            s3_secret_key=credentials['minio']['password'],
        ),
    ],
    opts=pulumi.ResourceOptions(depends_on=[polaris_bootstrap]),
)


# =============================================================================
# STACK EXPORTS
# =============================================================================

# Export service endpoints for external access and integration
pulumi.export('environment', env)

# MinIO endpoints
pulumi.export('minio_endpoint', minio.endpoint)
pulumi.export('minio_console', minio.console_endpoint)
pulumi.export('minio_url', minio.api_url)
pulumi.export('minio_console_url', minio.console_url)

# Polaris endpoints
pulumi.export('polaris_endpoint', polaris.endpoint)
pulumi.export('polaris_url', polaris.api_url)
