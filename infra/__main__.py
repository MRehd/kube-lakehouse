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

from kafka import AutoscalingArgs, Kafka, KafkaArgs, TopicArgs
from kafka_ui import KafkaUi, KafkaUiArgs
from keda import Keda, KedaArgs, KafkaTrigger, ScaledObjectArgs, TriggerArgs
from minio import BucketArgs, Minio, MinioArgs
from polaris import CatalogArgs, CatalogGrantArgs, Polaris, PolarisArgs, PrincipalArgs, RoleArgs
from psql import DatabaseArgs, GrantArgs, Psql, PsqlArgs, UserArgs
from trino import Trino, TrinoArgs, TrinoAutoscalingArgs, TrinoIcebergCatalogArgs
from flink import Flink, FlinkArgs, FlinkIcebergCatalogArgs, FlinkJobArgs
from producer import Producer, ProducerArgs
from spark import Spark, SparkArgs
from airflow import Airflow, AirflowArgs, AirflowConnectionArgs
from secrets import LakehouseSecrets, SecretArgs
from service_accounts import PolicyRuleArgs, ServiceAccountArgs, ServiceAccounts, ServiceAccountsArgs


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
        'admin_password': config.require_secret('airflow_admin_password'),
        'fernet_key': config.require_secret('airflow_fernet_key'),
        'webserver_secret_key': config.require_secret('airflow_webserver_secret_key'),
        'git_token': config.require_secret('airflow_git_token'),
    },
}

airflow_git_repo   = config.require('airflow_git_repo')
airflow_git_branch = config.require('airflow_git_branch')

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
        mode='distributed',
        replicas=4,
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
        BucketArgs(name='bronze', versioning=True),      # Raw data
        BucketArgs(name='silver', versioning=True),      # Cleaned data
        BucketArgs(name='gold', versioning=True),        # Aggregated data
        BucketArgs(name='spark-logs', versioning=False), # Spark event logs for History Server
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
# SERVICE ACCOUNTS
# =============================================================================

sas = ServiceAccounts(
    f'sas-{project_name}-{env}',
    ServiceAccountsArgs(namespace=ns.metadata.name),
    opts=pulumi.ResourceOptions(depends_on=[ns]),
)

# SA for the Polaris principal provisioning job — needs get/create on Secrets
polaris_provisioner_sa = sas.provision(
    f'polaris-provisioner-{project_name}-{env}',
    ServiceAccountArgs(
        name='polaris-principal-provisioner',
        rules=[
            PolicyRuleArgs(resources=['secrets'], verbs=['get', 'create']),
        ],
    ),
)

# SA for Spark driver/executor pods — needs pod/service/configmap CRUD
spark_sa = sas.provision(
    f'spark-{project_name}-{env}',
    ServiceAccountArgs(
        name='spark',
        rules=[
            PolicyRuleArgs(
                resources=['pods', 'services', 'configmaps'],
                verbs=['create', 'get', 'list', 'watch', 'delete', 'patch', 'update'],
            ),
        ],
    ),
)


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

# Create RBAC roles for data access
# The 'data_engineer' role grants full access to create/modify schemas, tables, and data
polaris_roles = polaris.create_roles(
    f'roles-{project_name}-{env}',
    [
        RoleArgs(
            name='data_engineer',
            catalog_grants=[
                CatalogGrantArgs(catalog='bronze', role='catalog_admin'),
                CatalogGrantArgs(catalog='silver', role='catalog_admin'),
                CatalogGrantArgs(catalog='gold', role='catalog_admin'),
            ],
        ),
    ],
    opts=pulumi.ResourceOptions(depends_on=[polaris_bootstrap]),
)

# Service principals for compute engines
# Each principal is assigned the 'data_engineer' role for full catalog access
trino_credentials_secret = 'polaris-trino-credentials'
spark_credentials_secret = 'polaris-spark-credentials'
flink_credentials_secret = 'polaris-flink-credentials'

polaris_principals = polaris.create_principals(
    f'principals-{project_name}-{env}',
    [
        PrincipalArgs(name='spark', credentials_secret_name=spark_credentials_secret, roles=['data_engineer']),
        PrincipalArgs(name='flink', credentials_secret_name=flink_credentials_secret, roles=['data_engineer']),
        PrincipalArgs(name='trino', credentials_secret_name=trino_credentials_secret, roles=['data_engineer']),
    ],
    provisioner_sa_name=polaris_provisioner_sa.metadata.name,
    opts=pulumi.ResourceOptions(depends_on=[polaris_roles, polaris_provisioner_sa]),
)


# =============================================================================
# APACHE KAFKA - EVENT STREAMING
# =============================================================================

# Deploy Apache Kafka using Bitnami Helm chart for event streaming
# Used for real-time data ingestion and change data capture
kafka_name = f'kafka-{project_name}-{env}'
kafka = Kafka(
    kafka_name,
    KafkaArgs(
        namespace=ns.metadata.name,
        release_name=kafka_name,
        replicas=1,
        persistence_size='1Gi',
        autoscaling=AutoscalingArgs(
            enabled=True,
            min_replicas=1,
            max_replicas=2,
            target_cpu_utilization=70,
        ),
        topics=[
            TopicArgs(name='btc', partitions=2, replicas=1),
            TopicArgs(name='eth', partitions=2, replicas=1)
        ],
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx]),
)

# Deploy Kafka UI for cluster inspection and management
kafka_ui_name = f'kafka-ui-{project_name}-{env}'
kafka_ui = KafkaUi(
    kafka_ui_name,
    KafkaUiArgs(
        namespace=ns.metadata.name,
        release_name=kafka_ui_name,
        bootstrap_servers=kafka.bootstrap_servers,
        cluster_name=f'{project_name}-{env}',
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
    ),
    opts=pulumi.ResourceOptions(depends_on=[kafka]),
)


# =============================================================================
# KEDA - KUBERNETES EVENT-DRIVEN AUTOSCALING
# =============================================================================

# Deploy the KEDA operator for event-driven worker scaling.
# KEDA watches Kafka consumer group lag and adjusts worker replica counts.
keda_name = f'keda-{project_name}-{env}'
keda = Keda(
    keda_name,
    KedaArgs(
        namespace=ns.metadata.name,
        release_name=keda_name,
        operator_replicas=2,
        metrics_server_replicas=1,
        watch_namespace='',  # Watch all namespaces
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx]),
)

# Uncomment and adapt when adding compute workers:
#
# keda.create_scaled_object(
#     f'scaledobject-{project_name}-{env}-spark',
#     ScaledObjectArgs(
#         name='spark-worker-scaler',
#         target_name=f'spark-worker-{project_name}-{env}',
#         target_kind='Deployment',
#         min_replica_count=1,
#         max_replica_count=10,
#         triggers=[
#             KafkaTriggerArgs(
#                 bootstrap_servers=kafka.bootstrap_servers,
#                 consumer_group='spark-consumer-group',
#                 topic='btc',
#                 lag_threshold=10,
#             ),
#         ],
#     ),
#     opts=pulumi.ResourceOptions(depends_on=[kafka]),
# )
#
# keda.create_scaled_object(
#     f'scaledobject-{project_name}-{env}-flink',
#     ScaledObjectArgs(
#         name='flink-taskmanager-scaler',
#         target_name=f'flink-taskmanager-{project_name}-{env}',
#         target_kind='Deployment',
#         min_replica_count=1,
#         max_replica_count=10,
#         triggers=[
#             KafkaTriggerArgs(
#                 bootstrap_servers=kafka.bootstrap_servers,
#                 consumer_group='flink-consumer-group',
#                 topic='btc',
#                 lag_threshold=10,
#             ),
#         ],
#     ),
#     opts=pulumi.ResourceOptions(depends_on=[kafka]),
# )
#
# keda.create_scaled_object(
#     f'scaledobject-{project_name}-{env}-trino',
#     ScaledObjectArgs(
#         name='trino-worker-scaler',
#         target_name=f'trino-worker-{project_name}-{env}',
#         target_kind='Deployment',
#         min_replica_count=1,
#         max_replica_count=10,
#         triggers=[
#             KafkaTriggerArgs(
#                 bootstrap_servers=kafka.bootstrap_servers,
#                 consumer_group='trino-consumer-group',
#                 topic='btc',
#                 lag_threshold=10,
#             ),
#         ],
#     ),
#     opts=pulumi.ResourceOptions(depends_on=[kafka]),
# )


# =============================================================================
# TRINO - DISTRIBUTED SQL QUERY ENGINE
# =============================================================================
# TODO: Add authentication. Currently deployed with no auth (anyone can log in
#       with any username and no password). To enforce auth, set
#       server.config.authenticationType=PASSWORD and configure a password file
#       or LDAP via coordinator.additionalConfigFiles.

# Deploy Trino as the query layer over the Iceberg tables managed by Polaris.
# Each medallion catalog is registered so Trino can query bronze/silver/gold layers.
trino_name = f'trino-{project_name}-{env}'
trino = Trino(
    trino_name,
    TrinoArgs(
        namespace=ns.metadata.name,
        release_name=trino_name,
        workers=1,
        coordinator_heap='1G',
        worker_heap='1G',
        autoscaling=TrinoAutoscalingArgs(
            min_replicas=1,
            max_replicas=5,
            target_cpu_utilization=70,
            target_memory_utilization=80,
        ),
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
    ),
    opts=pulumi.ResourceOptions(depends_on=[polaris_bootstrap, minio, polaris_principals]),
)

trino.create_catalogs(
    f'catalogs-{project_name}-{env}',
    [
        TrinoIcebergCatalogArgs(
            name='bronze',
            polaris_endpoint=polaris.endpoint,
            warehouse='bronze',
            credentials_secret=trino_credentials_secret,
            s3_endpoint=minio.endpoint,
            s3_access_key=credentials['minio']['user'],
            s3_secret_key=credentials['minio']['password'],
        ),
        TrinoIcebergCatalogArgs(
            name='silver',
            polaris_endpoint=polaris.endpoint,
            warehouse='silver',
            credentials_secret=trino_credentials_secret,
            s3_endpoint=minio.endpoint,
            s3_access_key=credentials['minio']['user'],
            s3_secret_key=credentials['minio']['password'],
        ),
        TrinoIcebergCatalogArgs(
            name='gold',
            polaris_endpoint=polaris.endpoint,
            warehouse='gold',
            credentials_secret=trino_credentials_secret,
            s3_endpoint=minio.endpoint,
            s3_access_key=credentials['minio']['user'],
            s3_secret_key=credentials['minio']['password'],
        ),
    ],
)

# =============================================================================
# PRODUCER - KAFKA EVENT PRODUCER
# =============================================================================

producer_name = f'producer-{project_name}-{env}'
producer = Producer(
    producer_name,
    ProducerArgs(
        namespace=ns.metadata.name,
        image_name=config.require('docker_producer_image_name'),
        registry_username=config.require('docker_registry_username'),
        registry_password=config.require_secret('docker_registry_password'),
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx, kafka]),
)


# =============================================================================
# APACHE SPARK - BATCH PROCESSING
# =============================================================================

spark_name = f'spark-{project_name}-{env}'
spark = Spark(
    spark_name,
    SparkArgs(
        namespace=ns.metadata.name,
        release_name=spark_name,
        service_account_name=spark_sa.metadata.name,
        connect_master='k8s://https://kubernetes.default.svc:443',
        s3_endpoint=minio.endpoint,
        s3_access_key=credentials['minio']['user'],
        s3_secret_key=credentials['minio']['password'],
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx, minio, spark_sa, polaris_principals]),
)


# =============================================================================
# APACHE AIRFLOW - WORKFLOW ORCHESTRATION
# =============================================================================

airflow_metadata_secret = LakehouseSecrets(
    f'secrets-airflow-meta-{project_name}-{env}',
    ns.metadata.name,
    [
        SecretArgs(
            name='airflow-metadata',
            data={
                'connection': pulumi.Output.all(
                    user=credentials['airflow']['user'],
                    pw=credentials['airflow']['password'],
                    host=psql.host,
                ).apply(lambda v: f"postgresql+psycopg2://{v['user']}:{v['pw']}@{v['host']}:5432/{airflow_db}"),
            },
        ),
        SecretArgs(name='airflow-fernet',    data={'fernet-key':           credentials['airflow']['fernet_key']}),
        SecretArgs(name='airflow-webserver', data={'webserver-secret-key': credentials['airflow']['webserver_secret_key']}),
        SecretArgs(name='airflow-git-credentials', data={
            'GIT_SYNC_USERNAME':  'x-token',
            'GIT_SYNC_PASSWORD':  credentials['airflow']['git_token'],
            'GITSYNC_USERNAME':   'x-token',
            'GITSYNC_PASSWORD':   credentials['airflow']['git_token'],
        }),
    ],
    opts=pulumi.ResourceOptions(depends_on=[ns, psql]),
)

airflow_name = f'airflow-{project_name}-{env}'
airflow = Airflow(
    airflow_name,
    AirflowArgs(
        namespace=ns.metadata.name,
        release_name=airflow_name,
        admin_password=credentials['airflow']['admin_password'],
        db_metadata_secret='airflow-metadata',
        fernet_key_secret='airflow-fernet',
        webserver_secret_key_secret='airflow-webserver',
        git_repo=airflow_git_repo,
        git_branch=airflow_git_branch,
        git_credentials_secret='airflow-git-credentials',
        git_subpath='infra/airflow/dags',
        ingress_enabled=True,
        ingress_domain=domain,
        ingress_class_name='nginx',
        connections=[
            AirflowConnectionArgs(
                conn_id='spark_default',
                uri=spark.connect_server_url,
            ),
        ],
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns, ingress_nginx, db['airflow']['instance'], airflow_metadata_secret, spark]),
)


# =============================================================================
# APACHE FLINK - STREAM PROCESSING
# =============================================================================

flink_name = f'flink-{project_name}-{env}'
flink = Flink(
    flink_name,
    FlinkArgs(
        namespace=ns.metadata.name,
        release_name=flink_name,
    ),
    opts=pulumi.ResourceOptions(depends_on=[ns]),
)

# Jobs are submitted via flink.submit_job() once job images are built.
# Example:
#
# flink.submit_job(
#     f'job-{project_name}-{env}-btc-ingest',
#     FlinkJobArgs(
#         job_name='btc-kafka-to-iceberg',
#         image='my-registry/flink-btc-job',
#         python_script='/opt/flink/jobs/ingest.py',
#         parallelism=2,
#         credentials_secret=flink_credentials_secret,
#         iceberg_catalogs=[
#             FlinkIcebergCatalogArgs(
#                 name='bronze',
#                 polaris_endpoint=polaris.endpoint,
#                 warehouse='bronze',
#                 credentials_secret=flink_credentials_secret,
#                 s3_endpoint=minio.endpoint,
#                 s3_access_key=credentials['minio']['user'],
#                 s3_secret_key=credentials['minio']['password'],
#             ),
#         ],
#         autoscaling_enabled=True,
#         autoscaling_target_utilization=0.75,
#         autoscaling_metrics_window='5m',
#         autoscaling_stabilization_interval='1m',
#         extra_flink_config={
#             'restart-strategy.type': 'exponential-delay',
#             'restart-strategy.exponential-delay.initial-backoff': '1 s',
#             'restart-strategy.exponential-delay.max-backoff': '5 min',
#             'restart-strategy.exponential-delay.reset-backoff-threshold': '10 min',
#         },
#     ),
#     opts=pulumi.ResourceOptions(depends_on=[polaris_principals]),
# )


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

# Kafka endpoints
pulumi.export('kafka_bootstrap_servers', kafka.bootstrap_servers)
pulumi.export('kafka_bootstrap_endpoint', kafka.bootstrap_endpoint)

# Kafka UI
pulumi.export('kafka_ui_url', kafka_ui.ui_url)

# Trino
pulumi.export('trino_endpoint', trino.endpoint)
pulumi.export('trino_url', trino.ui_url)

# KEDA
pulumi.export('keda_namespace', keda.namespace)

# Producer
pulumi.export('producer_url', producer.url)

# Airflow
pulumi.export('airflow_url', airflow.ui_url)

# Spark
pulumi.export('spark_namespace', spark.namespace)
pulumi.export('spark_connect_server_url', spark.connect_server_url)
pulumi.export('spark_history_server_url', spark.history_server_url)

# Flink
pulumi.export('flink_namespace', flink.namespace)
