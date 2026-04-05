'''Reusable PostgreSQL component for Kubernetes using Helm charts.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi_kubernetes.batch.v1 import Job
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class UserArgs:
    '''Configuration for a PostgreSQL user.'''

    name: str
    '''Username to create.'''

    password: str
    '''Password for the user.'''

    superuser: bool = False
    '''Grant superuser privileges.'''

    createdb: bool = False
    '''Allow user to create databases.'''

    createrole: bool = False
    '''Allow user to create roles.'''

    login: bool = True
    '''Allow user to login (set False for group roles).'''

    connection_limit: int = -1
    '''Maximum concurrent connections (-1 for unlimited).'''

    valid_until: Optional[str] = None
    '''Password expiration date (e.g., '2025-12-31').'''

    grants: Optional[List['GrantArgs']] = None
    '''List of database/schema grants for this user.'''


@dataclass
class GrantArgs:
    '''Configuration for database/schema privileges.'''

    database: str
    '''Database to grant access to.'''

    privileges: List[str] = field(default_factory=lambda: ['SELECT'])
    '''Privileges to grant: SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, ALL.'''

    schemas: List[str] = field(default_factory=lambda: ['public'])
    '''Schemas to grant access to.'''

    tables: str = 'ALL TABLES'
    '''Tables to grant access to: 'ALL TABLES' or specific table name.'''

    grant_option: bool = False
    '''Allow user to grant these privileges to others.'''


@dataclass
class DatabaseArgs:
    '''Configuration for a PostgreSQL database.'''

    name: str
    '''Name of the database to create.'''

    owner: Optional[str] = None
    '''Database owner (defaults to postgres superuser).'''

    encoding: str = 'UTF8'
    '''Character encoding for the database.'''

    lc_collate: str = 'en_US.UTF-8'
    '''Collation order for the database.'''

    lc_ctype: str = 'en_US.UTF-8'
    '''Character classification for the database.'''

    template: str = 'template0'
    '''Template database to use when creating this database.'''

    extensions: Optional[List[str]] = None
    '''List of PostgreSQL extensions to enable (e.g., ['uuid-ossp', 'pgcrypto']).'''


@dataclass
class PsqlArgs:
    '''Configuration arguments for PostgreSQL deployment.'''

    namespace: str = 'postgresql'
    '''Kubernetes namespace to deploy PostgreSQL into (must already exist).'''

    architecture: str = 'standalone'
    '''Deployment architecture: 'standalone' or 'replication'.'''

    existing_secret: str = None
    '''Name of existing Kubernetes secret containing postgres-password key.'''

    database: str = 'postgres'
    '''Default database to create.'''

    persistence_enabled: bool = True
    '''Enable persistent storage for PostgreSQL data.'''

    persistence_size: str = '10Gi'
    '''Size of the persistent volume for PostgreSQL data.'''

    storage_class: str = 'hostpath'
    '''Kubernetes storage class to use for persistence (default: hostpath for Docker Desktop).'''

    service_type: str = 'ClusterIP'
    '''Kubernetes service type: ClusterIP, NodePort, or LoadBalancer.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '256Mi', 'cpu': '250m'},
        'limits': {'memory': '512Mi', 'cpu': '500m'},
    })
    '''Resource requests and limits for PostgreSQL pods.'''

    chart_version: str = '15.5.38'
    '''Version of the Bitnami PostgreSQL Helm chart to deploy.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix (usually 'cluster.local').'''

    port: int = 5432
    '''PostgreSQL port.'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    max_connections: int = 100
    '''Maximum number of concurrent connections.'''

    shared_buffers: str = '128MB'
    '''Amount of memory for shared buffers.'''


class Psql(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying PostgreSQL to Kubernetes using Helm.

    Example:
        ```python
        from psql import Psql, PsqlArgs

        psql = Psql('my-postgres', PsqlArgs(
            namespace='data',
            persistence_size='50Gi',
            existing_secret='postgres-secret',
        ))
        ```
    '''

    def __init__(
        self,
        name: str,
        args: PsqlArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:psql:Psql', name, {}, opts)

        args = args or PsqlArgs()
        self._args = args
        self._name = name
        # Helm release name determines K8s resource names
        self._release_name = args.release_name or name

        # Build Helm values from args
        values = self._build_values(args)

        # Deploy PostgreSQL using Bitnami Helm chart
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='postgresql',
                version=args.chart_version,
                namespace=args.namespace,
                fetch_opts=FetchOpts(
                    repo='https://charts.bitnami.com/bitnami',
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
            self.host, ':', str(args.port)
        )
        self.secret_name = pulumi.Output.from_input(args.existing_secret)

        self.register_outputs({
            'namespace': self.namespace,
            'host': self.host,
            'endpoint': self.endpoint,
            'secret_name': self.secret_name,
        })

    def _build_values(self, args: PsqlArgs) -> dict:
        '''Build Helm chart values from PsqlArgs.'''
        values = json.loads((CONFIG_DIR / 'helm/helm_values_psql.json').read_text())

        # Override with args
        values['fullnameOverride'] = self._release_name
        values['architecture'] = args.architecture
        values['primary']['persistence']['enabled'] = args.persistence_enabled
        values['primary']['persistence']['size'] = args.persistence_size
        values['primary']['persistence']['storageClass'] = args.storage_class
        values['primary']['resources'] = args.resources
        values['primary']['extendedConfiguration'] = f'''
max_connections = {args.max_connections}
shared_buffers = {args.shared_buffers}
'''
        values['primary']['service']['type'] = args.service_type
        values['primary']['service']['ports']['postgresql'] = args.port
        values['auth']['database'] = args.database
        values['auth']['existingSecret'] = args.existing_secret

        # Merge extra values (allowing overrides)
        values = self._deep_merge(values, args.extra_values)

        return values

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        '''Deep merge two dictionaries, with override taking precedence.'''
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Psql._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def create_databases(
        self,
        name: str,
        databases: DatabaseArgs | List[DatabaseArgs],
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create one or more PostgreSQL databases using a Kubernetes Job.

        Args:
            name: Unique name for the Pulumi resource.
            databases: Single database or list of database configurations.
            opts: Optional Pulumi resource options.

        Returns:
            The Kubernetes Job resource that creates the databases.

        Example:
            ```python
            psql = Psql('my-postgres', PsqlArgs(namespace='data'))
            
            # Single database
            psql.create_databases('app-db', DatabaseArgs(name='myapp'))
            
            # Multiple databases
            psql.create_databases('lakehouse-dbs', [
                DatabaseArgs(name='iceberg_catalog', extensions=['uuid-ossp']),
                DatabaseArgs(name='hive_metastore'),
                DatabaseArgs(name='airflow'),
            ])
            ```
        '''
        if isinstance(databases, DatabaseArgs):
            databases = [databases]

        # Build the psql commands for each database
        commands = self._build_psql_commands(databases)

        # Load job spec and configure
        spec = json.loads((CONFIG_DIR / 'jobs/psql_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['args'] = [commands]
        container['env'][0]['value'] = pulumi.Output.concat(
            self._release_name, '.', self._args.namespace,
            '.svc.', self._args.cluster_domain
        )
        container['env'][1]['value'] = str(self._args.port)
        container['env'][3]['valueFrom']['secretKeyRef']['name'] = self._args.existing_secret

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-db-job',
            metadata={'namespace': self._args.namespace, 'labels': {'app': 'postgres-db-provisioner'}},
            spec=spec,
            opts=job_opts,
        )

    def _build_psql_commands(self, databases: List[DatabaseArgs]) -> str:
        '''Build the shell commands to create and configure databases.'''
        commands = [
            'sleep 5',  # Wait for PostgreSQL to be ready
        ]

        for db in databases:
            # Check if database exists, create if not
            create_db_cmd = f"psql -tc \"SELECT 1 FROM pg_database WHERE datname = '{db.name}'\" | grep -q 1 || "
            create_db_cmd += f"createdb --encoding={db.encoding} --lc-collate={db.lc_collate} "
            create_db_cmd += f"--lc-ctype={db.lc_ctype} --template={db.template} "
            
            if db.owner:
                create_db_cmd += f"--owner={db.owner} "
            
            create_db_cmd += db.name

            commands.append(create_db_cmd)
            commands.append(f"echo 'Database {db.name} ready'")

            # Enable extensions if specified
            if db.extensions:
                for ext in db.extensions:
                    commands.append(
                        f"psql -d {db.name} -c 'CREATE EXTENSION IF NOT EXISTS \"{ext}\"'"
                    )

        return ' && '.join(commands)

    def create_users(
        self,
        name: str,
        users: UserArgs | List[UserArgs],
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create one or more PostgreSQL users with permissions using a Kubernetes Job.

        Args:
            name: Unique name for the Pulumi resource.
            users: Single user or list of user configurations.
            opts: Optional Pulumi resource options.

        Returns:
            The Kubernetes Job resource that creates the users.

        Example:
            ```python
            psql = Psql('my-postgres', PsqlArgs(namespace='data'))
            
            # Single user with full access
            psql.create_users('admin-user', UserArgs(
                name='admin',
                password='secret',
                superuser=True,
            ))
            
            # Multiple users with specific grants
            psql.create_users('app-users', [
                UserArgs(
                    name='app_reader',
                    password='reader_pass',
                    grants=[GrantArgs(database='myapp', privileges=['SELECT'])],
                ),
                UserArgs(
                    name='app_writer',
                    password='writer_pass',
                    grants=[GrantArgs(database='myapp', privileges=['SELECT', 'INSERT', 'UPDATE', 'DELETE'])],
                ),
            ])
            ```
        '''
        if isinstance(users, UserArgs):
            users = [users]

        # Build the psql commands for each user
        commands = self._build_user_commands(users)

        # Load job spec and configure
        spec = json.loads((CONFIG_DIR / 'jobs/psql_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['args'] = [commands]
        container['env'][0]['value'] = pulumi.Output.concat(
            self._release_name, '.', self._args.namespace,
            '.svc.', self._args.cluster_domain
        )
        container['env'][1]['value'] = str(self._args.port)
        container['env'][3]['valueFrom']['secretKeyRef']['name'] = self._args.existing_secret

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-user-job',
            metadata={'namespace': self._args.namespace, 'labels': {'app': 'postgres-user-provisioner'}},
            spec=spec,
            opts=job_opts,
        )

    def _build_user_commands(self, users: List[UserArgs]) -> pulumi.Output[str]:
        '''
        Build the shell commands to create and configure users.
        
        Handles Pulumi Outputs in user arguments (e.g., passwords from secrets).
        '''
        # Resolve each user's password (which may be a Pulumi Output) 
        def resolve_user(user: UserArgs) -> pulumi.Output[dict]:
            return pulumi.Output.from_input(user.password).apply(lambda pw: {
                'name': user.name,
                'password': pw,
                'superuser': user.superuser,
                'createdb': user.createdb,
                'createrole': user.createrole,
                'login': user.login,
                'connection_limit': user.connection_limit,
                'valid_until': user.valid_until,
                'grants': user.grants,
            })
        
        resolved_user_outputs = [resolve_user(u) for u in users]
        
        def build_script(resolved_users: List[dict]) -> str:
            commands = [
                'sleep 5',  # Wait for PostgreSQL to be ready
            ]

            for user in resolved_users:
                # Build CREATE ROLE options
                options = []
                options.append('SUPERUSER' if user['superuser'] else 'NOSUPERUSER')
                options.append('CREATEDB' if user['createdb'] else 'NOCREATEDB')
                options.append('CREATEROLE' if user['createrole'] else 'NOCREATEROLE')
                options.append('LOGIN' if user['login'] else 'NOLOGIN')
                # Escape single quotes in password for SQL
                escaped_pw = user['password'].replace("'", "''")
                options.append(f"PASSWORD '{escaped_pw}'")
                
                if user['connection_limit'] != -1:
                    options.append(f"CONNECTION LIMIT {user['connection_limit']}")
                
                if user['valid_until']:
                    options.append(f"VALID UNTIL '{user['valid_until']}'")

                options_str = ' '.join(options)

                # Create or alter user (idempotent)
                create_user_cmd = f"psql -tc \"SELECT 1 FROM pg_roles WHERE rolname = '{user['name']}'\" | grep -q 1"
                create_user_cmd += f" && psql -c \"ALTER USER {user['name']} WITH {options_str}\""
                create_user_cmd += f" || psql -c \"CREATE USER {user['name']} WITH {options_str}\""
                
                commands.append(create_user_cmd)
                commands.append(f"echo 'User {user['name']} ready'")

                # Process grants if specified
                if user['grants']:
                    for grant in user['grants']:
                        privileges = ', '.join(grant.privileges)
                        grant_option = ' WITH GRANT OPTION' if grant.grant_option else ''
                        
                        # Grant connect on database
                        commands.append(
                            f"psql -c \"GRANT CONNECT ON DATABASE {grant.database} TO {user['name']}\""
                        )
                        
                        # Grant privileges on each schema
                        for schema in grant.schemas:
                            # Grant usage on schema
                            commands.append(
                                f"psql -d {grant.database} -c \"GRANT USAGE ON SCHEMA {schema} TO {user['name']}\""
                            )
                            
                            # Grant privileges on tables
                            if grant.tables == 'ALL TABLES':
                                commands.append(
                                    f"psql -d {grant.database} -c \"GRANT {privileges} ON ALL TABLES IN SCHEMA {schema} TO {user['name']}{grant_option}\""
                                )
                                # Also grant on future tables
                                commands.append(
                                    f"psql -d {grant.database} -c \"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT {privileges} ON TABLES TO {user['name']}{grant_option}\""
                                )
                            else:
                                commands.append(
                                    f"psql -d {grant.database} -c \"GRANT {privileges} ON TABLE {schema}.{grant.tables} TO {user['name']}{grant_option}\""
                                )

            return ' && '.join(commands)
        
        return pulumi.Output.all(*resolved_user_outputs).apply(build_script)