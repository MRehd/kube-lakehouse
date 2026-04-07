'''
PostgreSQL on Kubernetes — Bitnami Helm chart.

Deploys a PostgreSQL instance and exposes create_databases() and create_users()
to provision databases and users via Kubernetes Jobs.

Example:
    psql = Psql('psql', PsqlArgs(
        namespace=ns.metadata.name,
        existing_secret='postgres-secret',
    ))

    db_job = psql.create_databases('dbs', [
        DatabaseArgs(name='polaris'),
        DatabaseArgs(name='airflow'),
    ])

    psql.create_users('users', [
        UserArgs(name='polaris', password=config.require_secret('polaris_postgres_password'),
                 grants=[GrantArgs(database='polaris', privileges=['ALL'])]),
        UserArgs(name='airflow', password=config.require_secret('airflow_postgres_password'),
                 grants=[GrantArgs(database='airflow', privileges=['ALL'])]),
    ], opts=pulumi.ResourceOptions(depends_on=[db_job]))
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.batch.v1 import Job
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

CONFIG_DIR = Path(__file__).parent.parent / 'config'


def _deep_merge(base: dict, override: dict) -> dict:
    '''Recursively merge override into base, with override taking precedence.'''
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@dataclass
class GrantArgs:
    '''Privileges to grant to a user on a specific database/schema.'''

    database: str
    '''Database to grant access to.'''

    privileges: List[str] = field(default_factory=lambda: ['SELECT'])
    '''SQL privileges: SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, ALL.'''

    schemas: List[str] = field(default_factory=lambda: ['public'])
    '''Schemas to grant access to.'''

    tables: str = 'ALL TABLES'
    '''Tables to grant: "ALL TABLES" or a specific table name.'''

    grant_option: bool = False
    '''Allow the grantee to re-grant these privileges to others.'''


@dataclass
class UserArgs:
    '''Configuration for a PostgreSQL user (role with LOGIN).'''

    name: str
    '''Username to create.'''

    password: Input[str]
    '''Password. Accepts a plain string or a Pulumi secret Output.'''

    superuser: bool = False
    '''Grant SUPERUSER privilege.'''

    createdb: bool = False
    '''Allow the user to create databases.'''

    createrole: bool = False
    '''Allow the user to create roles.'''

    login: bool = True
    '''Allow login. Set False for group roles.'''

    connection_limit: int = -1
    '''Max concurrent connections (-1 = unlimited).'''

    valid_until: Optional[str] = None
    '''Password expiry date, e.g. "2026-12-31".'''

    grants: Optional[List[GrantArgs]] = None
    '''Database/schema grants to apply after creating the user.'''


@dataclass
class DatabaseArgs:
    '''Configuration for a PostgreSQL database.'''

    name: str
    '''Database name to create.'''

    owner: Optional[str] = None
    '''Database owner (defaults to the postgres superuser).'''

    encoding: str = 'UTF8'
    '''Character encoding.'''

    lc_collate: str = 'en_US.UTF-8'
    '''Collation order.'''

    lc_ctype: str = 'en_US.UTF-8'
    '''Character classification.'''

    template: str = 'template0'
    '''Template to clone when creating the database.'''

    extensions: Optional[List[str]] = None
    '''PostgreSQL extensions to enable (e.g. ["uuid-ossp", "pgcrypto"]).'''


@dataclass
class PsqlArgs:
    '''Configuration arguments for PostgreSQL deployment.'''

    namespace: Input[str] = 'postgresql'
    '''Kubernetes namespace to deploy PostgreSQL into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '15.5.38'
    '''Version of the Bitnami PostgreSQL Helm chart.'''

    architecture: str = 'standalone'
    '''Deployment architecture: "standalone" or "replication".'''

    existing_secret: str = None
    '''Name of an existing K8s Secret containing a "postgres-password" key.'''

    database: str = 'postgres'
    '''Default database created at startup.'''

    persistence_enabled: bool = True
    '''Mount a PersistentVolumeClaim for durable storage.'''

    persistence_size: str = '10Gi'
    '''PVC size, e.g. "10Gi".'''

    storage_class: str = 'hostpath'
    '''StorageClass for the PVC (default "hostpath" works on Docker Desktop / Rancher Desktop).'''

    service_type: str = 'ClusterIP'
    '''Kubernetes service type: ClusterIP, NodePort, or LoadBalancer.'''

    port: int = 5432
    '''PostgreSQL port.'''

    max_connections: int = 100
    '''Maximum concurrent connections.'''

    shared_buffers: str = '128MB'
    '''Shared-buffer memory allocation.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix, usually "cluster.local".'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '256Mi', 'cpu': '250m'},
        'limits':   {'memory': '512Mi', 'cpu': '500m'},
    })
    '''CPU and memory requests/limits for the PostgreSQL pod.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Psql(pulumi.ComponentResource):
    '''
    Deploys PostgreSQL to Kubernetes using the Bitnami Helm chart.

    Use create_databases() and create_users() after construction to provision
    databases and users via Kubernetes Jobs (psql client in a pod).

    Outputs:
        namespace   — Kubernetes namespace
        host        — Internal DNS hostname (<release>.<ns>.svc.<domain>)
        endpoint    — host:port
        secret_name — Name of the K8s secret holding the postgres password

    Example:
        psql = Psql('psql', PsqlArgs(
            namespace=ns.metadata.name,
            existing_secret='postgres-secret',
            persistence_size='20Gi',
        ))

        db_job = psql.create_databases('dbs', [
            DatabaseArgs(name='polaris'),
            DatabaseArgs(name='airflow'),
        ])

        psql.create_users('users', [
            UserArgs(
                name='airflow',
                password=config.require_secret('airflow_postgres_password'),
                grants=[GrantArgs(database='airflow', privileges=['ALL'])],
            ),
        ], opts=pulumi.ResourceOptions(depends_on=[db_job]))
    '''

    def __init__(
        self,
        name: str,
        args: PsqlArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:psql:Psql', name, {}, opts)

        args = args or PsqlArgs()
        self._release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)
        # Stored for use in create_databases() and create_users()
        self._existing_secret = args.existing_secret
        self._port = args.port
        self._cluster_domain = args.cluster_domain

        # ── Helm values ───────────────────────────────────────────────────────
        values = json.loads((CONFIG_DIR / 'helm/helm_values_psql.json').read_text())

        values['fullnameOverride'] = self._release_name
        values['architecture']     = args.architecture
        values['primary']['persistence']['enabled']      = args.persistence_enabled
        values['primary']['persistence']['size']         = args.persistence_size
        values['primary']['persistence']['storageClass'] = args.storage_class
        values['primary']['resources']                   = args.resources
        values['primary']['extendedConfiguration']       = (
            f'max_connections = {args.max_connections}\n'
            f'shared_buffers = {args.shared_buffers}\n'
        )
        values['primary']['service']['type']                   = args.service_type
        values['primary']['service']['ports']['postgresql']    = args.port
        values['auth']['database']      = args.database
        values['auth']['existingSecret'] = args.existing_secret

        values = _deep_merge(values, args.extra_values)

        # ── Chart ─────────────────────────────────────────────────────────────
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='postgresql',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://charts.bitnami.com/bitnami'),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        self.namespace = self._namespace
        self.host = Output.concat(
            self._release_name, '.', self._namespace,
            '.svc.', args.cluster_domain,
        )
        self.endpoint = Output.concat(self.host, ':', str(args.port))
        self.secret_name = Output.from_input(args.existing_secret)

        self.register_outputs({
            'namespace':   self.namespace,
            'host':        self.host,
            'endpoint':    self.endpoint,
            'secret_name': self.secret_name,
        })

    def create_databases(
        self,
        name: str,
        databases: 'DatabaseArgs | List[DatabaseArgs]',
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create one or more PostgreSQL databases using a Kubernetes Job.

        Reads SQL templates from config/scripts/, substitutes placeholders,
        and runs the resulting script via psql inside a job pod.

        Args:
            name:      Pulumi resource name prefix.
            databases: Single DatabaseArgs or a list.
            opts:      Optional extra resource options (e.g. depends_on).

        Returns:
            The Kubernetes Job resource.

        Example:
            psql.create_databases('lakehouse-dbs', [
                DatabaseArgs(name='polaris'),
                DatabaseArgs(name='airflow'),
            ])
        '''
        if isinstance(databases, DatabaseArgs):
            databases = [databases]

        # ── Build SQL script from templates ───────────────────────────────────
        scripts_dir    = CONFIG_DIR / 'scripts'
        create_db_tpl  = (scripts_dir / 'create_database.sql').read_text()
        enable_ext_tpl = (scripts_dir / 'enable_extension.sql').read_text()

        sql_parts = []
        for db in databases:
            owner_clause = f" OWNER ''{db.owner}''" if db.owner else ''
            sql = (create_db_tpl
                   .replace('{{NAME}}',       db.name)
                   .replace('{{ENCODING}}',   db.encoding)
                   .replace('{{LC_COLLATE}}', db.lc_collate)
                   .replace('{{LC_CTYPE}}',   db.lc_ctype)
                   .replace('{{TEMPLATE}}',   db.template)
                   .replace('{{OWNER_CLAUSE}}', owner_clause))
            sql_parts.append(sql)
            if db.extensions:
                for ext in db.extensions:
                    sql_parts.append(
                        f'\\c {db.name}\n'
                        + enable_ext_tpl.replace('{{EXTENSION}}', ext)
                    )

        # ── Job spec ──────────────────────────────────────────────────────────
        spec = json.loads((CONFIG_DIR / 'jobs/psql_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['env'][0]['value'] = Output.concat(
            self._release_name, '.', self._namespace,
            '.svc.', self._cluster_domain,
        )
        container['env'][1]['value'] = str(self._port)
        container['env'][3]['valueFrom']['secretKeyRef']['name'] = self._existing_secret
        container['env'][4]['value'] = '\n'.join(sql_parts)

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-db-job',
            metadata={
                'namespace': self._namespace,
                'labels': {'app': 'postgres-db-provisioner'},
            },
            spec=spec,
            opts=job_opts,
        )

    def create_users(
        self,
        name: str,
        users: 'UserArgs | List[UserArgs]',
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create one or more PostgreSQL users with optional grants using a Kubernetes Job.

        Passwords are Pulumi Outputs — they are resolved at deploy time and never
        appear in plaintext in the state file.

        Args:
            name:  Pulumi resource name prefix.
            users: Single UserArgs or a list.
            opts:  Optional extra resource options (e.g. depends_on=[db_job]).

        Returns:
            The Kubernetes Job resource.

        Example:
            psql.create_users('users', [
                UserArgs(
                    name='airflow',
                    password=config.require_secret('airflow_postgres_password'),
                    grants=[GrantArgs(database='airflow', privileges=['ALL'])],
                ),
            ])
        '''
        if isinstance(users, UserArgs):
            users = [users]

        # ── Resolve all passwords (they may be Output[str]) ───────────────────
        scripts_dir       = CONFIG_DIR / 'scripts'
        create_user_tpl   = (scripts_dir / 'create_user.sql').read_text()
        grant_connect_tpl = (scripts_dir / 'grant_connect.sql').read_text()
        grant_all_tpl     = (scripts_dir / 'grant_privileges.sql').read_text()
        grant_table_tpl   = (scripts_dir / 'grant_table.sql').read_text()

        # Wrap each user as an Output so we can resolve passwords uniformly
        resolved_user_outputs = [
            Output.from_input(u).apply(lambda u: {
                'name':             u.name,
                'password':         u.password,
                'superuser':        u.superuser,
                'createdb':         u.createdb,
                'createrole':       u.createrole,
                'login':            u.login,
                'connection_limit': u.connection_limit,
                'valid_until':      u.valid_until,
                'grants':           u.grants,
            })
            for u in users
        ]

        def build_script(resolved_users: list) -> str:
            sql_parts = []
            for user in resolved_users:
                options = [
                    'SUPERUSER'   if user['superuser']  else 'NOSUPERUSER',
                    'CREATEDB'    if user['createdb']   else 'NOCREATEDB',
                    'CREATEROLE'  if user['createrole'] else 'NOCREATEROLE',
                    'LOGIN'       if user['login']      else 'NOLOGIN',
                    f"PASSWORD '{user['password'].replace(chr(39), chr(39)*2)}'",
                ]
                if user['connection_limit'] != -1:
                    options.append(f"CONNECTION LIMIT {user['connection_limit']}")
                if user['valid_until']:
                    options.append(f"VALID UNTIL '{user['valid_until']}'")

                sql_parts.append(
                    create_user_tpl
                    .replace('{{NAME}}',    user['name'])
                    .replace('{{OPTIONS}}', ' '.join(options))
                )

                for grant in (user['grants'] or []):
                    privileges = ', '.join(grant.privileges)
                    grant_opt  = ' WITH GRANT OPTION' if grant.grant_option else ''

                    sql_parts.append(
                        grant_connect_tpl
                        .replace('{{DATABASE}}', grant.database)
                        .replace('{{USERNAME}}', user['name'])
                    )
                    for schema in grant.schemas:
                        template = grant_all_tpl if grant.tables == 'ALL TABLES' else grant_table_tpl
                        privs_sql = (template
                                     .replace('{{SCHEMA}}',       schema)
                                     .replace('{{USERNAME}}',     user['name'])
                                     .replace('{{PRIVILEGES}}',   privileges)
                                     .replace('{{GRANT_OPTION}}', grant_opt))
                        if grant.tables != 'ALL TABLES':
                            privs_sql = privs_sql.replace('{{TABLE}}', grant.tables)
                        sql_parts.append(f'\\c {grant.database}\n{privs_sql}')

            return '\n'.join(sql_parts)

        sql_script = pulumi.Output.all(*resolved_user_outputs).apply(build_script)

        # ── Job spec ──────────────────────────────────────────────────────────
        spec = json.loads((CONFIG_DIR / 'jobs/psql_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['env'][0]['value'] = Output.concat(
            self._release_name, '.', self._namespace,
            '.svc.', self._cluster_domain,
        )
        container['env'][1]['value'] = str(self._port)
        container['env'][3]['valueFrom']['secretKeyRef']['name'] = self._existing_secret
        container['env'][4]['value'] = sql_script

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-user-job',
            metadata={
                'namespace': self._namespace,
                'labels': {'app': 'postgres-user-provisioner'},
            },
            spec=spec,
            opts=job_opts,
        )
