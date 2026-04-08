'''
Apache Polaris on Kubernetes — official Helm chart.

Deploys Polaris (Iceberg REST catalog) and exposes methods to bootstrap the
database schema, create catalogs, create principal roles, and create principals
(service accounts) — all via Kubernetes Jobs that call the Polaris REST API.

Dependency order:
    Polaris chart → create_bootstrap → create_catalogs
                                     → create_roles → create_principals

Example:
    polaris = Polaris('polaris', PolarisArgs(
        namespace=ns.metadata.name,
        persistence_type='relational-jdbc',
        persistence_secret_name='polaris-db',
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))

    bootstrap = polaris.create_bootstrap(
        'bootstrap',
        root_client_id='root',
        root_client_secret=config.require_secret('polaris_root_secret'),
    )

    catalogs = polaris.create_catalogs('catalogs', [
        CatalogArgs(name='bronze', s3_endpoint=minio.endpoint, s3_bucket='bronze',
                    s3_access_key='minioadmin', s3_secret_key=minio_password),
    ], opts=pulumi.ResourceOptions(depends_on=[bootstrap]))

    roles = polaris.create_roles('roles', [
        RoleArgs(name='data_engineer', catalog_grants=[
            CatalogGrantArgs(catalog='bronze', role='catalog_admin'),
        ]),
    ], opts=pulumi.ResourceOptions(depends_on=[catalogs]))

    polaris.create_principals('principals', [
        PrincipalArgs(name='trino', roles=['data_engineer'],
                      credentials_secret_name='polaris-trino-credentials'),
    ], opts=pulumi.ResourceOptions(depends_on=[roles]))
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.batch.v1 import Job
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts
from pulumi_kubernetes.networking.v1 import Ingress

from config.utils.utils import _deep_merge

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class CatalogArgs:
    '''Configuration for an Apache Polaris catalog backed by S3/MinIO storage.'''

    name: str
    '''Catalog name in Polaris (also used as the Iceberg warehouse name).'''

    s3_endpoint: Input[str]
    '''S3/MinIO endpoint URL. Accepts a Pulumi Output (e.g. minio.endpoint).'''

    s3_bucket: str
    '''S3/MinIO bucket backing this catalog's warehouse.'''

    s3_access_key: Input[str]
    '''S3 access key ID. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str]
    '''S3 secret access key. Accepts a Pulumi secret Output.'''

    s3_path_style_access: bool = True
    '''Use path-style S3 access (required for MinIO).'''

    s3_region: str = 'us-east-1'
    '''S3 region (any value works for MinIO).'''

    realm: str = 'POLARIS'
    '''Polaris realm to create the catalog in.'''

    catalog_type: str = 'INTERNAL'
    '''Catalog type: INTERNAL or EXTERNAL.'''

    default_base_location: Optional[str] = None
    '''Default warehouse base location. Defaults to s3://<bucket>/.'''


@dataclass
class CatalogGrantArgs:
    '''A catalog-role grant assigned to a principal role.'''

    catalog: str
    '''Name of the catalog to grant access to.'''

    role: str = 'catalog_admin'
    '''Catalog role to grant (e.g. "catalog_admin").'''


@dataclass
class RoleArgs:
    '''Configuration for a Polaris principal role (RBAC role).'''

    name: str
    '''Name of the principal role to create.'''

    catalog_grants: List[CatalogGrantArgs] = field(default_factory=list)
    '''Catalog roles to grant to this principal role.'''


@dataclass
class PrincipalArgs:
    '''Configuration for a Polaris principal (service account / user).'''

    name: str
    '''Name of the principal to create.'''

    credentials_secret_name: str
    '''
    K8s Secret name where this principal's OAuth2 credentials (CLIENT_ID /
    CLIENT_SECRET) will be stored. Created once at principal creation time and
    never overwritten. The provisioner job must run under a ServiceAccount
    with get/create on Secrets.
    '''

    roles: List[str] = field(default_factory=list)
    '''Principal role names to assign to this principal.'''


@dataclass
class PolarisArgs:
    '''Configuration arguments for Apache Polaris deployment.'''

    namespace: Input[str] = 'polaris'
    '''Kubernetes namespace to deploy Polaris into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '1.3.0-incubating'
    '''Version of the Apache Polaris Helm chart.'''

    chart_repo: str = 'https://downloads.apache.org/polaris/helm-chart/'
    '''Helm chart repository URL.'''

    image_repository: str = 'apache/polaris'
    '''Docker image repository.'''

    image_tag: str = 'latest'
    '''Docker image tag.'''

    image_pull_policy: str = 'IfNotPresent'
    '''Image pull policy: Always, IfNotPresent, or Never.'''

    replica_count: int = 1
    '''Number of Polaris replicas.'''

    service_type: str = 'LoadBalancer'
    '''Kubernetes service type: ClusterIP, NodePort, or LoadBalancer.'''

    service_port: int = 8181
    '''Polaris REST API port.'''

    management_port: int = 8182
    '''Polaris management port (health checks, metrics).'''

    realms: List[str] = field(default_factory=lambda: ['POLARIS'])
    '''Valid Polaris realms. First realm is the default for catalog/bootstrap operations.'''

    persistence_type: str = 'relational-jdbc'
    '''Persistence backend: "in-memory" or "relational-jdbc".'''

    persistence_secret_name: Optional[str] = None
    '''K8s Secret containing database credentials (jdbcUrl, username, password keys).'''

    persistence_secret_username_key: str = 'username'
    '''Key in the secret for the database username.'''

    persistence_secret_password_key: str = 'password'
    '''Key in the secret for the database password.'''

    persistence_secret_jdbc_url_key: str = 'jdbcUrl'
    '''Key in the secret for the JDBC connection URL.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '512Mi', 'cpu': '250m'},
        'limits':   {'memory': '1Gi',   'cpu': '500m'},
    })
    '''CPU and memory requests/limits for Polaris pods.'''

    metrics_enabled: bool = True
    '''Enable Prometheus metrics collection.'''

    logging_level: str = 'INFO'
    '''Root logging level.'''

    logging_console_json: bool = False
    '''Emit console logs in JSON format.'''

    autoscaling_enabled: bool = True
    '''Enable the Horizontal Pod Autoscaler.'''

    autoscaling_min_replicas: int = 1
    '''Minimum HPA replicas.'''

    autoscaling_max_replicas: int = 2
    '''Maximum HPA replicas.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix.'''

    ingress_enabled: bool = True
    '''Create an Ingress for external access.'''

    ingress_domain: Optional[str] = None
    '''Base domain. Creates polaris.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''

    ingress_annotations: Optional[dict] = None
    '''Extra Ingress annotations.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Polaris(pulumi.ComponentResource):
    '''
    Deploys Apache Polaris (Iceberg REST catalog) to Kubernetes.

    Call create_bootstrap() once before any other provisioning method.
    Then call create_catalogs(), create_roles(), and create_principals()
    in dependency order to set up the full RBAC and catalog hierarchy.

    Outputs:
        namespace          — Kubernetes namespace
        host               — Internal DNS hostname
        endpoint           — Internal REST API URL (http://<host>:<port>)
        management_endpoint — Internal management URL
        api_url            — External URL if ingress enabled, else same as endpoint

    Example:
        polaris = Polaris('polaris', PolarisArgs(
            namespace=ns.metadata.name,
            persistence_type='relational-jdbc',
            persistence_secret_name='polaris-db',
            ingress_enabled=True,
            ingress_domain='k8lh.local',
        ))

        bootstrap = polaris.create_bootstrap('bootstrap',
            root_client_secret=config.require_secret('polaris_root_secret'))
        catalogs = polaris.create_catalogs('catalogs', [...],
            opts=pulumi.ResourceOptions(depends_on=[bootstrap]))
        roles    = polaris.create_roles('roles', [...],
            opts=pulumi.ResourceOptions(depends_on=[catalogs]))
        polaris.create_principals('principals', [...],
            opts=pulumi.ResourceOptions(depends_on=[roles]))
    '''

    def __init__(
        self,
        name: str,
        args: PolarisArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:polaris:Polaris', name, {}, opts)

        args = args or PolarisArgs()
        self._release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)
        # Internal URL used by provisioning Jobs (Jobs run inside the cluster)
        self._polaris_url = f'http://{self._release_name}:{args.service_port}'
        # Bootstrap credentials — set by create_bootstrap(), read by create_catalogs/roles/principals
        self._root_client_id: str = 'root'
        self._root_client_secret: Output[str] = Output.from_input('root')
        # Fields needed by create_bootstrap()
        self._image_tag                        = args.image_tag
        self._realms                           = args.realms
        self._persistence_type                 = args.persistence_type
        self._persistence_secret_name          = args.persistence_secret_name
        self._persistence_secret_username_key  = args.persistence_secret_username_key
        self._persistence_secret_password_key  = args.persistence_secret_password_key
        self._persistence_secret_jdbc_url_key  = args.persistence_secret_jdbc_url_key

        # ── Helm values ───────────────────────────────────────────────────────
        values = json.loads((CONFIG_DIR / 'helm/helm_values_polaris.json').read_text())

        values['fullnameOverride']                = self._release_name
        values['replicaCount']                    = args.replica_count
        values['image']['repository']             = args.image_repository
        values['image']['tag']                    = args.image_tag
        values['image']['pullPolicy']             = args.image_pull_policy
        values['service']['type']                 = args.service_type
        values['service']['ports'][0]['port']     = args.service_port
        values['managementService']['ports'][0]['port'] = args.management_port
        values['resources']                       = args.resources
        values['realmContext']['realms']          = args.realms
        values['metrics']['enabled']              = args.metrics_enabled
        values['logging']['level']                = args.logging_level
        values['logging']['console']['json']      = args.logging_console_json
        values['autoscaling']['enabled']          = args.autoscaling_enabled
        values['autoscaling']['minReplicas']      = args.autoscaling_min_replicas
        values['autoscaling']['maxReplicas']      = args.autoscaling_max_replicas

        if args.persistence_type == 'relational-jdbc':
            values['persistence'] = {
                'type': 'relational-jdbc',
                'relationalJdbc': {
                    'secret': {
                        'name':     args.persistence_secret_name,
                        'username': args.persistence_secret_username_key,
                        'password': args.persistence_secret_password_key,
                        'jdbcUrl':  args.persistence_secret_jdbc_url_key,
                    },
                },
            }
        else:
            values['persistence'] = {'type': 'in-memory'}

        values = _deep_merge(values, args.extra_values)

        # ── Chart ─────────────────────────────────────────────────────────────
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='polaris',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo=args.chart_repo),
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
        self.endpoint = Output.concat('http://', self.host, ':', str(args.service_port))
        self.management_endpoint = Output.concat(
            'http://', self._release_name, '.', self._namespace,
            '.svc.', args.cluster_domain, ':', str(args.management_port),
        )

        # ── Ingress ───────────────────────────────────────────────────────────
        if args.ingress_enabled and args.ingress_domain:
            api_host = f'polaris.{args.ingress_domain}'

            annotations = {
                'nginx.ingress.kubernetes.io/proxy-body-size':    '0',
                'nginx.ingress.kubernetes.io/proxy-read-timeout': '600',
                'nginx.ingress.kubernetes.io/proxy-send-timeout': '600',
            }
            if args.ingress_annotations:
                annotations.update(args.ingress_annotations)

            spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            spec['ingressClassName'] = args.ingress_class_name
            spec['rules'][0]['host'] = api_host
            spec['rules'][0]['http']['paths'][0]['backend']['service']['name']           = self._release_name
            spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'] = args.service_port

            Ingress(
                f'{self._release_name}-ingress',
                metadata={'namespace': self._namespace, 'annotations': annotations},
                spec=spec,
                opts=pulumi.ResourceOptions(parent=self, depends_on=[self.chart]),
            )
            self.api_url = Output.from_input(f'http://{api_host}')
        else:
            self.api_url = self.endpoint

        self.register_outputs({
            'namespace':           self.namespace,
            'endpoint':            self.endpoint,
            'management_endpoint': self.management_endpoint,
            'api_url':             self.api_url,
            'host':                self.host,
        })

    def create_bootstrap(
        self,
        name: str,
        root_client_id: str = 'root',
        root_client_secret: pulumi.Input[str] = 'root',
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Bootstrap Polaris: create the database schema and the root principal.

        Must be run once before create_catalogs(), create_roles(), and
        create_principals(). Uses the polaris-admin-tool image.

        Args:
            name:               Pulumi resource name prefix.
            root_client_id:     Client ID for the root principal (default: "root").
            root_client_secret: Client secret. Accepts a Pulumi secret Output.
            opts:               Optional extra resource options.

        Returns:
            The Kubernetes Job resource.

        Example:
            bootstrap = polaris.create_bootstrap(
                'bootstrap',
                root_client_secret=config.require_secret('polaris_root_secret'),
                opts=pulumi.ResourceOptions(depends_on=[psql_job]),
            )
        '''
        # Store for downstream provisioning jobs
        self._root_client_id     = root_client_id
        self._root_client_secret = Output.from_input(root_client_secret)

        realm = self._realms[0] if self._realms else 'POLARIS'

        script_template = (CONFIG_DIR / 'scripts/bootstrap.sh').read_text()

        bootstrap_script = self._root_client_secret.apply(
            lambda secret: script_template
            .replace('{{REALM}}', realm)
            .replace('{{CREDENTIAL}}', f'{realm},{root_client_id},{secret}')
        )

        spec = json.loads((CONFIG_DIR / 'jobs/bootstrap_job_spec.json').read_text())
        container = spec['template']['spec']['containers'][0]
        container['image']            = f'apache/polaris-admin-tool:{self._image_tag}'
        container['args']             = [bootstrap_script]
        container['env'][0]['value']  = self._persistence_type
        container['env'][1]['valueFrom']['secretKeyRef']['name'] = self._persistence_secret_name
        container['env'][1]['valueFrom']['secretKeyRef']['key']  = self._persistence_secret_username_key
        container['env'][2]['valueFrom']['secretKeyRef']['name'] = self._persistence_secret_name
        container['env'][2]['valueFrom']['secretKeyRef']['key']  = self._persistence_secret_password_key
        container['env'][3]['valueFrom']['secretKeyRef']['name'] = self._persistence_secret_name
        container['env'][3]['valueFrom']['secretKeyRef']['key']  = self._persistence_secret_jdbc_url_key

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-bootstrap-job',
            metadata={'namespace': self._namespace, 'labels': {'app': 'polaris-bootstrap'}},
            spec=spec,
            opts=job_opts,
        )

    def create_catalogs(
        self,
        name: str,
        catalogs: List[CatalogArgs],
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create Iceberg catalogs in Polaris via REST API.

        Requires create_bootstrap() to have completed first.
        All S3/MinIO credentials are resolved at deploy time — they may be Pulumi Outputs.

        Args:
            name:     Pulumi resource name prefix.
            catalogs: List of catalog configurations.
            opts:     Optional extra resource options (e.g. depends_on=[bootstrap]).

        Returns:
            The Kubernetes Job resource.

        Example:
            polaris.create_catalogs('catalogs', [
                CatalogArgs(
                    name='bronze',
                    s3_endpoint=minio.endpoint,
                    s3_bucket='bronze',
                    s3_access_key='minioadmin',
                    s3_secret_key=minio_password,
                ),
            ], opts=pulumi.ResourceOptions(depends_on=[bootstrap]))
        '''
        script_template = (CONFIG_DIR / 'scripts/create_catalogs.sh').read_text()

        # Gather all Output[str] values from all catalogs keyed by catalog name
        inputs = {'secret': self._root_client_secret}
        for c in catalogs:
            inputs[f'{c.name}_endpoint']   = c.s3_endpoint
            inputs[f'{c.name}_access_key'] = c.s3_access_key
            inputs[f'{c.name}_secret_key'] = c.s3_secret_key

        def build_script(r: dict) -> str:
            def make_call(c: CatalogArgs) -> str:
                base = c.default_base_location or f's3://{c.s3_bucket}/'
                return (
                    f"create_catalog '{c.name}' '{c.s3_bucket}' "
                    f"'{r[f'{c.name}_endpoint']}' '{r[f'{c.name}_access_key']}' "
                    f"'{r[f'{c.name}_secret_key']}' '{c.s3_region}' "
                    f"'{str(c.s3_path_style_access).lower()}' '{base}'"
                )
            return (
                script_template
                .replace('{{POLARIS_URL}}',    self._polaris_url)
                .replace('{{CLIENT_ID}}',      self._root_client_id)
                .replace('{{CLIENT_SECRET}}',  r['secret'])
                .replace('{{CATALOG_CALLS}}',  '\n'.join(make_call(c) for c in catalogs))
            )

        spec = json.loads((CONFIG_DIR / 'jobs/catalog_job_spec.json').read_text())
        spec['template']['spec']['containers'][0]['args'] = [
            pulumi.Output.all(**inputs).apply(build_script)
        ]

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-catalog-job',
            metadata={'namespace': self._namespace, 'labels': {'app': 'polaris-catalogs'}},
            spec=spec,
            opts=job_opts,
        )

    def create_roles(
        self,
        name: str,
        roles: List[RoleArgs],
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create principal roles and grant catalog access in Polaris via REST API.

        Requires create_bootstrap() to have completed first.

        Args:
            name:  Pulumi resource name prefix.
            roles: List of role configurations with catalog grants.
            opts:  Optional extra resource options (e.g. depends_on=[catalogs_job]).

        Returns:
            The Kubernetes Job resource.

        Example:
            polaris.create_roles('roles', [
                RoleArgs(
                    name='data_engineer',
                    catalog_grants=[
                        CatalogGrantArgs(catalog='bronze', role='catalog_admin'),
                        CatalogGrantArgs(catalog='silver', role='catalog_admin'),
                    ],
                ),
            ], opts=pulumi.ResourceOptions(depends_on=[catalogs]))
        '''
        script_template = (CONFIG_DIR / 'scripts/manage_roles.sh').read_text()

        def build_script(secret: str) -> str:
            lines = []
            for r in roles:
                lines.append(f"create_role '{r.name}'")
                for grant in r.catalog_grants:
                    lines.append(f"grant_catalog_role '{r.name}' '{grant.catalog}' '{grant.role}'")
            return (
                script_template
                .replace('{{POLARIS_URL}}',   self._polaris_url)
                .replace('{{CLIENT_ID}}',     self._root_client_id)
                .replace('{{CLIENT_SECRET}}', secret)
                .replace('{{ROLE_CALLS}}',    '\n'.join(lines))
            )

        spec = json.loads((CONFIG_DIR / 'jobs/role_job_spec.json').read_text())
        spec['template']['spec']['containers'][0]['args'] = [
            self._root_client_secret.apply(build_script)
        ]

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-role-job',
            metadata={'namespace': self._namespace, 'labels': {'app': 'polaris-roles'}},
            spec=spec,
            opts=job_opts,
        )

    def create_principals(
        self,
        name: str,
        principals: List[PrincipalArgs],
        provisioner_sa_name: Optional[Input[str]] = None,
        opts: pulumi.ResourceOptions = None,
    ) -> Job:
        '''
        Create Polaris principals and assign roles via REST API.

        Each principal's OAuth2 credentials (CLIENT_ID / CLIENT_SECRET) are
        stored in the K8s Secret named by credentials_secret_name. The job pod
        must run under a ServiceAccount with get/create permissions on Secrets.

        Args:
            name:                Pulumi resource name prefix.
            principals:          List of principal configurations.
            provisioner_sa_name: ServiceAccount name for the job pod. Required
                                 when any principal has credentials_secret_name set.
            opts:                Optional extra resource options.

        Returns:
            The Kubernetes Job resource.

        Example:
            from service_accounts import ServiceAccounts, ServiceAccountsArgs, ServiceAccountArgs, PolicyRuleArgs

            sas = ServiceAccounts('sas', ServiceAccountsArgs(namespace=ns.metadata.name))
            provisioner_sa = sas.provision('polaris-provisioner', ServiceAccountArgs(
                name='polaris-principal-provisioner',
                rules=[PolicyRuleArgs(resources=['secrets'], verbs=['get', 'create'])],
            ))

            polaris.create_principals('principals', [
                PrincipalArgs(name='trino', roles=['data_engineer'],
                              credentials_secret_name='polaris-trino-credentials'),
            ], provisioner_sa_name='polaris-principal-provisioner',
               opts=pulumi.ResourceOptions(depends_on=[roles, provisioner_sa]))
        '''
        script_template = (CONFIG_DIR / 'scripts/manage_principals.sh').read_text()

        def build_script(secret: str) -> str:
            lines = []
            for p in principals:
                lines.append(f"create_principal_with_secret '{p.name}' '{p.credentials_secret_name}'")
                for role in p.roles:
                    lines.append(f"assign_role '{p.name}' '{role}'")
            return (
                script_template
                .replace('{{POLARIS_URL}}',      self._polaris_url)
                .replace('{{CLIENT_ID}}',        self._root_client_id)
                .replace('{{CLIENT_SECRET}}',    secret)
                .replace('{{PRINCIPAL_CALLS}}',  '\n'.join(lines))
            )

        spec = json.loads((CONFIG_DIR / 'jobs/principal_job_spec.json').read_text())
        spec['template']['spec']['containers'][0]['args'] = [
            self._root_client_secret.apply(build_script)
        ]
        if provisioner_sa_name:
            spec['template']['spec']['serviceAccountName'] = provisioner_sa_name

        job_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            job_opts = pulumi.ResourceOptions.merge(job_opts, opts)

        return Job(
            f'{name}-principal-job',
            metadata={'namespace': self._namespace, 'labels': {'app': 'polaris-principals'}},
            spec=spec,
            opts=job_opts,
        )
