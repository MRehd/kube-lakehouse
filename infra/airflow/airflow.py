'''
Apache Airflow on Kubernetes — official Helm chart.

Deploys Airflow using the official Apache Airflow Helm chart (v1.20.0+).
Uses KubernetesExecutor by default — each Airflow task runs in its own pod,
created on demand and deleted when the task completes. Zero idle worker cost.

Required K8s secrets (create in __main__.py before deploying Airflow):
    airflow-metadata  — {"connection": "postgresql+psycopg2://user:pw@host:5432/airflow"}
    airflow-fernet    — {"fernet-key": "<Fernet key>"}
    airflow-webserver — {"webserver-secret-key": "<hex secret>"}

DAGs are synced from a git repository via git-sync sidecar. Set git_repo in AirflowArgs.

Connections are auto-registered via AIRFLOW_CONN_<ID> env vars — no manual UI step needed.

Example:
    airflow = Airflow('airflow', AirflowArgs(
        namespace=ns.metadata.name,
        admin_password=config.require_secret('airflow_admin_password'),
        git_repo='https://github.com/org/dags-repo.git',
        git_branch='master',
        git_credentials_secret='airflow-git-credentials',
        env={
            'KAFKA_BOOTSTRAP_SERVERS': kafka.bootstrap_servers,
            'MINIO_ENDPOINT':          minio.endpoint,
        },
        env_secrets=['spark-s3-credentials'],
        connections=[
            AirflowConnectionArgs(
                conn_id='spark_default',
                uri=spark.connect_server_url,
            ),
        ],
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))
'''

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import json
import yaml
import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

from config.utils.utils import _deep_merge

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class AirflowConnectionArgs:
    '''A single Airflow connection to auto-register via AIRFLOW_CONN_<ID> env var.'''

    conn_id: str
    '''
    Airflow connection ID (e.g. "spark_default").
    Injected as AIRFLOW_CONN_<ID_UPPER> into the scheduler, webserver, and worker pods.
    Airflow parses and registers it on startup — no manual UI step needed.
    '''

    uri: Input[str]
    '''
    Connection URI (e.g. "sc://spark-connect.ns.svc.cluster.local:15002").
    Accepts a Pulumi Output — resolved at deploy time.
    '''


@dataclass
class AirflowArgs:
    '''Configuration arguments for Apache Airflow deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '1.20.0'
    '''Version of the official Apache Airflow Helm chart.'''

    executor: str = 'KubernetesExecutor'
    '''
    Task executor backend.
    - "KubernetesExecutor" — each task gets its own K8s pod, zero idle cost (default)
    - "LocalExecutor"      — tasks run as subprocesses in the scheduler pod, no scaling
    '''

    db_metadata_secret: str = 'airflow-metadata'
    '''
    K8s secret with a single "connection" key containing the full SQLAlchemy URI:
    postgresql+psycopg2://user:password@host:5432/airflow
    Create in __main__.py via LakehouseSecrets before deploying Airflow.
    '''

    fernet_key_secret: str = 'airflow-fernet'
    '''K8s secret holding the Fernet key for encrypting stored connections/variables.'''

    fernet_key_secret_key: str = 'fernet-key'
    '''Key inside fernet_key_secret.'''

    webserver_secret_key_secret: str = 'airflow-webserver'
    '''K8s secret holding the Flask session signing key.'''

    webserver_secret_key_secret_key: str = 'webserver-secret-key'
    '''Key inside webserver_secret_key_secret.'''

    admin_username: str = 'admin'
    '''Airflow admin username.'''

    admin_password: Input[str] = 'admin'
    '''Airflow admin password. Accepts a Pulumi secret Output.'''

    admin_email: str = 'admin@example.com'
    '''Admin user email.'''

    admin_firstname: str = 'Admin'
    '''Admin user first name.'''

    admin_lastname: str = 'User'
    '''Admin user last name.'''

    webserver_replicas: int = 1
    '''Number of webserver (api-server in Airflow 3.x) replicas.'''

    git_repo: str = ''
    '''Git repository URL containing DAG files. Leave empty to skip git-sync.'''

    git_branch: str = 'main'
    '''Git branch to sync from.'''

    git_subpath: str = 'dags'
    '''Subdirectory within the repo containing DAG files.'''

    git_sync_interval: int = 60
    '''Seconds between git-sync polling intervals.'''

    git_credentials_secret: str = ''
    '''
    Name of a K8s secret with "GIT_SYNC_USERNAME" / "GIT_SYNC_PASSWORD" keys
    (and their GITSYNC_* equivalents for git-sync v4 compatibility).
    Use "x-token" as the username and a GitHub Personal Access Token as the password.
    Leave empty for public repos.
    '''

    env: dict = field(default_factory=dict)
    '''
    Plain key/value env vars injected into scheduler, webserver, and worker pods.
    Values may be Pulumi Outputs (e.g. kafka.bootstrap_servers, minio.endpoint).
    '''

    env_secrets: list = field(default_factory=list)
    '''
    List of K8s secret names whose keys are injected as env vars via envFrom.secretRef.
    Use for credentials that should not appear in Helm values (e.g. S3 access keys).
    '''

    connections: list = field(default_factory=list)
    '''
    List of AirflowConnectionArgs. Each becomes AIRFLOW_CONN_<ID_UPPER>=<uri>,
    which Airflow parses and registers as a connection on startup.
    Supports Output[str] URIs (e.g. spark.connect_server_url).
    '''

    ingress_enabled: bool = False
    '''Create an Ingress for the Airflow UI (api-server in Airflow 3.x).'''

    ingress_domain: str = ''
    '''Base domain. Creates airflow.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Airflow(pulumi.ComponentResource):
    '''
    Deploys Apache Airflow using the official Helm chart.

    Uses KubernetesExecutor by default — each task runs in its own pod.
    DAGs are synced from a git repository via the git-sync sidecar.
    Connections are auto-registered via AIRFLOW_CONN_<ID> env vars.

    Outputs:
        namespace — Kubernetes namespace
        ui_url    — http://airflow.<domain> if ingress enabled, else internal svc URL

    Example:
        airflow = Airflow('airflow', AirflowArgs(
            namespace=ns.metadata.name,
            admin_password=config.require_secret('airflow_admin_password'),
            git_repo='https://github.com/org/dags.git',
            git_branch='master',
            git_credentials_secret='airflow-git-credentials',
            connections=[
                AirflowConnectionArgs(
                    conn_id='spark_default',
                    uri=spark.connect_server_url,
                ),
            ],
            ingress_enabled=True,
            ingress_domain='k8lh.local',
        ))
    '''

    def __init__(
        self,
        name: str,
        args: AirflowArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:airflow:Airflow', name, {}, opts)

        args = args or AirflowArgs()
        self._namespace = Output.from_input(args.namespace)
        admin_password  = Output.from_input(args.admin_password)
        env_values      = {k: Output.from_input(v) for k, v in args.env.items()}
        conn_uris       = {c.conn_id: Output.from_input(c.uri) for c in args.connections}
        release         = args.release_name or name

        v = json.loads((CONFIG_DIR / 'helm/helm_values_airflow.json').read_text())
        v['executor']   = args.executor
        v['postgresql'] = {'enabled': False}
        v['data']       = {'metadataSecretName': args.db_metadata_secret}
        v['fernetKeySecretName']          = args.fernet_key_secret
        v['fernetKeySecretKey']           = args.fernet_key_secret_key
        v['webserverSecretKeySecretName'] = args.webserver_secret_key_secret
        v['webserverSecretKeySecretKey']  = args.webserver_secret_key_secret_key
        v['airflowUser'] = {
            'username':  args.admin_username,
            'password':  admin_password,
            'email':     args.admin_email,
            'firstname': args.admin_firstname,
            'lastname':  args.admin_lastname,
            'role':      'Admin',
        }
        v.setdefault('webserver', {})['replicas'] = args.webserver_replicas

        if args.git_repo:
            git_sync = {
                'enabled': True,
                'repo':    args.git_repo,
                'branch':  args.git_branch,
                'subPath': args.git_subpath,
                'period':  f'{args.git_sync_interval}s',
            }
            if args.git_credentials_secret:
                git_sync['credentialsSecret'] = args.git_credentials_secret
            v['dags'] = {'gitSync': git_sync}

        env_list = [{'name': k, 'value': val} for k, val in env_values.items()]
        for conn_id, uri in conn_uris.items():
            env_list.append({'name': f'AIRFLOW_CONN_{conn_id.upper()}', 'value': uri})
        if env_list:
            v['env'] = env_list

        if args.env_secrets:
            v['extraEnvFrom'] = yaml.dump([{'secretRef': {'name': s}} for s in args.env_secrets])

        if args.ingress_enabled and args.ingress_domain:
            v['ingress'] = {'apiServer': {
                'enabled':          True,
                'ingressClassName': args.ingress_class_name,
                'hosts':            [{'name': f'airflow.{args.ingress_domain}'}],
            }}

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='airflow',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://airflow.apache.org'),
                values=_deep_merge(v, args.extra_values),
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        if args.ingress_enabled and args.ingress_domain:
            self.ui_url = Output.from_input(f'http://airflow.{args.ingress_domain}')
        else:
            self.ui_url = Output.concat(
                'http://', release, '-api-server.', self._namespace,
                '.svc.cluster.local:8080',
            )

        self.namespace = self._namespace
        self.register_outputs({
            'namespace': self.namespace,
            'ui_url':    self.ui_url,
        })
