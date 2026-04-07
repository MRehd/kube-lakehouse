'''Apache Airflow on Kubernetes — official Helm chart.'''

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import json
import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class AirflowConnectionArgs:
    conn_id: str
    '''Airflow connection ID, e.g. "spark_default". Injected as AIRFLOW_CONN_<ID_UPPER>.'''
    uri: Input[str]
    '''
    Connection URI, e.g. "sc://spark-connect.ns.svc.cluster.local:15002".
    Accepts Pulumi Output[str] — resolved at deploy time.
    '''


@dataclass
class AirflowArgs:
    namespace: Input[str] = 'default'
    release_name: Optional[str] = None
    chart_version: str = '1.20.0'

    executor: str = 'KubernetesExecutor'
    '''
    Task executor.
    - "KubernetesExecutor" — each task gets its own K8s pod, zero idle cost (default)
    - "LocalExecutor"      — tasks run as subprocesses in the scheduler pod, no scaling
    '''

    # External PostgreSQL — full SQLAlchemy URI stored in a K8s secret
    db_metadata_secret: str = 'airflow-metadata'
    '''
    K8s secret with a single "connection" key containing the full SQLAlchemy URI:
    postgresql+psycopg2://user:password@host:5432/airflow
    Created in __main__.py via LakehouseSecrets.
    '''

    # Encryption keys (required for stable operation)
    fernet_key_secret: str = 'airflow-fernet'
    fernet_key_secret_key: str = 'fernet-key'
    '''K8s secret holding the Fernet key for encrypting stored connections/variables.'''

    webserver_secret_key_secret: str = 'airflow-webserver'
    webserver_secret_key_secret_key: str = 'webserver-secret-key'
    '''K8s secret holding the Flask session signing key.'''

    # Webserver
    webserver_replicas: int = 1

    # Git-sync for DAGs
    git_repo: str = ''
    '''Git repository URL containing DAG files. Required for git-sync.'''
    git_branch: str = 'main'
    git_subpath: str = 'dags'
    git_sync_interval: int = 60
    '''Seconds between git-sync polling intervals.'''
    git_credentials_secret: str = ''
    '''
    Name of a K8s secret with "GIT_SYNC_USERNAME" and "GIT_SYNC_PASSWORD" keys.
    Use "x-token" as the username and a GitHub Personal Access Token as the password.
    Leave empty for public repos.
    '''

    # Plain environment variables injected into all pods
    env: dict = field(default_factory=dict)
    '''Plain key/value env vars injected into scheduler, webserver, and worker pods.'''

    # K8s secrets mounted as env vars into all pods
    env_secrets: list = field(default_factory=list)
    '''List of K8s secret names whose keys are injected as env vars via envFrom.secretRef.'''

    # Airflow connections registered automatically via env vars
    connections: list = field(default_factory=list)
    '''
    List of AirflowConnectionArgs. Each becomes an AIRFLOW_CONN_<ID_UPPER> env var,
    which Airflow parses and registers as a connection — no manual UI step needed.
    Supports Output[str] URIs (e.g. spark.connect_server_url).
    '''

    # Ingress
    ingress_enabled: bool = False
    ingress_domain: str = ''
    ingress_class_name: str = 'nginx'

    extra_values: dict = field(default_factory=dict)


class Airflow(pulumi.ComponentResource):
    '''
    Deploys Apache Airflow using the official Helm chart.

    Uses KubernetesExecutor by default — each Airflow task runs in its own K8s pod,
    created on demand and deleted when the task completes. Zero idle worker cost.

    DAGs are synced from a git repository via git-sync sidecar. Set git_repo in AirflowArgs.

    Required K8s secrets (created in __main__.py before this resource):
        airflow-metadata  — {"connection": "postgresql+psycopg2://user:pw@host:5432/airflow"}
        airflow-fernet    — {"fernet-key": "<Fernet key>"}
        airflow-webserver — {"webserver-secret-key": "<hex secret>"}

    Connections:
        Pass AirflowConnectionArgs in the connections list. Each is injected as
        AIRFLOW_CONN_<ID_UPPER>=<uri> — Airflow auto-registers them on startup.
        Supports Output[str] URIs so you can wire in dynamic endpoints directly:

            connections=[
                AirflowConnectionArgs(
                    conn_id='spark_default',
                    uri=spark.connect_server_url,
                ),
            ]
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
        release = args.release_name or name

        values_output = self._build_values(args)

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='airflow',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://airflow.apache.org'),
                values=values_output,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        if args.ingress_enabled and args.ingress_domain:
            self.ui_url = Output.from_input(f'http://airflow.{args.ingress_domain}')
        else:
            self.ui_url = Output.concat(
                'http://', release, '-webserver.', self._namespace,
                '.svc.cluster.local:8080',
            )

        self.namespace = self._namespace
        self.register_outputs({
            'namespace': self.namespace,
            'ui_url': self.ui_url,
        })

    def _build_values(self, args: AirflowArgs) -> Output:
        base = json.loads((CONFIG_DIR / 'helm/helm_values_airflow.json').read_text())

        # Collect all Output values from connections so we can resolve them together
        conn_ids = [c.conn_id for c in args.connections]
        conn_uris = [Output.from_input(c.uri) for c in args.connections]

        return Output.all(*conn_uris).apply(lambda resolved: self._assemble_values(
            base=base,
            args=args,
            conn_ids=conn_ids,
            conn_uris=list(resolved),
        ))

    def _assemble_values(self, base: dict, args: AirflowArgs, conn_ids: list, conn_uris: list) -> dict:
        values = base.copy()

        values['executor'] = args.executor

        # DB — full SQLAlchemy URI in a K8s secret
        values['postgresql'] = {'enabled': False}
        values['data'] = {'metadataSecretName': args.db_metadata_secret}

        # Encryption keys
        values['fernetKeySecretName'] = args.fernet_key_secret
        values['fernetKeySecretKey'] = args.fernet_key_secret_key
        values['webserverSecretKeySecretName'] = args.webserver_secret_key_secret
        values['webserverSecretKeySecretKey'] = args.webserver_secret_key_secret_key

        # Webserver replicas
        values.setdefault('webserver', {})['replicas'] = args.webserver_replicas

        # Git-sync DAGs
        if args.git_repo:
            git_sync = {
                'enabled': True,
                'repo': args.git_repo,
                'branch': args.git_branch,
                'subPath': args.git_subpath,
                'period': f'{args.git_sync_interval}s',
            }
            if args.git_credentials_secret:
                git_sync['credentialsSecret'] = args.git_credentials_secret
            values['dags'] = {'gitSync': git_sync}

        # Env vars: plain + connection URIs merged together
        env_list = [{'name': k, 'value': v} for k, v in args.env.items()]
        for conn_id, uri in zip(conn_ids, conn_uris):
            env_list.append({
                'name': f'AIRFLOW_CONN_{conn_id.upper()}',
                'value': uri,
            })
        if env_list:
            values['env'] = env_list

        # Secrets mounted as env vars
        if args.env_secrets:
            values['extraEnvFrom'] = [
                {'secretRef': {'name': s}} for s in args.env_secrets
            ]

        # Ingress
        if args.ingress_enabled and args.ingress_domain:
            values['ingress'] = {
                'web': {
                    'enabled': True,
                    'ingressClassName': args.ingress_class_name,
                    'hosts': [{'name': f'airflow.{args.ingress_domain}'}],
                }
            }

        return self._deep_merge(values, args.extra_values)

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Airflow._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
