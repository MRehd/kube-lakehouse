'''
MLflow Tracking Server on Kubernetes — community-charts/mlflow Helm chart.

Deploys the MLflow tracking server backed by an external PostgreSQL database
and MinIO (S3-compatible) artifact store. Both backend and artifact credentials
are resolved at deploy time via Pulumi Outputs — never embedded in plain text.

Example:
    mlflow = Mlflow('mlflow', MlflowArgs(
        namespace=ns.metadata.name,
        postgres_host=psql.host,
        postgres_db='mlflow',
        postgres_user='mlflow',
        postgres_password=config.require_secret('mlflow_postgres_password'),
        s3_endpoint=minio.endpoint,
        s3_bucket='mlflow-artifacts',
        s3_access_key='minioadmin',
        s3_secret_key=minio_password,
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts


CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class MlflowArgs:
    '''Configuration arguments for the MLflow tracking server.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name. Defaults to the Pulumi resource name.'''

    chart_version: str = '1.8.1'
    '''Version of the community-charts/mlflow Helm chart.'''

    postgres_host: Input[str] = ''
    '''External PostgreSQL host. Accepts a Pulumi Output.'''

    postgres_port: int = 5432
    '''PostgreSQL port.'''

    postgres_db: str = 'mlflow'
    '''Database name for MLflow metadata.'''

    postgres_user: Input[str] = ''
    '''PostgreSQL user for MLflow. Accepts a Pulumi Output.'''

    postgres_password: Input[str] = ''
    '''PostgreSQL password. Accepts a Pulumi secret Output.'''

    s3_endpoint: Input[str] = ''
    '''S3-compatible artifact store endpoint (e.g. MinIO). Accepts a Pulumi Output.'''

    s3_bucket: str = 'mlflow-artifacts'
    '''MinIO/S3 bucket for MLflow artifacts.'''

    s3_access_key: Input[str] = ''
    '''S3 access key. Accepts a Pulumi Output.'''

    s3_secret_key: Input[str] = ''
    '''S3 secret key. Accepts a Pulumi secret Output.'''

    s3_region: str = 'us-east-1'
    '''S3 region (any value works for MinIO).'''

    ingress_enabled: bool = False
    '''Create an Ingress for the MLflow UI.'''

    ingress_domain: Optional[Input[str]] = None
    '''Base domain. Creates mlflow.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Mlflow(pulumi.ComponentResource):
    '''
    Deploys the MLflow tracking server using the community-charts Helm chart.

    Uses an external PostgreSQL database as the backend store and MinIO
    as the S3-compatible artifact store.

    Outputs:
        namespace  — Kubernetes namespace
        endpoint   — Internal cluster URL (http://<release>.<ns>.svc.cluster.local:80)
        ui_url     — External URL if ingress enabled, else same as endpoint
    '''

    def __init__(
        self,
        name: str,
        args: MlflowArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:mlflow:Mlflow', name, {}, opts)

        args = args or MlflowArgs()
        self._namespace   = Output.from_input(args.namespace)
        release           = args.release_name or name
        postgres_host     = Output.from_input(args.postgres_host)
        postgres_user     = Output.from_input(args.postgres_user)
        postgres_password = Output.from_input(args.postgres_password)
        s3_access_key     = Output.from_input(args.s3_access_key)
        s3_secret_key     = Output.from_input(args.s3_secret_key)
        s3_endpoint       = Output.from_input(args.s3_endpoint)

        v = json.loads((CONFIG_DIR / 'helm/helm_values_mlflow.json').read_text())
        v['fullnameOverride']                         = release
        v['backendStore']['postgres']['port']         = args.postgres_port
        v['backendStore']['postgres']['database']     = args.postgres_db
        v['backendStore']['postgres']['host']         = postgres_host
        v['backendStore']['postgres']['user']         = postgres_user
        v['backendStore']['postgres']['password']     = postgres_password
        v['artifactRoot']['s3']['bucket']             = args.s3_bucket
        v['artifactRoot']['s3']['awsAccessKeyId']     = s3_access_key
        v['artifactRoot']['s3']['awsSecretAccessKey'] = s3_secret_key
        v['extraEnvVars'] = {
            'MLFLOW_S3_ENDPOINT_URL': s3_endpoint,
            'AWS_DEFAULT_REGION':     args.s3_region,
        }
        v['ingress']['enabled']   = args.ingress_enabled
        v['ingress']['className'] = args.ingress_class_name
        if args.ingress_enabled and args.ingress_domain:
            v['ingress']['hosts'] = [{'host': Output.concat('mlflow.', Output.from_input(args.ingress_domain)), 'paths': [{'path': '/', 'pathType': 'ImplementationSpecific'}]}]

        def ignore_generated_secret_data(t: pulumi.ResourceTransformArgs) -> pulumi.ResourceTransformResult:
            if t.type_ == 'kubernetes:core/v1:Secret':
                t.opts.ignore_changes = ['data']
            return pulumi.ResourceTransformResult(props=t.props, opts=t.opts)

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='mlflow',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://community-charts.github.io/helm-charts'),
                values=v,
            ),
            opts=pulumi.ResourceOptions(parent=self, transforms=[ignore_generated_secret_data]),
        )

        self.namespace = self._namespace
        self.endpoint  = Output.concat(
            'http://', release, '.', self._namespace, '.svc.cluster.local:80'
        )
        self.ui_url = (
            Output.concat('http://mlflow.', Output.from_input(args.ingress_domain))
            if args.ingress_enabled and args.ingress_domain
            else self.endpoint
        )

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint':  self.endpoint,
            'ui_url':    self.ui_url,
        })
