'''Apache Spark on Kubernetes — operator + history server.'''

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import json
import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apps.v1 import Deployment
from pulumi_kubernetes.core.v1 import Service
from pulumi_kubernetes.networking.v1 import Ingress
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

CONFIG_DIR = Path(__file__).parent.parent / 'config'

HISTORY_SERVER_PORT = 18080


@dataclass
class SparkArgs:
    namespace: Input[str] = 'default'
    release_name: Optional[str] = None
    chart_version: str = '1.4.6'
    '''spark-on-k8s-operator Helm chart version.'''

    service_account_name: Input[str] = 'spark'
    '''
    Name of the ServiceAccount used by driver/executor pods.
    Create via the ServiceAccounts module and pass the output here.
    '''

    # History Server
    history_server_image: str = 'apache/spark'
    history_server_image_tag: str = '3.5.3'
    event_log_bucket: str = 'spark-logs'
    '''MinIO bucket where Spark writes event logs.'''

    s3_endpoint: Input[str] = ''
    s3_access_key: Input[str] = ''
    s3_secret_key: Input[str] = ''

    ingress_enabled: bool = False
    ingress_domain: str = ''
    ingress_class_name: str = 'nginx'

    extra_values: dict = field(default_factory=dict)


class Spark(pulumi.ComponentResource):
    '''
    Deploys the Kubeflow spark-on-k8s-operator and a Spark History Server.

    The operator handles SparkApplication CRs submitted by Airflow via the
    SparkKubernetesOperator. The ServiceAccount for driver/executor pods is
    created externally via the ServiceAccounts module and passed in via
    SparkArgs.service_account_name.

    The History Server reads event logs from MinIO and provides a persistent
    UI at spark.<domain> showing completed and running jobs.

    Airflow integration:
        SparkApplication CRs must set:
          driver.serviceAccount: <service_account_name>
          sparkConf:
            spark.eventLog.enabled: "true"
            spark.eventLog.dir: "s3a://spark-logs/"
    '''

    def __init__(
        self,
        name: str,
        args: SparkArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:spark:Spark', name, {}, opts)

        args = args or SparkArgs()
        self._namespace = Output.from_input(args.namespace)
        release = args.release_name or name

        # ── Operator ──────────────────────────────────────────────────────────

        values = json.loads((CONFIG_DIR / 'helm/helm_values_spark.json').read_text())
        values['spark']['jobNamespaces'] = [args.namespace]
        values = self._deep_merge(values, args.extra_values)

        def ignore_crd_changes(t: pulumi.ResourceTransformArgs) -> pulumi.ResourceTransformResult:
            if t.type_ == 'kubernetes:apiextensions.k8s.io/v1:CustomResourceDefinition':
                t.opts.ignore_changes = ['spec']
            return pulumi.ResourceTransformResult(props=t.props, opts=t.opts)

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='spark-operator',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://kubeflow.github.io/spark-operator'),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self, transforms=[ignore_crd_changes]),
        )

        # ── History Server ────────────────────────────────────────────────────

        hs_name = f'{release}-history-server'
        app_label = {'app': hs_name}
        hs_image = f'{args.history_server_image}:{args.history_server_image_tag}'

        spark_history_opts = Output.all(
            ep=args.s3_endpoint,
            key=args.s3_access_key,
            sec=args.s3_secret_key,
        ).apply(lambda v: (
            f'-Dspark.history.fs.logDirectory=s3a://{args.event_log_bucket}/ '
            f'-Dspark.hadoop.fs.s3a.endpoint={v["ep"]} '
            f'-Dspark.hadoop.fs.s3a.access.key={v["key"]} '
            f'-Dspark.hadoop.fs.s3a.secret.key={v["sec"]} '
            f'-Dspark.hadoop.fs.s3a.path.style.access=true '
            f'-Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'
        ))

        hs_spec = json.loads((CONFIG_DIR / 'resources/history_server_spec.json').read_text())
        hs_spec['selector']['matchLabels']          = app_label
        hs_spec['template']['metadata']['labels']   = app_label
        hs_spec['template']['spec']['containers'][0]['image'] = hs_image
        hs_spec['template']['spec']['containers'][0]['env']   = [
            {'name': 'SPARK_HISTORY_OPTS', 'value': spark_history_opts},
        ]

        Deployment(
            f'{name}-history-server',
            metadata={'name': hs_name, 'namespace': self._namespace},
            spec=hs_spec,
            opts=pulumi.ResourceOptions(parent=self),
        )

        Service(
            f'{name}-history-server-svc',
            metadata={'name': hs_name, 'namespace': self._namespace},
            spec={
                'selector': app_label,
                'ports': [{'port': HISTORY_SERVER_PORT, 'targetPort': HISTORY_SERVER_PORT}],
            },
            opts=pulumi.ResourceOptions(parent=self),
        )

        if args.ingress_enabled and args.ingress_domain:
            host = f'spark.{args.ingress_domain}'

            ingress_spec = json.loads(
                (CONFIG_DIR / 'resources/ingress_spec.json').read_text()
            )
            ingress_spec['ingressClassName'] = args.ingress_class_name
            ingress_spec['rules'][0]['host'] = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend'] = {
                'service': {
                    'name': hs_name,
                    'port': {'number': HISTORY_SERVER_PORT},
                },
            }

            Ingress(
                f'{name}-history-server-ingress',
                metadata={
                    'name': f'{hs_name}-ingress',
                    'namespace': self._namespace,
                    'annotations': {'kubernetes.io/ingress.class': args.ingress_class_name},
                },
                spec=ingress_spec,
                opts=pulumi.ResourceOptions(parent=self),
            )
            self.history_server_url = Output.from_input(f'http://{host}')
        else:
            self.history_server_url = Output.concat(
                'http://', hs_name, '.', self._namespace,
                '.svc.cluster.local:', str(HISTORY_SERVER_PORT),
            )

        self.namespace = self._namespace
        self.register_outputs({
            'namespace': self.namespace,
            'history_server_url': self.history_server_url,
        })

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Spark._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
