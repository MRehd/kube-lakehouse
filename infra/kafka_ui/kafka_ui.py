'''Reusable Kafka UI component for Kubernetes using the provectus/kafka-ui Helm chart.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, TypeVar

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class KafkaUiArgs:
    '''Configuration arguments for Kafka UI deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy Kafka UI into (must already exist).'''

    bootstrap_servers: Input[str] = 'kafka:9092'
    '''Kafka bootstrap server address. Accepts a plain string or Pulumi Output (e.g. kafka.bootstrap_servers).'''

    cluster_name: str = 'local'
    '''Display name for the Kafka cluster shown in the UI.'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    chart_version: str = '0.7.6'
    '''Version of the provectus kafka-ui Helm chart.'''

    replica_count: int = 1
    '''Number of Kafka UI pod replicas.'''

    service_port: int = 80
    '''Service port for the Kafka UI.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '100m', 'memory': '256Mi'},
        'limits':   {'cpu': '500m', 'memory': '512Mi'},
    })
    '''Resource requests and limits for the Kafka UI pod.'''

    ingress_enabled: bool = True
    '''Enable Ingress for external access.'''

    ingress_domain: Optional[str] = None
    '''Domain for Ingress (e.g. 'k8lh.local'). Creates kafka-ui.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name (e.g. 'nginx', 'traefik').'''

    ingress_annotations: Optional[dict] = None
    '''Additional annotations for the Ingress resource.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''


class KafkaUi(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying Kafka UI to Kubernetes using Helm.

    Kafka UI (by Provectus) provides a web interface for inspecting topics,
    consumer groups, brokers, and messages in a Kafka cluster.

    Example:
        ```python
        from kafka_ui import KafkaUi, KafkaUiArgs

        kafka_ui = KafkaUi('my-kafka-ui', KafkaUiArgs(
            namespace='data',
            bootstrap_servers=kafka.bootstrap_servers,
            cluster_name='lakehouse',
            ingress_enabled=True,
            ingress_domain='k8lh.local',
        ))
        ```
    '''

    T = TypeVar('T')

    @staticmethod
    def resolve(value: Input[T]) -> Output[T]:
        '''Convert an Input[T] to Output[T] without modification.

        Use this to normalize values that may be plain types or Outputs
        so you can use .apply() on them consistently.
        '''
        return Output.from_input(value)

    def __init__(
        self,
        name: str,
        args: KafkaUiArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:kafkaui:KafkaUi', name, {}, opts)

        args = args or KafkaUiArgs()
        self._args = args
        self._name = name
        self._release_name = args.release_name or name

        # Resolve Input fields upfront
        self._namespace = self.resolve(args.namespace)
        self._bootstrap_servers = self.resolve(args.bootstrap_servers)

        # bootstrap_servers may be a pulumi.Output, so values are built inside apply()
        values = self._bootstrap_servers.apply(
            lambda bs: self._build_values(args, bs)
        )

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='kafka-ui',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(
                    repo='https://provectus.github.io/kafka-ui-charts',
                ),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace
        self.endpoint = pulumi.Output.concat(
            'http://', self._release_name, '.', self._namespace,
            '.svc.cluster.local:', str(args.service_port)
        )

        if args.ingress_enabled and args.ingress_domain:
            self.ui_url = self.resolve(f'http://kafka-ui.{args.ingress_domain}')
        else:
            self.ui_url = self.endpoint

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint': self.endpoint,
            'ui_url': self.ui_url,
        })

    def _build_values(self, args: KafkaUiArgs, bootstrap_servers: str) -> dict:
        '''Build Helm chart values from KafkaUiArgs and the resolved bootstrap_servers string.'''
        values = json.loads((CONFIG_DIR / 'helm/helm_values_kafka_ui.json').read_text())

        values['fullnameOverride'] = self._release_name
        values['replicaCount'] = args.replica_count
        values['resources'] = args.resources
        values['service']['port'] = args.service_port
        values['yamlApplicationConfig'] = {
            'kafka': {
                'clusters': [
                    {
                        'name': args.cluster_name,
                        'bootstrapServers': bootstrap_servers,
                    }
                ]
            }
        }

        if args.ingress_enabled and args.ingress_domain:
            annotations = args.ingress_annotations or {}
            values['ingress'] = {
                'enabled': True,
                'ingressClassName': args.ingress_class_name,
                'host': f'kafka-ui.{args.ingress_domain}',
                'path': '/',
                'pathType': 'Prefix',
                'annotations': annotations,
                'tls': {'enabled': False},
            }

        values = self._deep_merge(values, args.extra_values)

        return values

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        '''Deep merge two dictionaries, with override taking precedence.'''
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = KafkaUi._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
