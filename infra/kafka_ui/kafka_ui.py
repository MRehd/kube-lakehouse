'''
Kafka UI on Kubernetes — provectus/kafka-ui Helm chart.

Deploys a web UI for inspecting Kafka topics, consumer groups, brokers, and messages.
The bootstrap_servers field accepts a Pulumi Output so you can wire in kafka.bootstrap_servers directly.

Example:
    kafka_ui = KafkaUi('kafka-ui', KafkaUiArgs(
        namespace=ns.metadata.name,
        bootstrap_servers=kafka.bootstrap_servers,
        cluster_name='lakehouse',
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

from config.utils.utils import _deep_merge

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class KafkaUiArgs:
    '''Configuration arguments for Kafka UI deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy Kafka UI into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '0.7.6'
    '''Version of the provectus/kafka-ui Helm chart.'''

    bootstrap_servers: Input[str] = 'kafka:9092'
    '''
    Kafka bootstrap server address.
    Accepts a plain string or a Pulumi Output (e.g. kafka.bootstrap_servers).
    '''

    cluster_name: Input[str] = 'local'
    '''Display name for the Kafka cluster shown in the UI.'''

    replica_count: int = 1
    '''Number of Kafka UI pod replicas.'''

    service_port: int = 80
    '''Service port for the Kafka UI.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '100m', 'memory': '256Mi'},
        'limits':   {'cpu': '500m', 'memory': '512Mi'},
    })
    '''CPU and memory requests/limits for the Kafka UI pod.'''

    ingress_enabled: bool = True
    '''Create an Ingress for external access.'''

    ingress_domain: Optional[Input[str]] = None
    '''Base domain. Creates kafka-ui.<domain>.'''

    ingress_class_name: Input[str] = 'nginx'
    '''Ingress class name.'''

    ingress_annotations: Optional[dict] = None
    '''Extra Ingress annotations.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class KafkaUi(pulumi.ComponentResource):
    '''
    Deploys Kafka UI (provectus/kafka-ui) to Kubernetes using Helm.

    Kafka UI provides a web interface for inspecting topics, consumer groups,
    brokers, and messages in a Kafka cluster. The bootstrap_servers value is
    resolved at deploy time, so Pulumi Outputs are safe to pass directly.

    Outputs:
        namespace — Kubernetes namespace
        endpoint  — Internal cluster URL
        ui_url    — External URL if ingress enabled, else same as endpoint

    Example:
        kafka_ui = KafkaUi('kafka-ui', KafkaUiArgs(
            namespace=ns.metadata.name,
            bootstrap_servers=kafka.bootstrap_servers,
            cluster_name='lakehouse',
            ingress_enabled=True,
            ingress_domain='k8lh.local',
        ))
    '''

    def __init__(
        self,
        name: str,
        args: KafkaUiArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:kafkaui:KafkaUi', name, {}, opts)

        args = args or KafkaUiArgs()
        self._namespace       = Output.from_input(args.namespace)
        bootstrap_servers     = Output.from_input(args.bootstrap_servers)
        release_name          = args.release_name or name

        values = json.loads((CONFIG_DIR / 'helm/helm_values_kafka_ui.json').read_text())
        values['fullnameOverride'] = release_name
        values['replicaCount']     = args.replica_count
        values['resources']        = args.resources
        values['service']['port']  = args.service_port
        values['yamlApplicationConfig'] = {
            'kafka': {'clusters': [{'name': args.cluster_name, 'bootstrapServers': bootstrap_servers}]},
        }

        if args.ingress_enabled and args.ingress_domain:
            values['ingress'] = {
                'enabled':          True,
                'ingressClassName': args.ingress_class_name,
                'host':             Output.concat('kafka-ui.', Output.from_input(args.ingress_domain)),
                'path':             '/',
                'pathType':         'Prefix',
                'annotations':      args.ingress_annotations or {},
                'tls':              {'enabled': False},
            }

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='kafka-ui',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://provectus.github.io/kafka-ui-charts'),
                values=_deep_merge(values, args.extra_values),
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace
        self.endpoint = Output.concat(
            'http://', release_name, '.', self._namespace,
            '.svc.cluster.local:', str(args.service_port),
        )
        self.ui_url = (
            Output.concat('http://kafka-ui.', Output.from_input(args.ingress_domain))
            if args.ingress_enabled and args.ingress_domain
            else self.endpoint
        )

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint':  self.endpoint,
            'ui_url':    self.ui_url,
        })
