'''Reusable Apache Kafka component for Kubernetes using Bitnami Helm chart.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class TopicArgs:
    '''Configuration for a Kafka topic.'''

    name: str
    '''Name of the topic to create.'''

    partitions: int = 3
    '''Number of partitions for the topic.'''

    replicas: int = 1
    '''Replication factor for the topic.'''

    retention_ms: Optional[int] = None
    '''Message retention time in milliseconds.'''

    retention_bytes: Optional[int] = None
    '''Maximum size of the topic in bytes before oldest messages are deleted.'''

    cleanup_policy: str = 'delete'
    '''Cleanup policy: 'delete', 'compact', or 'compact,delete'.'''

    min_insync_replicas: int = 1
    '''Minimum number of in-sync replicas required for writes.'''


@dataclass
class AutoscalingArgs:
    '''Configuration for Kafka broker autoscaling.'''

    enabled: bool = True
    '''Enable autoscaling for Kafka brokers.'''

    min_replicas: int = 1
    '''Minimum number of broker replicas.'''

    max_replicas: int = 5
    '''Maximum number of broker replicas.'''

    target_cpu_utilization: int = 70
    '''Target CPU utilization percentage for scaling.'''

    target_memory_utilization: Optional[int] = None
    '''Target memory utilization percentage for scaling (optional).'''


@dataclass
class KafkaArgs:
    '''Configuration arguments for Kafka deployment.'''

    namespace: Input[str] = 'kafka'
    '''Kubernetes namespace to deploy Kafka into (must already exist).'''

    replicas: int = 3
    '''Number of Kafka broker replicas (combined controller+broker in KRaft mode).'''

    listener_port: int = 9092
    '''Internal listener port for Kafka brokers.'''

    persistence_enabled: bool = True
    '''Enable persistent storage for Kafka data.'''

    persistence_size: str = '10Gi'
    '''Size of the persistent volume for each Kafka broker.'''

    storage_class: Optional[str] = None
    '''Kubernetes storage class to use for persistence.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '1Gi', 'cpu': '500m'},
        'limits': {'memory': '2Gi', 'cpu': '1000m'},
    })
    '''Resource requests and limits for Kafka broker pods.'''

    heap_opts: str = '-Xms512m -Xmx1g'
    '''JVM heap options for Kafka brokers.'''

    log_retention_hours: int = 168
    '''Default log retention time in hours (default: 7 days).'''

    log_retention_bytes: int = -1
    '''Default log retention size in bytes (-1 for unlimited).'''

    num_partitions: int = 3
    '''Default number of partitions for auto-created topics.'''

    default_replication_factor: int = 1
    '''Default replication factor for auto-created topics.'''

    auto_create_topics: bool = True
    '''Allow automatic topic creation.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix.'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    chart_version: str = '32.4.3'
    '''Version of the Bitnami Kafka Helm chart.'''

    autoscaling: AutoscalingArgs = field(default_factory=AutoscalingArgs)
    '''Autoscaling configuration for Kafka brokers.'''

    extra_config: str = ''
    '''Additional Kafka broker configuration (as string, one per line).'''

    topics: List[TopicArgs] = field(default_factory=list)
    '''Topics to create on startup via provisioning.'''


class Kafka(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying Apache Kafka to Kubernetes using Bitnami.

    Bitnami Kafka runs in KRaft mode (no ZooKeeper required) and provides a
    simple, production-ready Kafka deployment.

    Features:
        - KRaft mode (no ZooKeeper required)
        - Built-in autoscaling support
        - Persistent storage with configurable retention
        - Topic provisioning via Helm values

    Example:
        ```python
        from kafka import Kafka, KafkaArgs, AutoscalingArgs, TopicArgs

        kafka = Kafka('my-kafka', KafkaArgs(
            namespace='data',
            replicas=3,
            persistence_size='50Gi',
            autoscaling=AutoscalingArgs(
                enabled=True,
                min_replicas=3,
                max_replicas=10,
                target_cpu_utilization=70,
            ),
            topics=[
                TopicArgs(name='events', partitions=6, replicas=3),
            ],
        ))
        ```
    '''

    def __init__(
        self,
        name: str,
        args: KafkaArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:kafka:Kafka', name, {}, opts)

        args = args or KafkaArgs()
        self._args = args
        self._name = name
        self._release_name = args.release_name or name

        # Resolve Input fields upfront
        self._namespace = Output.from_input(args.namespace)

        # Deploy Kafka cluster via Helm
        self.chart = self._deploy_kafka(args)

        # Export useful outputs
        self.namespace = self._namespace
        self.bootstrap_servers = pulumi.Output.concat(
            self._release_name, '.', self._namespace,
            '.svc.', args.cluster_domain, ':', str(args.listener_port)
        )
        self.bootstrap_endpoint = pulumi.Output.concat(
            self._release_name, ':', str(args.listener_port)
        )

        self.register_outputs({
            'namespace': self.namespace,
            'bootstrap_servers': self.bootstrap_servers,
            'bootstrap_endpoint': self.bootstrap_endpoint,
        })

    def _deploy_kafka(self, args: KafkaArgs) -> Chart:
        '''Deploy Kafka using Bitnami Helm chart.'''
        values = json.loads((CONFIG_DIR / 'helm/helm_values_kafka.json').read_text())

        # Configure release name
        values['fullnameOverride'] = self._release_name

        # Configure controller replicas (combined controller+broker in KRaft mode)
        values['controller']['replicaCount'] = args.replicas
        values['controller']['persistence']['enabled'] = args.persistence_enabled
        values['controller']['persistence']['size'] = args.persistence_size
        if args.storage_class:
            values['controller']['persistence']['storageClass'] = args.storage_class
        values['controller']['resources'] = args.resources
        values['controller']['heapOpts'] = args.heap_opts

        # Configure listeners
        values['listeners']['client']['containerPort'] = args.listener_port

        # Configure log retention
        values['log']['retentionHours'] = args.log_retention_hours
        values['log']['retentionBytes'] = args.log_retention_bytes

        # Configure topic defaults
        values['defaultReplicationFactor'] = args.default_replication_factor
        values['numPartitions'] = args.num_partitions
        values['autoCreateTopicsEnable'] = args.auto_create_topics

        # Additional config
        if args.extra_config:
            values['extraConfig'] = args.extra_config

        # Configure autoscaling
        if args.autoscaling.enabled:
            values['autoscaling']['enabled'] = True
            values['autoscaling']['minReplicas'] = args.autoscaling.min_replicas
            values['autoscaling']['maxReplicas'] = args.autoscaling.max_replicas
            values['autoscaling']['targetCPU'] = args.autoscaling.target_cpu_utilization
            if args.autoscaling.target_memory_utilization:
                values['autoscaling']['targetMemory'] = args.autoscaling.target_memory_utilization

        # Configure topic provisioning
        if args.topics:
            values['provisioning']['enabled'] = True
            values['provisioning']['topics'] = []
            for topic in args.topics:
                topic_config = {
                    'name': topic.name,
                    'partitions': topic.partitions,
                    'replicationFactor': topic.replicas,
                    'config': {
                        'cleanup.policy': topic.cleanup_policy,
                        'min.insync.replicas': topic.min_insync_replicas,
                    },
                }
                if topic.retention_ms is not None:
                    topic_config['config']['retention.ms'] = topic.retention_ms
                if topic.retention_bytes is not None:
                    topic_config['config']['retention.bytes'] = topic.retention_bytes
                values['provisioning']['topics'].append(topic_config)

        def ignore_kraft_changes(args: pulumi.ResourceTransformArgs) -> pulumi.ResourceTransformResult:
            # The KRaft secret data and its checksum annotation in the StatefulSet pod
            # template are regenerated randomly on every chart render. Ignoring them
            # prevents spurious updates and avoids replacing the secret (which would
            # break existing Kafka storage).
            if args.type_ == 'kubernetes:core/v1:Secret' and 'kraft' in (args.name or ''):
                args.opts.ignore_changes = ['data']
            if args.type_ == 'kubernetes:apps/v1:StatefulSet' and 'controller' in (args.name or ''):
                args.opts.ignore_changes = [
                    'spec.template.metadata.annotations',
                    'spec.volumeClaimTemplates',
                ]
            return pulumi.ResourceTransformResult(props=args.props, opts=args.opts)

        return Chart(
            f'{self._name}-kafka',
            ChartOpts(
                chart='oci://registry-1.docker.io/bitnamicharts/kafka',
                version=args.chart_version,
                namespace=self._namespace,
                values=values,
            ),
            opts=pulumi.ResourceOptions(
                parent=self,
                transforms=[ignore_kraft_changes],
            ),
        )
