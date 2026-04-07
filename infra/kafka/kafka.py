'''
Apache Kafka on Kubernetes — Bitnami Helm chart (KRaft mode, no ZooKeeper).

Deploys a Kafka cluster in KRaft mode with optional autoscaling and
topic provisioning via Helm values.

Example:
    kafka = Kafka('kafka', KafkaArgs(
        namespace=ns.metadata.name,
        replicas=3,
        persistence_size='20Gi',
        topics=[
            TopicArgs(name='btc-prices', partitions=6, replicas=3),
        ],
    ))

    # Use kafka.bootstrap_servers as Input[str] in other components:
    #   kafka_ui bootstrap_servers, KEDA KafkaTrigger, Airflow env vars, etc.
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts

CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class TopicArgs:
    '''Configuration for a Kafka topic provisioned at deploy time.'''

    name: str
    '''Topic name.'''

    partitions: int = 3
    '''Number of partitions.'''

    replicas: int = 1
    '''Replication factor. Should not exceed the number of brokers.'''

    retention_ms: Optional[int] = None
    '''Message retention in milliseconds. None uses the broker default.'''

    retention_bytes: Optional[int] = None
    '''Max topic size in bytes before oldest messages are deleted. None = unlimited.'''

    cleanup_policy: str = 'delete'
    '''Cleanup policy: "delete", "compact", or "compact,delete".'''

    min_insync_replicas: int = 1
    '''Minimum in-sync replicas required for writes to succeed.'''


@dataclass
class AutoscalingArgs:
    '''HPA configuration for Kafka broker pods.'''

    enabled: bool = True
    '''Enable the Horizontal Pod Autoscaler.'''

    min_replicas: int = 1
    '''Minimum broker replicas.'''

    max_replicas: int = 5
    '''Maximum broker replicas.'''

    target_cpu_utilization: int = 70
    '''Target CPU utilization percentage for scale-up.'''

    target_memory_utilization: Optional[int] = None
    '''Target memory utilization percentage. None omits this metric.'''


@dataclass
class KafkaArgs:
    '''Configuration arguments for Kafka deployment.'''

    namespace: Input[str] = 'kafka'
    '''Kubernetes namespace to deploy Kafka into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '32.4.3'
    '''Version of the Bitnami Kafka Helm chart.'''

    replicas: int = 3
    '''Number of Kafka broker replicas (combined controller+broker in KRaft mode).'''

    listener_port: int = 9092
    '''Internal Kafka listener port.'''

    persistence_enabled: bool = True
    '''Mount a PersistentVolumeClaim per broker for durable log storage.'''

    persistence_size: str = '10Gi'
    '''PVC size per broker.'''

    storage_class: Optional[str] = None
    '''StorageClass for broker PVCs. None uses the cluster default.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'memory': '1Gi',  'cpu': '500m'},
        'limits':   {'memory': '2Gi',  'cpu': '1000m'},
    })
    '''CPU and memory requests/limits for broker pods.'''

    heap_opts: str = '-Xms512m -Xmx1g'
    '''JVM heap options for broker pods.'''

    log_retention_hours: int = 168
    '''Log retention duration in hours (default: 7 days).'''

    log_retention_bytes: int = -1
    '''Max log size in bytes per partition (-1 = unlimited).'''

    num_partitions: int = 3
    '''Default number of partitions for auto-created topics.'''

    default_replication_factor: int = 1
    '''Default replication factor for auto-created topics.'''

    auto_create_topics: bool = True
    '''Allow Kafka to create topics automatically on first use.'''

    cluster_domain: str = 'cluster.local'
    '''Kubernetes cluster domain suffix.'''

    autoscaling: AutoscalingArgs = field(default_factory=AutoscalingArgs)
    '''HPA configuration for broker pods.'''

    topics: List[TopicArgs] = field(default_factory=list)
    '''Topics to create at deploy time via Helm provisioning.'''

    extra_config: str = ''
    '''Additional broker configuration lines (appended to server.properties).'''


class Kafka(pulumi.ComponentResource):
    '''
    Deploys Apache Kafka to Kubernetes using the Bitnami Helm chart in KRaft mode.

    KRaft mode runs without ZooKeeper — each broker also acts as a controller.
    Spurious updates to the KRaft secret and StatefulSet annotations are ignored
    via resource transforms to prevent unnecessary pod restarts.

    Outputs:
        namespace          — Kubernetes namespace
        bootstrap_servers  — Full internal bootstrap address (host:port)
        bootstrap_endpoint — Short form (release:port) for same-namespace clients

    Example:
        kafka = Kafka('kafka', KafkaArgs(
            namespace=ns.metadata.name,
            replicas=3,
            topics=[TopicArgs(name='events', partitions=6, replicas=3)],
        ))
    '''

    def __init__(
        self,
        name: str,
        args: KafkaArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:kafka:Kafka', name, {}, opts)

        args = args or KafkaArgs()
        release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)

        # ── Helm values ───────────────────────────────────────────────────────
        values = json.loads((CONFIG_DIR / 'helm/helm_values_kafka.json').read_text())

        values['fullnameOverride']                          = release_name
        values['controller']['replicaCount']                = args.replicas
        values['controller']['persistence']['enabled']      = args.persistence_enabled
        values['controller']['persistence']['size']         = args.persistence_size
        values['controller']['resources']                   = args.resources
        values['controller']['heapOpts']                    = args.heap_opts
        values['listeners']['client']['containerPort']      = args.listener_port
        values['log']['retentionHours']                     = args.log_retention_hours
        values['log']['retentionBytes']                     = args.log_retention_bytes
        values['defaultReplicationFactor']                  = args.default_replication_factor
        values['numPartitions']                             = args.num_partitions
        values['autoCreateTopicsEnable']                    = args.auto_create_topics

        if args.storage_class:
            values['controller']['persistence']['storageClass'] = args.storage_class
        if args.extra_config:
            values['extraConfig'] = args.extra_config

        if args.autoscaling.enabled:
            values['autoscaling']['enabled']     = True
            values['autoscaling']['minReplicas']  = args.autoscaling.min_replicas
            values['autoscaling']['maxReplicas']  = args.autoscaling.max_replicas
            values['autoscaling']['targetCPU']    = args.autoscaling.target_cpu_utilization
            if args.autoscaling.target_memory_utilization:
                values['autoscaling']['targetMemory'] = args.autoscaling.target_memory_utilization

        if args.topics:
            values['provisioning']['enabled'] = True
            values['provisioning']['topics']  = []
            for topic in args.topics:
                topic_config = {
                    'name':              topic.name,
                    'partitions':        topic.partitions,
                    'replicationFactor': topic.replicas,
                    'config': {
                        'cleanup.policy':    topic.cleanup_policy,
                        'min.insync.replicas': topic.min_insync_replicas,
                    },
                }
                if topic.retention_ms is not None:
                    topic_config['config']['retention.ms'] = topic.retention_ms
                if topic.retention_bytes is not None:
                    topic_config['config']['retention.bytes'] = topic.retention_bytes
                values['provisioning']['topics'].append(topic_config)

        # ── Chart ─────────────────────────────────────────────────────────────
        # The KRaft secret data and the StatefulSet's checksum annotation are
        # regenerated randomly on every chart render. Ignore them to prevent
        # spurious updates that would replace the secret and break existing brokers.
        def ignore_kraft_changes(t: pulumi.ResourceTransformArgs) -> pulumi.ResourceTransformResult:
            if t.type_ == 'kubernetes:core/v1:Secret' and 'kraft' in (t.name or ''):
                t.opts.ignore_changes = ['data']
            if t.type_ == 'kubernetes:apps/v1:StatefulSet' and 'controller' in (t.name or ''):
                t.opts.ignore_changes = [
                    'spec.template.metadata.annotations',
                    'spec.volumeClaimTemplates',
                ]
            return pulumi.ResourceTransformResult(props=t.props, opts=t.opts)

        self.chart = Chart(
            f'{name}-kafka',
            ChartOpts(
                chart='oci://registry-1.docker.io/bitnamicharts/kafka',
                version=args.chart_version,
                namespace=self._namespace,
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self, transforms=[ignore_kraft_changes]),
        )

        # ── Outputs ───────────────────────────────────────────────────────────
        self.namespace = self._namespace
        self.bootstrap_servers = Output.concat(
            release_name, '.', self._namespace,
            '.svc.', args.cluster_domain, ':', str(args.listener_port),
        )
        self.bootstrap_endpoint = Output.concat(
            release_name, ':', str(args.listener_port),
        )

        self.register_outputs({
            'namespace':          self.namespace,
            'bootstrap_servers':  self.bootstrap_servers,
            'bootstrap_endpoint': self.bootstrap_endpoint,
        })
