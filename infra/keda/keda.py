'''
KEDA (Kubernetes Event-Driven Autoscaling) on Kubernetes — kedacore Helm chart.

Deploys the KEDA operator and exposes create_scaled_object() to attach any
KEDA-supported trigger type to a workload. KafkaTrigger() is a convenience
factory for the most common use case.

Example:
    keda = Keda('keda', KedaArgs(namespace=ns.metadata.name))

    keda.create_scaled_object('producer-scaler', ScaledObjectArgs(
        name='producer-scaler',
        target_name='producer',
        triggers=[KafkaTrigger(
            bootstrap_servers=kafka.bootstrap_servers,
            consumer_group='my-group',
            topic='events',
            lag_threshold=10,
        )],
    ))
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apiextensions import CustomResource
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
class TriggerArgs:
    '''A generic KEDA trigger. Metadata values may be plain strings or Pulumi Outputs.'''

    type: str
    '''KEDA scaler type (e.g. "kafka", "cpu", "prometheus", "redis").'''

    metadata: Dict[str, Input[str]]
    '''Trigger metadata key-value pairs as defined by the KEDA scaler spec.'''


def KafkaTrigger(
    bootstrap_servers: Input[str],
    consumer_group: str,
    topic: str,
    lag_threshold: int = 10,
    offset_reset_policy: str = 'latest',
    partition_limitation: Optional[int] = None,
) -> TriggerArgs:
    '''
    Build a KEDA Kafka consumer-lag trigger.

    Scales a workload based on how far behind a consumer group is on a topic.
    bootstrap_servers accepts a Pulumi Output so you can wire in kafka.bootstrap_servers directly.

    Args:
        bootstrap_servers:    Kafka bootstrap address. Accepts a Pulumi Output.
        consumer_group:       Kafka consumer group ID to monitor.
        topic:                Kafka topic to watch.
        lag_threshold:        Messages behind per partition before scaling up (default: 10).
        offset_reset_policy:  "latest" or "earliest".
        partition_limitation: Limit scaling to N partitions. None means all partitions.

    Returns:
        TriggerArgs configured for the KEDA Kafka scaler.

    Example:
        KafkaTrigger(
            bootstrap_servers=kafka.bootstrap_servers,
            consumer_group='spark-group',
            topic='events',
            lag_threshold=10,
        )
    '''
    metadata: Dict[str, Input[str]] = {
        'bootstrapServers':  bootstrap_servers,
        'consumerGroup':     consumer_group,
        'topic':             topic,
        'lagThreshold':      str(lag_threshold),
        'offsetResetPolicy': offset_reset_policy,
    }
    if partition_limitation is not None:
        metadata['partitionLimitation'] = str(partition_limitation)
    return TriggerArgs(type='kafka', metadata=metadata)


@dataclass
class ScaledObjectArgs:
    '''Configuration for a KEDA ScaledObject targeting a Kubernetes workload.'''

    name: str
    '''Name for the ScaledObject K8s resource.'''

    target_name: str
    '''Name of the Deployment or StatefulSet to scale.'''

    triggers: List[TriggerArgs]
    '''One or more triggers that drive scaling decisions.'''

    target_kind: str = 'Deployment'
    '''Kind of the scale target: "Deployment" or "StatefulSet".'''

    target_api_version: str = 'apps/v1'
    '''API version of the scale target resource.'''

    min_replica_count: int = 1
    '''Minimum replicas — KEDA will not scale below this.'''

    max_replica_count: int = 10
    '''Maximum replicas — KEDA will not scale above this.'''

    polling_interval: int = 30
    '''Seconds between trigger metric checks.'''

    cooldown_period: int = 300
    '''Seconds to wait after the last active trigger before scaling back to minimum.'''


@dataclass
class KedaArgs:
    '''Configuration arguments for KEDA operator deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy KEDA into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name — controls K8s resource names. Defaults to the Pulumi resource name.'''

    chart_version: str = '2.16.1'
    '''Version of the kedacore/keda Helm chart.'''

    operator_replicas: int = 2
    '''Number of KEDA operator replicas (supports leader election for HA).'''

    metrics_server_replicas: int = 1
    '''Number of KEDA metrics server replicas.'''

    watch_namespace: str = ''
    '''Namespace KEDA watches. Empty string = cluster-wide.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '100m', 'memory': '128Mi'},
        'limits':   {'cpu': '500m', 'memory': '512Mi'},
    })
    '''CPU and memory requests/limits for the KEDA operator and metrics server pods.'''

    deploy_metrics_server: bool = True
    '''Deploy the Kubernetes Metrics Server alongside KEDA. Required for cpu/memory triggers.'''

    metrics_server_version: str = '3.12.2'
    '''Version of the kubernetes-sigs/metrics-server Helm chart.'''

    metrics_server_kubelet_insecure_tls: bool = True
    '''Pass --kubelet-insecure-tls to the Metrics Server. Required on most local/dev clusters.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values deep-merged over the base config.'''


class Keda(pulumi.ComponentResource):
    '''
    Deploys KEDA to Kubernetes and exposes create_scaled_object() to attach
    event-driven scaling to any workload.

    KEDA extends Kubernetes with ScaledObject resources that poll external
    metrics (Kafka lag, CPU, Prometheus, etc.) and adjust replica counts.
    The optional Metrics Server is deployed alongside for cpu/memory triggers.

    Outputs:
        namespace — Kubernetes namespace

    Example:
        keda = Keda('keda', KedaArgs(namespace=ns.metadata.name))

        keda.create_scaled_object('events-scaler', ScaledObjectArgs(
            name='events-worker-scaler',
            target_name='events-worker',
            triggers=[KafkaTrigger(
                bootstrap_servers=kafka.bootstrap_servers,
                consumer_group='events-group',
                topic='events',
            )],
        ))
    '''

    def __init__(
        self,
        name: str,
        args: KedaArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:keda:Keda', name, {}, opts)

        args = args or KedaArgs()
        release_name = args.release_name or name
        self._namespace = Output.from_input(args.namespace)

        # ── Helm values ───────────────────────────────────────────────────────
        values = json.loads((CONFIG_DIR / 'helm/helm_values_keda.json').read_text())

        values['fullnameOverride']              = release_name
        values['watchNamespace']                = args.watch_namespace
        values['operator']['replicaCount']      = args.operator_replicas
        values['operator']['resources']         = args.resources
        values['metricsServer']['replicaCount'] = args.metrics_server_replicas
        values['metricsServer']['resources']    = args.resources

        values = _deep_merge(values, args.extra_values)

        # ── Chart ─────────────────────────────────────────────────────────────
        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='keda',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://kedacore.github.io/charts'),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        # ── Optional Metrics Server ───────────────────────────────────────────
        if args.deploy_metrics_server:
            ms_values = {}
            if args.metrics_server_kubelet_insecure_tls:
                ms_values['args'] = ['--kubelet-insecure-tls']
            Chart(
                f'{name}-metrics-server',
                ChartOpts(
                    chart='metrics-server',
                    version=args.metrics_server_version,
                    namespace=self._namespace,
                    fetch_opts=FetchOpts(
                        repo='https://kubernetes-sigs.github.io/metrics-server/',
                    ),
                    values=ms_values,
                ),
                opts=pulumi.ResourceOptions(parent=self),
            )

        self.namespace = self._namespace
        self.register_outputs({'namespace': self.namespace})

    def create_scaled_object(
        self,
        name: str,
        args: ScaledObjectArgs,
        opts: pulumi.ResourceOptions = None,
    ) -> CustomResource:
        '''
        Create a KEDA ScaledObject for any supported trigger type.

        Trigger metadata values may be plain strings or Pulumi Outputs — all are
        resolved transparently via Output.all() before the spec is assembled.

        Args:
            name: Pulumi resource name prefix.
            args: ScaledObject configuration including target workload and triggers.
            opts: Optional extra resource options.

        Returns:
            The created ScaledObject CustomResource.

        Example:
            keda.create_scaled_object('lag-scaler', ScaledObjectArgs(
                name='events-worker-scaler',
                target_name='events-worker',
                min_replica_count=0,
                max_replica_count=5,
                triggers=[KafkaTrigger(
                    bootstrap_servers=kafka.bootstrap_servers,
                    consumer_group='events-group',
                    topic='events',
                    lag_threshold=5,
                )],
            ))
        '''
        # Flatten all trigger metadata Input[str] values into a single keyed dict
        # so they can all be resolved in one Output.all() call.
        all_inputs = {
            f't{i}_{key}': val
            for i, trigger in enumerate(args.triggers)
            for key, val in trigger.metadata.items()
        }

        def build_spec(resolved: dict) -> dict:
            return {
                'scaleTargetRef': {
                    'apiVersion': args.target_api_version,
                    'kind':       args.target_kind,
                    'name':       args.target_name,
                },
                'minReplicaCount': args.min_replica_count,
                'maxReplicaCount': args.max_replica_count,
                'pollingInterval': args.polling_interval,
                'cooldownPeriod':  args.cooldown_period,
                'triggers': [
                    {
                        'type': trigger.type,
                        'metadata': {
                            key: resolved[f't{i}_{key}']
                            for key in trigger.metadata
                        },
                    }
                    for i, trigger in enumerate(args.triggers)
                ],
            }

        resource_opts = pulumi.ResourceOptions(parent=self, depends_on=[self.chart])
        if opts:
            resource_opts = pulumi.ResourceOptions.merge(resource_opts, opts)

        return CustomResource(
            f'{name}-scaledobject',
            api_version='keda.sh/v1alpha1',
            kind='ScaledObject',
            metadata={'name': args.name, 'namespace': self._namespace},
            spec=pulumi.Output.all(**all_inputs).apply(build_spec),
            opts=resource_opts,
        )
