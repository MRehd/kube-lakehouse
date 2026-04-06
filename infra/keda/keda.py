'''Reusable KEDA (Kubernetes Event-Driven Autoscaling) component for Kubernetes.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apiextensions import CustomResource
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class TriggerArgs:
    '''A generic KEDA trigger. Metadata values may be plain strings or Pulumi Outputs.'''

    type: str
    '''KEDA trigger type (e.g. 'kafka', 'cpu', 'prometheus', 'redis').'''

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

    Args:
        bootstrap_servers: Kafka bootstrap address. Accepts a plain string or Pulumi Output.
        consumer_group: Kafka consumer group ID to monitor for lag.
        topic: Kafka topic to watch.
        lag_threshold: Number of messages behind before scaling up.
        offset_reset_policy: 'latest' or 'earliest'.
        partition_limitation: Limit scaling to N partitions. None means all partitions.

    Returns:
        A TriggerArgs configured for the KEDA Kafka scaler.

    Example:
        ```python
        KafkaTrigger(
            bootstrap_servers=kafka.bootstrap_servers,
            consumer_group='spark-group',
            topic='events',
            lag_threshold=10,
        )
        ```
    '''
    metadata: Dict[str, Input[str]] = {
        'bootstrapServers': bootstrap_servers,
        'consumerGroup': consumer_group,
        'topic': topic,
        'lagThreshold': str(lag_threshold),
        'offsetResetPolicy': offset_reset_policy,
    }
    if partition_limitation is not None:
        metadata['partitionLimitation'] = str(partition_limitation)

    return TriggerArgs(type='kafka', metadata=metadata)


@dataclass
class ScaledObjectArgs:
    '''Configuration for a KEDA ScaledObject targeting a Kubernetes workload.'''

    name: str
    '''Name for the ScaledObject Kubernetes resource.'''

    target_name: str
    '''Name of the Deployment or StatefulSet to scale.'''

    triggers: List[TriggerArgs]
    '''One or more triggers that drive scaling.'''

    target_kind: str = 'Deployment'
    '''Kind of the scale target: 'Deployment' or 'StatefulSet'.'''

    target_api_version: str = 'apps/v1'
    '''API version of the scale target resource.'''

    min_replica_count: int = 1
    '''Minimum number of replicas (KEDA will not scale below this).'''

    max_replica_count: int = 10
    '''Maximum number of replicas (KEDA will not scale above this).'''

    polling_interval: int = 30
    '''Seconds between trigger metric checks.'''

    cooldown_period: int = 300
    '''Seconds to wait after the last active trigger before scaling down to minimum.'''


@dataclass
class KedaArgs:
    '''Configuration arguments for KEDA operator deployment.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy KEDA into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name (controls K8s resource names). If not set, uses the Pulumi resource name.'''

    chart_version: str = '2.16.1'
    '''Version of the kedacore/keda Helm chart to deploy.'''

    operator_replicas: int = 2
    '''Number of KEDA operator replicas (supports leader election for HA).'''

    metrics_server_replicas: int = 1
    '''Number of KEDA metrics server replicas.'''

    watch_namespace: str = ''
    '''Namespace for KEDA to watch. Empty string means cluster-wide.'''

    resources: dict = field(default_factory=lambda: {
        'requests': {'cpu': '100m', 'memory': '128Mi'},
        'limits':   {'cpu': '500m', 'memory': '512Mi'},
    })
    '''Resource requests and limits for KEDA operator and metrics server pods.'''

    deploy_metrics_server: bool = True
    '''Deploy the Kubernetes Metrics Server alongside KEDA. Required for cpu/memory triggers.'''

    metrics_server_version: str = '3.12.2'
    '''Version of the kubernetes-sigs/metrics-server Helm chart.'''

    metrics_server_kubelet_insecure_tls: bool = True
    '''Pass --kubelet-insecure-tls to the Metrics Server. Required for most local/dev clusters.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''


class Keda(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying KEDA to Kubernetes using Helm.

    KEDA (Kubernetes Event-Driven Autoscaling) extends Kubernetes with event-driven
    scaling via custom ScaledObject resources. This component deploys the KEDA
    operator and exposes a create_scaled_object() method for wiring any KEDA-supported
    trigger type to a workload.

    Example:
        ```python
        from keda import Keda, KedaArgs, KafkaTrigger, ScaledObjectArgs, TriggerArgs

        keda = Keda('my-keda', KedaArgs(namespace='data'))

        # Kafka trigger (built-in factory)
        keda.create_scaled_object(
            'spark-scaler',
            ScaledObjectArgs(
                name='spark-worker-scaler',
                target_name='spark-worker',
                triggers=[KafkaTrigger(
                    bootstrap_servers=kafka.bootstrap_servers,
                    consumer_group='spark-group',
                    topic='events',
                )],
            ),
        )

        # Any other KEDA trigger type via TriggerArgs directly
        keda.create_scaled_object(
            'cpu-scaler',
            ScaledObjectArgs(
                name='worker-cpu-scaler',
                target_name='worker',
                triggers=[TriggerArgs(
                    type='cpu',
                    metadata={'type': 'Utilization', 'value': '70'},
                )],
            ),
        )
        ```
    '''

    def __init__(
        self,
        name: str,
        args: KedaArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:keda:Keda', name, {}, opts)

        args = args or KedaArgs()
        self._args = args
        self._name = name
        self._release_name = args.release_name or name

        self._namespace = Output.from_input(args.namespace)

        values = self._build_values(args)

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='keda',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(
                    repo='https://kedacore.github.io/charts',
                ),
                values=values,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace

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

        self.register_outputs({
            'namespace': self.namespace,
        })

    def _build_values(self, args: KedaArgs) -> dict:
        '''Build Helm chart values from KedaArgs.'''
        values = json.loads((CONFIG_DIR / 'helm/helm_values_keda.json').read_text())

        values['fullnameOverride'] = self._release_name
        values['watchNamespace'] = args.watch_namespace
        values['operator']['replicaCount'] = args.operator_replicas
        values['operator']['resources'] = args.resources
        values['metricsServer']['replicaCount'] = args.metrics_server_replicas
        values['metricsServer']['resources'] = args.resources

        values = self._deep_merge(values, args.extra_values)

        return values

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> dict:
        '''Deep merge two dictionaries, with override taking precedence.'''
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = Keda._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def create_scaled_object(
        self,
        name: str,
        args: ScaledObjectArgs,
        opts: pulumi.ResourceOptions = None,
    ) -> CustomResource:
        '''
        Create a KEDA ScaledObject for any supported trigger type.

        Metadata values in each TriggerArgs may be plain strings or Pulumi Outputs —
        all are resolved transparently before the ScaledObject spec is built.

        Args:
            name: Unique Pulumi resource name.
            args: ScaledObject configuration including target and triggers.
            opts: Optional Pulumi resource options.

        Returns:
            The created ScaledObject CustomResource.
        '''
        # Collect every metadata Input[str] across all triggers, keyed uniquely.
        # Plain strings are wrapped by Output.all transparently.
        all_inputs = {
            f't{i}_{key}': val
            for i, trigger in enumerate(args.triggers)
            for key, val in trigger.metadata.items()
        }

        def build_spec(resolved: dict) -> dict:
            triggers = [
                {
                    'type': trigger.type,
                    'metadata': {
                        key: resolved[f't{i}_{key}']
                        for key in trigger.metadata
                    },
                }
                for i, trigger in enumerate(args.triggers)
            ]
            return {
                'scaleTargetRef': {
                    'apiVersion': args.target_api_version,
                    'kind': args.target_kind,
                    'name': args.target_name,
                },
                'minReplicaCount': args.min_replica_count,
                'maxReplicaCount': args.max_replica_count,
                'pollingInterval': args.polling_interval,
                'cooldownPeriod': args.cooldown_period,
                'triggers': triggers,
            }

        resource_opts = pulumi.ResourceOptions(
            parent=self,
            depends_on=[self.chart],
        )
        if opts:
            resource_opts = pulumi.ResourceOptions.merge(resource_opts, opts)

        return CustomResource(
            f'{name}-scaledobject',
            api_version='keda.sh/v1alpha1',
            kind='ScaledObject',
            metadata={
                'name': args.name,
                'namespace': self._namespace,
            },
            spec=pulumi.Output.all(**all_inputs).apply(build_spec),
            opts=resource_opts,
        )
