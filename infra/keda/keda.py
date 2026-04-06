'''Reusable KEDA (Kubernetes Event-Driven Autoscaling) component for Kubernetes.'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.apiextensions import CustomResource
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts

# Config directory for templates
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class KafkaTriggerArgs:
    '''Configuration for a KEDA Kafka consumer-lag trigger.'''

    bootstrap_servers: Input[str]
    '''Kafka bootstrap server address (e.g. kafka-svc:9092). Accepts a plain string or Pulumi Output.'''

    consumer_group: str
    '''Kafka consumer group ID to monitor for lag.'''

    topic: str
    '''Kafka topic to watch.'''

    lag_threshold: int = 10
    '''Number of messages behind before scaling up.'''

    offset_reset_policy: str = 'latest'
    '''Offset reset policy: 'latest' or 'earliest'.'''

    partition_limitation: Optional[int] = None
    '''Limit scaling decisions to N partitions. None means all partitions.'''


@dataclass
class ScaledObjectArgs:
    '''Configuration for a KEDA ScaledObject targeting a Kubernetes workload.'''

    name: str
    '''Name for the ScaledObject Kubernetes resource.'''

    target_name: str
    '''Name of the Deployment or StatefulSet to scale.'''

    triggers: List[KafkaTriggerArgs]
    '''One or more Kafka consumer-lag triggers that drive scaling.'''

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

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values to pass to the chart.'''


class Keda(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for deploying KEDA to Kubernetes using Helm.

    KEDA (Kubernetes Event-Driven Autoscaling) extends Kubernetes with event-driven
    scaling via custom ScaledObject resources. This component deploys the KEDA
    operator and exposes a create_scaled_object() method for registering workload
    scalers driven by Kafka consumer lag.

    Example:
        ```python
        from keda import Keda, KedaArgs, KafkaTriggerArgs, ScaledObjectArgs

        keda = Keda('my-keda', KedaArgs(
            namespace='data',
            operator_replicas=2,
        ))

        keda.create_scaled_object(
            'spark-scaler',
            ScaledObjectArgs(
                name='spark-worker-scaler',
                target_name='spark-worker',
                target_kind='Deployment',
                min_replica_count=1,
                max_replica_count=10,
                triggers=[
                    KafkaTriggerArgs(
                        bootstrap_servers=kafka.bootstrap_servers,
                        consumer_group='spark-group',
                        topic='events',
                        lag_threshold=10,
                    ),
                ],
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

        # Resolve Input fields upfront
        self._namespace = Output.from_input(args.namespace)

        # Build Helm values from args
        values = self._build_values(args)

        # Deploy KEDA operator via Helm
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
        Create a KEDA ScaledObject that scales a workload based on Kafka consumer lag.

        The ScaledObject is a CRD (installed by the KEDA Helm chart). It watches
        one or more Kafka consumer groups and adjusts the replica count of the
        target Deployment or StatefulSet when lag crosses the threshold.

        Args:
            name: Unique Pulumi resource name.
            args: ScaledObject configuration including target and triggers.
            opts: Optional Pulumi resource options.

        Returns:
            The created ScaledObject CustomResource.

        Example:
            ```python
            keda.create_scaled_object(
                'flink-scaler',
                ScaledObjectArgs(
                    name='flink-taskmanager-scaler',
                    target_name='flink-taskmanager',
                    target_kind='Deployment',
                    min_replica_count=1,
                    max_replica_count=8,
                    triggers=[
                        KafkaTriggerArgs(
                            bootstrap_servers=kafka.bootstrap_servers,
                            consumer_group='flink-group',
                            topic='events',
                            lag_threshold=20,
                        ),
                    ],
                ),
                opts=pulumi.ResourceOptions(depends_on=[kafka]),
            )
            ```
        '''
        # Collect all bootstrap_servers Inputs — they may be pulumi.Output[str]
        # (e.g. kafka.bootstrap_servers), so they must be resolved before building the spec.
        bs_inputs = {f'bs_{i}': t.bootstrap_servers for i, t in enumerate(args.triggers)}
        resolved = pulumi.Output.all(**bs_inputs)

        def build_spec(r: dict) -> dict:
            triggers = []
            for i, trigger in enumerate(args.triggers):
                metadata = {
                    'bootstrapServers': r[f'bs_{i}'],
                    'consumerGroup': trigger.consumer_group,
                    'topic': trigger.topic,
                    'lagThreshold': str(trigger.lag_threshold),
                    'offsetResetPolicy': trigger.offset_reset_policy,
                }
                if trigger.partition_limitation is not None:
                    metadata['partitionLimitation'] = str(trigger.partition_limitation)
                triggers.append({'type': 'kafka', 'metadata': metadata})

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
            spec=resolved.apply(build_spec),
            opts=resource_opts,
        )
