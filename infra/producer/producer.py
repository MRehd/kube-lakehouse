'''
FastAPI Kafka producer — builds a Docker image and deploys it to Kubernetes.

Builds the producer image from infra/producer/app/, pushes it to a registry,
and deploys a Deployment + Service (+ optional Ingress). The FastAPI app exposes
/start-stream and /stop-stream endpoints to control Kafka producers at runtime —
no Kafka config is needed at pod startup; it is passed as env vars.

Example:
    producer = Producer('producer', ProducerArgs(
        namespace=ns.metadata.name,
        image_name='docker.io/myuser/producer',
        registry_username=config.require('docker_registry_username'),
        registry_password=config.require_secret('docker_registry_password'),
        env={
            'KAFKA_BOOTSTRAP_SERVERS': kafka.bootstrap_servers,
            'KAFKA_TOPIC':             'btc-prices',
        },
        ingress_enabled=True,
        ingress_domain='k8lh.local',
    ))
'''

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

import json
import pulumi
import pulumi_docker as docker
from pulumi import Input, Output
from pulumi_kubernetes.apps.v1 import Deployment
from pulumi_kubernetes.core.v1 import Service
from pulumi_kubernetes.networking.v1 import Ingress

BUILD_CONTEXT = str(Path(__file__).parent / 'app')
CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class ProducerArgs:
    '''Configuration arguments for the producer deployment.'''

    namespace: Input[str]
    '''Kubernetes namespace to deploy into (must already exist).'''

    image_name: str
    '''Full image name without tag, e.g. "docker.io/myuser/producer".'''

    registry_username: Input[str]
    '''Docker registry username. Leave empty to skip push (local dev).'''

    registry_password: Input[str]
    '''Docker registry password or access token. Accepts a Pulumi secret Output.'''

    registry_server: str = 'https://index.docker.io/v1/'
    '''Docker registry server URL.'''

    image_tag: str = 'latest'
    '''Image tag to build and push.'''

    port: int = 8000
    '''Container port exposed by the FastAPI app.'''

    replicas: int = 1
    '''Number of pod replicas.'''

    cpu_request: str = '100m'
    '''CPU request for the pod (e.g. "100m").'''

    cpu_limit: str = '500m'
    '''CPU limit for the pod.'''

    memory_request: str = '128Mi'
    '''Memory request for the pod.'''

    memory_limit: str = '256Mi'
    '''Memory limit for the pod.'''

    env: Dict[str, Input[str]] = field(default_factory=dict)
    '''Environment variables to inject into the pod. Values may be Pulumi Outputs (e.g. kafka.bootstrap_servers).'''

    ingress_enabled: bool = False
    '''Create an Ingress for external access.'''

    ingress_domain: str = ''
    '''Base domain. Creates producer.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''


class Producer(pulumi.ComponentResource):
    '''
    Builds the producer Docker image, pushes it to a registry, and deploys it
    to Kubernetes as a Deployment + Service (+ optional Ingress).

    The FastAPI app exposes /start-stream and /stop-stream endpoints to control
    Kafka producers at runtime — no Kafka config needed at pod startup.
    '''

    def __init__(
        self,
        name: str,
        args: ProducerArgs,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:producer:Producer', name, {}, opts)

        self._namespace      = Output.from_input(args.namespace)
        registry_username    = Output.from_input(args.registry_username)
        registry_password    = Output.from_input(args.registry_password)
        env_values           = {k: Output.from_input(v) for k, v in args.env.items()}
        full_image           = f'{args.image_name}:{args.image_tag}'

        image = docker.Image(
            f'{name}-image',
            image_name=full_image,
            build=docker.DockerBuildArgs(
                context=BUILD_CONTEXT,
                dockerfile=f'{BUILD_CONTEXT}/dockerfile',
            ),
            registry=docker.RegistryArgs(
                server=args.registry_server,
                username=registry_username,
                password=registry_password,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        env_vars = [{'name': k, 'value': v} for k, v in env_values.items()]

        Deployment(
            f'{name}-deployment',
            metadata={'namespace': self._namespace, 'name': name},
            spec={
                'replicas': args.replicas,
                'selector': {'matchLabels': {'app': name}},
                'template': {
                    'metadata': {'labels': {'app': name}},
                    'spec': {
                        'containers': [{
                            'name': 'producer',
                            'image': image.image_name,
                            'ports': [{'containerPort': args.port}],
                            'env': env_vars,
                            'resources': {
                                'requests': {'cpu': args.cpu_request, 'memory': args.memory_request},
                                'limits':   {'cpu': args.cpu_limit,   'memory': args.memory_limit},
                            },
                        }],
                    },
                },
            },
            opts=pulumi.ResourceOptions(parent=self, depends_on=[image]),
        )

        Service(
            f'{name}-service',
            metadata={'namespace': self._namespace, 'name': name},
            spec={
                'selector': {'app': name},
                'ports': [{'port': args.port, 'targetPort': args.port}],
            },
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.endpoint = Output.concat('http://', name, ':', str(args.port))

        if args.ingress_enabled and args.ingress_domain:
            host = f'producer.{args.ingress_domain}'

            ingress_spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            ingress_spec['ingressClassName'] = args.ingress_class_name
            ingress_spec['rules'][0]['host'] = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['name'] = name
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'] = args.port

            Ingress(
                f'{name}-ingress',
                metadata={
                    'namespace': self._namespace,
                    'name': f'{name}-ingress',
                    'annotations': {'kubernetes.io/ingress.class': args.ingress_class_name},
                },
                spec=ingress_spec,
                opts=pulumi.ResourceOptions(parent=self),
            )
            self.url = Output.from_input(f'http://{host}')
        else:
            self.url = self.endpoint

        self.register_outputs({'endpoint': self.endpoint, 'url': self.url})
