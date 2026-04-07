'''FastAPI Kafka producer — builds image via Docker and deploys to Kubernetes.'''

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
    namespace: Input[str]
    image_name: str
    '''Full image name without tag, e.g. "docker.io/myuser/producer".'''

    registry_username: Input[str]
    registry_password: Input[str]
    registry_server: str = 'https://index.docker.io/v1/'

    image_tag: str = 'latest'
    port: int = 8000
    replicas: int = 1
    cpu_request: str = '100m'
    cpu_limit: str = '500m'
    memory_request: str = '128Mi'
    memory_limit: str = '256Mi'
    env: Dict[str, str] = field(default_factory=dict)

    ingress_enabled: bool = False
    ingress_domain: str = ''
    ingress_class_name: str = 'nginx'


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

        self._namespace = Output.from_input(args.namespace)
        full_image = f'{args.image_name}:{args.image_tag}'

        # Build and push image (registry auth skipped if username is empty)
        registry = Output.all(args.registry_username, args.registry_password).apply(
            lambda vals: docker.RegistryArgs(
                server=args.registry_server,
                username=vals[0],
                password=vals[1],
            ) if vals[0] else None
        )
        image = docker.Image(
            f'{name}-image',
            image_name=full_image,
            build=docker.DockerBuildArgs(
                context=BUILD_CONTEXT,
                dockerfile=f'{BUILD_CONTEXT}/dockerfile',
            ),
            registry=registry,
            opts=pulumi.ResourceOptions(parent=self),
        )

        env_vars = [{'name': k, 'value': v} for k, v in args.env.items()]

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
