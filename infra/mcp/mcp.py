'''
FastMCP Trino server — builds a Docker image and deploys it to Kubernetes.

Builds the MCP image from infra/mcp/app/, pushes it to a registry, and deploys
a Deployment + Service (+ optional Ingress). The server exposes a `query` tool
over the MCP HTTP transport, letting AI agents run SQL against Trino.

Example:
    mcp = Mcp('mcp', McpArgs(
        namespace=ns.metadata.name,
        image_name='docker.io/myuser/mcp',
        registry_username=config.require('docker_registry_username'),
        registry_password=config.require_secret('docker_registry_password'),
        env={
            'TRINO_HOST':    trino.host,
            'TRINO_PORT':    '8080',
            'TRINO_CATALOG': 'bronze',
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
CONFIG_DIR    = Path(__file__).parent.parent / 'config'


@dataclass
class McpArgs:
    '''Configuration arguments for the MCP server deployment.'''

    namespace: Input[str]
    '''Kubernetes namespace to deploy into (must already exist).'''

    image_name: Input[str]
    '''Full image name without tag, e.g. "docker.io/myuser/mcp".'''

    registry_username: Input[str]
    '''Docker registry username. Leave empty to skip push (local dev).'''

    registry_password: Input[str]
    '''Docker registry password or access token. Accepts a Pulumi secret Output.'''

    registry_server: Input[str] = 'https://index.docker.io/v1/'
    '''Docker registry server URL.'''

    image_tag: Input[str] = 'latest'
    '''Image tag to build and push.'''

    port: int = 8000
    '''Container port exposed by the MCP HTTP transport.'''

    replicas: int = 1
    '''Number of pod replicas.'''

    cpu_request: Input[str] = '100m'
    '''CPU request for the pod.'''

    cpu_limit: Input[str] = '500m'
    '''CPU limit for the pod.'''

    memory_request: Input[str] = '128Mi'
    '''Memory request for the pod.'''

    memory_limit: Input[str] = '256Mi'
    '''Memory limit for the pod.'''

    env: Dict[str, Input[str]] = field(default_factory=dict)
    '''
    Environment variables injected into the pod. Pass Trino connection details
    here: TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_PASSWORD, TRINO_HTTP_SCHEME,
    TRINO_CATALOG, TRINO_SCHEMA. Values may be Pulumi Outputs.
    '''

    ingress_enabled: bool = False
    '''Create an Ingress for external agent access.'''

    ingress_domain: Input[str] = ''
    '''Base domain. Creates mcp.<domain>.'''

    ingress_class_name: Input[str] = 'nginx'
    '''Ingress class name.'''


class Mcp(pulumi.ComponentResource):
    '''
    Builds the MCP Docker image, pushes it to a registry, and deploys it to
    Kubernetes as a Deployment + Service (+ optional Ingress).

    The server reads Trino connection info from env vars and exposes a `query`
    tool over the MCP HTTP transport on the configured port.
    '''

    def __init__(
        self,
        name: str,
        args: McpArgs,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:mcp:Mcp', name, {}, opts)

        self._namespace   = Output.from_input(args.namespace)
        registry_username = Output.from_input(args.registry_username)
        registry_password = Output.from_input(args.registry_password)
        image_name        = Output.from_input(args.image_name)
        image_tag         = Output.from_input(args.image_tag)
        ingress_domain    = Output.from_input(args.ingress_domain)
        env_values        = {k: Output.from_input(v) for k, v in args.env.items()}
        full_image        = Output.concat(image_name, ':', image_tag)

        image = docker.Image(
            f'{name}-image',
            image_name=full_image,
            build=docker.DockerBuildArgs(
                context=BUILD_CONTEXT,
                dockerfile=f'{BUILD_CONTEXT}/dockerfile',
            ),
            registry=docker.RegistryArgs(
                server=Output.from_input(args.registry_server),
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
                            'name': 'mcp',
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
            host = Output.concat('mcp.', ingress_domain)

            ingress_spec = json.loads((CONFIG_DIR / 'resources/ingress_spec.json').read_text())
            ingress_spec['ingressClassName']                                                     = args.ingress_class_name
            ingress_spec['rules'][0]['host']                                                     = host
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['name']           = name
            ingress_spec['rules'][0]['http']['paths'][0]['backend']['service']['port']['number'] = args.port

            Ingress(
                f'{name}-ingress',
                metadata={
                    'namespace':   self._namespace,
                    'name':        f'{name}-ingress',
                    'annotations': {'kubernetes.io/ingress.class': args.ingress_class_name},
                },
                spec=ingress_spec,
                opts=pulumi.ResourceOptions(parent=self),
            )
            self.url = Output.concat('http://', host)
        else:
            self.url = self.endpoint

        self.register_outputs({'endpoint': self.endpoint, 'url': self.url})
