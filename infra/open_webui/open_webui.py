'''
Open WebUI on Kubernetes — open-webui Helm chart.

Deploys Open WebUI, a ChatGPT-style interface that connects to an external
Ollama instance. The bundled Ollama subchart is disabled — pass the Ollama
endpoint Output directly via ollama_endpoint.

Example:
    open_webui = OpenWebUI('open-webui', OpenWebUIArgs(
        namespace=ns.metadata.name,
        ollama_endpoint=ollama.endpoint,
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


CONFIG_DIR = Path(__file__).parent.parent / 'config'


@dataclass
class OpenWebUIArgs:
    '''Configuration arguments for Open WebUI.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name. Defaults to the Pulumi resource name.'''

    chart_version: str = '13.3.1'
    '''Version of the open-webui Helm chart.'''

    ollama_endpoint: Input[str] = 'http://localhost:11434'
    '''Ollama API base URL. Accepts a Pulumi Output (e.g. ollama.endpoint).'''

    ingress_enabled: bool = False
    '''Create an Ingress for the Open WebUI.'''

    ingress_domain: Optional[str] = None
    '''Base domain. Creates chat.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values merged over the base config.'''


class OpenWebUI(pulumi.ComponentResource):
    '''
    Deploys Open WebUI using the official Helm chart.

    Connects to an external Ollama instance — the bundled Ollama subchart
    is disabled. Pass ollama.endpoint as ollama_endpoint.

    Outputs:
        namespace — Kubernetes namespace
        ui_url    — http://chat.<domain> if ingress enabled, else internal svc URL
    '''

    def __init__(
        self,
        name: str,
        args: OpenWebUIArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:open_webui:OpenWebUI', name, {}, opts)

        args = args or OpenWebUIArgs()
        self._namespace  = Output.from_input(args.namespace)
        release          = args.release_name or name
        ollama_endpoint  = Output.from_input(args.ollama_endpoint)

        v = json.loads((CONFIG_DIR / 'helm/helm_values_open_webui.json').read_text())
        v['fullnameOverride']    = release
        v['ollamaUrls']          = [ollama_endpoint]
        v['ingress']['enabled']  = args.ingress_enabled
        v['ingress']['class']    = args.ingress_class_name
        if args.ingress_enabled and args.ingress_domain:
            v['ingress']['host'] = f'chat.{args.ingress_domain}'

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='open-webui',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://helm.openwebui.com/'),
                values=v,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace
        self.ui_url = (
            f'http://chat.{args.ingress_domain}'
            if args.ingress_enabled and args.ingress_domain
            else Output.concat('http://', release, '.', self._namespace, '.svc.cluster.local:80')
        )

        self.register_outputs({
            'namespace': self.namespace,
            'ui_url':    self.ui_url,
        })
