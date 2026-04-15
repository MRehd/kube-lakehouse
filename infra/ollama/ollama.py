'''
Ollama LLM server on Kubernetes — otwld/ollama Helm chart.

Deploys the Ollama inference server with optional persistent storage.
Models are pulled on demand via pull_model(), which creates a Kubernetes
Job that calls `ollama pull <model>` against the running service.

Example:
    ollama = Ollama('ollama', OllamaArgs(
        namespace=ns.metadata.name,
        ingress_enabled=True,
        ingress_domain='k8lh.local',
        gpu_enabled=False,
        storage_size='30Gi',
    ))

    # Pull models after the server is up
    ollama.pull_model('llama3.2')
    ollama.pull_model('nomic-embed-text')
'''

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pulumi
import pulumi_kubernetes as k8s
from pulumi import Input, Output
from pulumi_kubernetes.helm.v3 import Chart, ChartOpts, FetchOpts


CONFIG_DIR = Path(__file__).parent.parent / 'config'
OLLAMA_IMAGE = 'ollama/ollama:latest'


@dataclass
class OllamaArgs:
    '''Configuration arguments for the Ollama LLM server.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to deploy into (must already exist).'''

    release_name: Optional[str] = None
    '''Helm release name. Defaults to the Pulumi resource name.'''

    chart_version: str = '0.4.0'
    '''Version of the otwld/ollama Helm chart.'''

    gpu_enabled: bool = False
    '''Enable GPU support (requires NVIDIA device plugin in cluster).'''

    gpu_count: int = 1
    '''Number of GPUs to request per pod.'''

    cpu_request: str = '1000m'
    '''CPU request.'''

    memory_request: str = '2Gi'
    '''Memory request. Ollama loads models entirely into RAM when no GPU.'''

    storage_enabled: bool = True
    '''Enable a PersistentVolume to cache downloaded models across restarts.'''

    storage_size: str = '30Gi'
    '''PVC size for model storage.'''

    ingress_enabled: bool = False
    '''Create an Ingress for the Ollama API.'''

    ingress_domain: Optional[str] = None
    '''Base domain. Creates ollama.<domain>.'''

    ingress_class_name: str = 'nginx'
    '''Ingress class name.'''

    extra_values: dict = field(default_factory=dict)
    '''Additional Helm values merged over the base config.'''


class Ollama(pulumi.ComponentResource):
    '''
    Deploys the Ollama LLM inference server using the otwld Helm chart.

    Use pull_model() to download a model into the running server — each call
    creates a short-lived Kubernetes Job that runs `ollama pull <model>`.

    Outputs:
        namespace  — Kubernetes namespace
        endpoint   — Internal cluster URL (http://<release>.<ns>.svc.cluster.local:11434)
        ui_url     — External URL if ingress enabled, else same as endpoint
    '''

    def __init__(
        self,
        name: str,
        args: OllamaArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:ollama:Ollama', name, {}, opts)

        args = args or OllamaArgs()
        self._name      = name
        self._namespace = Output.from_input(args.namespace)
        release         = args.release_name or name

        v = json.loads((CONFIG_DIR / 'helm/helm_values_ollama.json').read_text())
        v['fullnameOverride']                   = release
        v['ollama']['gpu']['enable']            = args.gpu_enabled
        v['ollama']['gpu']['nbrGpu']            = args.gpu_count
        v['resources']['requests']['cpu']       = args.cpu_request
        v['resources']['requests']['memory']    = args.memory_request
        v['persistentVolume']['enabled']        = args.storage_enabled
        v['persistentVolume']['size']           = args.storage_size
        v['ingress']['enabled']                 = args.ingress_enabled
        v['ingress']['className']               = args.ingress_class_name
        if args.ingress_enabled and args.ingress_domain:
            v['ingress']['hosts'] = [{'host': f'ollama.{args.ingress_domain}', 'paths': [{'path': '/', 'pathType': 'Prefix'}]}]

        self.chart = Chart(
            f'{name}-chart',
            ChartOpts(
                chart='ollama',
                version=args.chart_version,
                namespace=self._namespace,
                fetch_opts=FetchOpts(repo='https://helm.otwld.com/'),
                values=v,
            ),
            opts=pulumi.ResourceOptions(parent=self),
        )

        self.namespace = self._namespace
        self.endpoint  = Output.concat(
            'http://', release, '.', self._namespace, '.svc.cluster.local:11434'
        )
        self.ui_url = (
            f'http://ollama.{args.ingress_domain}'
            if args.ingress_enabled and args.ingress_domain
            else self.endpoint
        )

        self.register_outputs({
            'namespace': self.namespace,
            'endpoint':  self.endpoint,
            'ui_url':    self.ui_url,
        })

    def pull_model(
        self,
        model: str,
        opts: pulumi.ResourceOptions = None,
    ) -> k8s.batch.v1.Job:
        '''
        Pull a model into the Ollama server.

        Creates a Kubernetes Job that runs `ollama pull <model>` against the
        running service. The Job is cleaned up automatically 120 s after it
        completes. Model names follow Ollama's library format, e.g.:
            "llama3.2", "mistral", "nomic-embed-text", "llama3.2:3b"

        Args:
            model: Ollama model name (optionally with tag, e.g. "llama3.2:3b").
            opts:  Pulumi resource options (parent, depends_on, etc.).

        Returns:
            The created Job resource.
        '''
        safe_name = model.replace(':', '-').replace('/', '-')

        return k8s.batch.v1.Job(
            f'{self._name}-pull-{safe_name}',
            metadata=k8s.meta.v1.ObjectMetaArgs(
                namespace=self._namespace,
            ),
            spec=k8s.batch.v1.JobSpecArgs(
                ttl_seconds_after_finished=120,
                template=k8s.core.v1.PodTemplateSpecArgs(
                    spec=k8s.core.v1.PodSpecArgs(
                        restart_policy='OnFailure',
                        containers=[k8s.core.v1.ContainerArgs(
                            name='ollama-pull',
                            image=OLLAMA_IMAGE,
                            command=['ollama', 'pull', model],
                            env=[k8s.core.v1.EnvVarArgs(
                                name='OLLAMA_HOST',
                                value=self.endpoint,
                            )],
                        )],
                    ),
                ),
            ),
            opts=pulumi.ResourceOptions(
                parent=self,
                depends_on=[self.chart],
                **(opts.__dict__ if opts else {}),
            ),
        )
