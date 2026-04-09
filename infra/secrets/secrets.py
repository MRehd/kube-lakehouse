'''Kubernetes secrets management for the data lakehouse.'''

from dataclasses import dataclass
from typing import Dict, List

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.core.v1 import Secret
from pulumi_kubernetes.meta.v1 import ObjectMetaArgs


@dataclass
class SecretArgs:
    '''Configuration for a Kubernetes secret.'''

    name: str
    '''Name of the Kubernetes secret.'''

    data: Dict[str, Input[str]]
    '''Key-value pairs for the secret data. Values can be plain strings or Pulumi Outputs.'''


class LakehouseSecrets(pulumi.ComponentResource):
    '''
    Manages Kubernetes secrets for the data lakehouse.

    Creates secrets upfront so Helm charts can reference them via existingSecret,
    avoiding issues with Pulumi Output serialization in Helm values.

    Example:
        ```python
        secrets = LakehouseSecrets('lakehouse-secrets', 'my-namespace', [
            SecretArgs(
                name='minio-secret',
                data={
                    'rootUser': 'admin',
                    'rootPassword': config.require_secret('minio_password'),
                },
            ),
            SecretArgs(
                name='postgres-secret',
                data={
                    'postgres-password': config.require_secret('postgres_password'),
                },
            ),
        ])
        ```
    '''

    secrets: Dict[str, Secret]
    '''Dictionary of created secrets by name.'''

    def __init__(
        self,
        name: str,
        namespace: Input[str],
        secrets: List[SecretArgs],
        opts: pulumi.ResourceOptions = None,
    ):
        '''
        Create secrets for the data lakehouse.

        Args:
            name: Pulumi resource name.
            namespace: Kubernetes namespace for secrets.
            secrets: List of secret configurations.
            opts: Pulumi resource options.
        '''
        super().__init__('lakehouse:secrets:LakehouseSecrets', name, None, opts)

        _namespace  = Output.from_input(namespace)
        child_opts  = pulumi.ResourceOptions(parent=self)

        self.secrets = {}
        for secret_args in secrets:
            secret = Secret(
                f'{name}-{secret_args.name}',
                metadata=ObjectMetaArgs(
                    name=secret_args.name,
                    namespace=_namespace,
                ),
                string_data=secret_args.data,
                opts=child_opts,
            )
            self.secrets[secret_args.name] = secret

        # Register outputs
        self.register_outputs({
            'secrets': {k: v.metadata.name for k, v in self.secrets.items()},
        })
