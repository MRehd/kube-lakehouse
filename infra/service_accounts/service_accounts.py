'''Reusable Kubernetes ServiceAccount provisioner component.'''

from dataclasses import dataclass, field
from typing import List, Optional

import pulumi
from pulumi import Input, Output
from pulumi_kubernetes.core.v1 import ServiceAccount
from pulumi_kubernetes.rbac.v1 import Role, RoleBinding


@dataclass
class PolicyRuleArgs:
    '''A single RBAC policy rule.'''

    resources: List[str]
    '''Kubernetes resource types (e.g. ["secrets", "configmaps"]).'''

    verbs: List[str]
    '''Allowed actions (e.g. ["get", "create", "list"]).'''

    api_groups: List[str] = field(default_factory=lambda: [''])
    '''API groups (default [""] for core resources).'''


@dataclass
class ServiceAccountArgs:
    '''Configuration for a single Kubernetes ServiceAccount with optional RBAC.'''

    name: str
    '''Name of the ServiceAccount to create.'''

    rules: List[PolicyRuleArgs] = field(default_factory=list)
    '''RBAC policy rules. When non-empty, a Role and RoleBinding are created.'''


@dataclass
class ServiceAccountsArgs:
    '''Configuration arguments for the ServiceAccounts component.'''

    namespace: Input[str] = 'default'
    '''Kubernetes namespace to create resources in (must already exist).'''


class ServiceAccounts(pulumi.ComponentResource):
    '''
    A reusable Pulumi component for provisioning Kubernetes ServiceAccounts with RBAC.

    Creates ServiceAccount resources and, when rules are provided, a matching
    Role and RoleBinding scoped to the component's namespace.

    Example:
        ```python
        from service_accounts import ServiceAccounts, ServiceAccountsArgs, ServiceAccountArgs, PolicyRuleArgs

        sas = ServiceAccounts('sas', ServiceAccountsArgs(namespace=ns.metadata.name))

        sa = sas.provision('polaris-provisioner', ServiceAccountArgs(
            name='polaris-principal-provisioner',
            rules=[
                PolicyRuleArgs(resources=['secrets'], verbs=['get', 'create']),
            ],
        ))
        ```
    '''

    def __init__(
        self,
        name: str,
        args: ServiceAccountsArgs = None,
        opts: pulumi.ResourceOptions = None,
    ):
        super().__init__('k8lh:service_accounts:ServiceAccounts', name, {}, opts)

        args = args or ServiceAccountsArgs()
        self._namespace = Output.from_input(args.namespace)
        self.namespace = self._namespace

        self.register_outputs({'namespace': self.namespace})

    def provision(
        self,
        name: str,
        args: ServiceAccountArgs,
        opts: pulumi.ResourceOptions = None,
    ) -> ServiceAccount:
        '''
        Create a ServiceAccount and, if rules are provided, a Role and RoleBinding.

        Args:
            name: Pulumi resource name prefix for created resources.
            args: ServiceAccount configuration including name and RBAC rules.
            opts: Optional Pulumi resource options.

        Returns:
            The created Kubernetes ServiceAccount resource.
        '''
        base_opts = pulumi.ResourceOptions(parent=self)
        if opts:
            base_opts = pulumi.ResourceOptions.merge(base_opts, opts)

        sa = ServiceAccount(
            f'{name}-sa',
            metadata={'name': args.name, 'namespace': self._namespace},
            opts=base_opts,
        )

        if args.rules:
            role = Role(
                f'{name}-role',
                metadata={'namespace': self._namespace},
                rules=[
                    {
                        'apiGroups': rule.api_groups,
                        'resources': rule.resources,
                        'verbs': rule.verbs,
                    }
                    for rule in args.rules
                ],
                opts=base_opts,
            )
            RoleBinding(
                f'{name}-rolebinding',
                metadata={'namespace': self._namespace},
                role_ref={
                    'apiGroup': 'rbac.authorization.k8s.io',
                    'kind': 'Role',
                    'name': role.metadata.name,
                },
                subjects=[{
                    'kind': 'ServiceAccount',
                    'name': args.name,
                    'namespace': self._namespace,
                }],
                opts=pulumi.ResourceOptions(
                    parent=self,
                    depends_on=[sa, role],
                ),
            )

        return sa
