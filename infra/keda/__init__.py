'''KEDA (Kubernetes Event-Driven Autoscaling) Pulumi component package.'''

from .keda import Keda, KedaArgs, KafkaTrigger, ScaledObjectArgs, TriggerArgs

__all__ = ['Keda', 'KedaArgs', 'KafkaTrigger', 'ScaledObjectArgs', 'TriggerArgs']
