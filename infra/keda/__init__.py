'''KEDA (Kubernetes Event-Driven Autoscaling) Pulumi component package.'''

from .keda import Keda, KedaArgs, KafkaTriggerArgs, ScaledObjectArgs

__all__ = ['Keda', 'KedaArgs', 'KafkaTriggerArgs', 'ScaledObjectArgs']
