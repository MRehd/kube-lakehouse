'''Trino Pulumi component package.'''

from .trino import Trino, TrinoArgs, TrinoAutoscalingArgs, TrinoIcebergCatalogArgs

__all__ = ['Trino', 'TrinoArgs', 'TrinoAutoscalingArgs', 'TrinoIcebergCatalogArgs']
