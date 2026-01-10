"""
CTAM Modules Package

Automatically registers all available analysis modules on import.
"""

from ..registry import ModuleRegistry
from .StormCast import StormCastModule

# Register all modules
ModuleRegistry.register(StormCastModule())

__all__ = [
    "StormCastModule",
]
