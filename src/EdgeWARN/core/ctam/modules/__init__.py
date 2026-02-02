"""
CTAM Modules Package

Automatically registers all available analysis modules on import.
"""

from ..registry import ModuleRegistry
from .StormCast import StormCastModule
from .morphowind import MorphoWindModule

# Register all modules
ModuleRegistry.register(StormCastModule())
ModuleRegistry.register(MorphoWindModule())

# GeoMapper is a file processor, not a per-entry module, so we don't register it
# It's accessed directly via: from EdgeWARN.core.ingest.nws.geomapper import process_warning

__all__ = [
    "StormCastModule",
    "MorphoWindModule",
]
