"""
EdgeWARN Alerts Package

Unified alert system for all CTAM modules.
"""

from .schema import AlertPayload
from .manager import AlertManager

__all__ = ["AlertPayload", "AlertManager"]
