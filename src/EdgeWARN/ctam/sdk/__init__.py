"""Dependency-light public Python client for CTAM internal API v1."""

from .client import CTAMClient, CTAMAPIError

__all__ = ["CTAMClient", "CTAMAPIError"]
