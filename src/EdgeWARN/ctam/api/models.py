"""Small transport-neutral models for the read-only CTAM API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class APIError(Exception):
    """A safe error which the HTTP transport can serialize."""

    code: str
    message: str
    status: int = 400
    resource: str | None = None
    limit: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "resource": self.resource,
            "pointer": None,
            "expected_revision": None,
            "observed_revision": None,
            "limit": self.limit,
            "retryable": False,
        }
