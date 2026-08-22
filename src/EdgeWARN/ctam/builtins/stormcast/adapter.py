"""StormCast's host-owned cycle boundary.

The forecasting implementation remains import-compatible at
``EdgeWARN.ctam.modules.StormCast``. New CTAM execution enters through this
adapter so history and alert state are supplied by the host.
"""
from __future__ import annotations

from typing import Any, Optional

from EdgeWARN.alerts import AlertManager
from EdgeWARN.alerts.schema import AlertPayload
from EdgeWARN.ctam.modules.StormCast import StormCastModule


class StormCastCycleService:
    """The narrow host service surface StormCast needs during one cycle."""

    def __init__(self, history_cache: Any | None = None) -> None:
        self._history_cache = history_cache

    def history(self, cell_id: Any) -> list[dict[str, Any]]:
        if self._history_cache is None:
            return []
        return list(reversed(self._history_cache.get(cell_id)))

    @staticmethod
    def previous_alert(cell_id: Any) -> Optional[AlertPayload]:
        return AlertManager.load("StormCast", cell_id)

    @staticmethod
    def publish(alerts: list[AlertPayload]) -> int:
        return AlertManager.publish_many(alerts)


class BuiltinStormCastAdapter:
    """Execute the reserved built-in through :class:`StormCastCycleService`."""

    module_id = "stormcast"
    name = "StormCast"

    def __init__(self, service: StormCastCycleService) -> None:
        self._service = service
        self._module = StormCastModule()

    def run(self, cell: dict[str, Any]) -> None:
        self._module.run(cell, history_provider=self._service.history)

    def alerts(self, cell: dict[str, Any]) -> list[AlertPayload]:
        return self._module.alerts(
            cell,
            previous_alert=self._service.previous_alert(cell.get("id", "unknown_cell")),
        ) or []

    def publish_alerts(self, alerts: list[AlertPayload]) -> int:
        return self._service.publish(alerts)
