"""
Alert Manager

Provides centralised publishing and retrieval of AlertPayloads
to/from the filesystem.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

import util.file as fs
from util.io import IOManager

from .schema import AlertPayload

io_manager = IOManager("[Alerts]")


class AlertManager:
    """
    Centralised alert publisher.

    Writes alert JSON files to ``fs.ALERTS_DIR`` using the naming convention
    ``alert_{source}_{cell_id}.json`` so that multiple modules can alert on
    the same cell without file-name collisions.
    """

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    @staticmethod
    def publish(alert: AlertPayload) -> bool:
        """
        Persist an alert to disk.

        Args:
            alert: A fully-populated ``AlertPayload``.

        Returns:
            ``True`` on success, ``False`` otherwise.
        """
        if not alert.cell_id or not alert.geometry:
            return False

        try:
            fs.ALERTS_DIR.mkdir(parents=True, exist_ok=True)

            filename = f"alert_{alert.source}_{alert.cell_id}.json"
            alert_file = fs.ALERTS_DIR / filename

            with open(alert_file, "w") as f:
                json.dump(alert.to_dict(), f, indent=4)

            return True

        except Exception as e:
            io_manager.write_error(
                f"Failed to publish alert for cell {alert.cell_id} "
                f"from {alert.source}: {e}"
            )
            return False

    @staticmethod
    def publish_many(alerts: List[AlertPayload]) -> int:
        """
        Publish a batch of alerts.

        Returns:
            The number of alerts successfully written.
        """
        return sum(1 for a in alerts if AlertManager.publish(a))

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    @staticmethod
    def _dict_to_payload(data: Dict[str, Any]) -> AlertPayload:
        """Reconstruct an AlertPayload from a JSON-parsed dictionary."""
        def _parse_dt(val: str) -> datetime:
            try:
                return datetime.fromisoformat(val)
            except (ValueError, TypeError):
                return datetime.now()

        return AlertPayload(
            alert_type=data.get("alert_type", "unknown"),
            source=data.get("source", "unknown"),
            cell_id=data.get("id", ""),
            geometry=data.get("geometry", []),
            effective_time=_parse_dt(data.get("effective", "")),
            expiry_time=_parse_dt(data.get("expires", "")),
            severity=data.get("severity", "warning"),
            threats=data.get("threats", {}),
        )

    @staticmethod
    def load(source: str, cell_id: str) -> Optional[AlertPayload]:
        """
        Load a single alert from disk by source and cell_id.

        Args:
            source:  The module name that published the alert.
            cell_id: The storm cell identifier.

        Returns:
            The ``AlertPayload`` if the file exists, otherwise ``None``.
        """
        alert_file = fs.ALERTS_DIR / f"alert_{source}_{cell_id}.json"
        if not alert_file.exists():
            return None

        try:
            with open(alert_file, "r") as f:
                data = json.load(f)
            return AlertManager._dict_to_payload(data)
        except Exception as e:
            io_manager.write_error(
                f"Failed to load alert {alert_file.name}: {e}"
            )
            return None

    @staticmethod
    def load_all(cell_id: str) -> List[AlertPayload]:
        """
        Load every alert for a given cell, regardless of source.

        Args:
            cell_id: The storm cell identifier.

        Returns:
            A list of ``AlertPayload`` objects (may be empty).
        """
        if not fs.ALERTS_DIR.exists():
            return []

        results: List[AlertPayload] = []
        for path in fs.ALERTS_DIR.glob(f"alert_*_{cell_id}.json"):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                results.append(AlertManager._dict_to_payload(data))
            except Exception as e:
                io_manager.write_error(
                    f"Failed to load alert {path.name}: {e}"
                )
        return results
