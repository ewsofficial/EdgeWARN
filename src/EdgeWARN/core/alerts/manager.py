"""
Alert Manager

Provides centralised publishing of AlertPayloads to the filesystem.
"""

import json
from typing import List

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
