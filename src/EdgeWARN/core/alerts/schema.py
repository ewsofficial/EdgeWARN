"""
Alert Schema

Defines the standardised AlertPayload dataclass used by all CTAM modules
that wish to emit alerts.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class AlertPayload:
    """
    Unified alert payload.

    Attributes:
        alert_type:     Category of the alert (e.g. "severe_weather", "flash_flood").
        source:         Name of the CTAM module that produced this alert.
        cell_id:        Storm cell identifier.
        geometry:       Polygon as a list of (lat, lon) coordinate pairs.
        effective_time: When the alert becomes active (ISO 8601 datetime).
        expiry_time:    When the alert expires (ISO 8601 datetime).
        severity:       Free-form severity label (e.g. "warning", "watch", "advisory").
        threats:        Module-specific threat metadata.
    """

    alert_type: str
    source: str
    cell_id: str
    geometry: List[Tuple[float, float]]
    effective_time: datetime
    expiry_time: datetime
    severity: str = "warning"
    threats: Dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        """Formatted alert ID: id:{alert_type}:{source}:{cell_id}:{YYYY}.{MM}.{DD}.{HH}.{MM}.{SS}"""
        formatted_time = self.effective_time.strftime("%Y.%m.%d.%H.%M.%S")
        return f"id:{self.alert_type}:{self.source}:{self.cell_id}:{formatted_time}"

    # ------------------------------------------------------------------
    # Serialisation helper
    # ------------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable dictionary."""
        # Round geometry coordinates to 4 decimal places to reduce file size
        rounded_geometry = [
            (round(float(lat), 4), round(float(lon), 4)) 
            for lat, lon in self.geometry
        ]
        
        return {
            "alert_type": self.alert_type,
            "source": self.source,
            "id": self.id,
            "cell_id": self.cell_id,
            "geometry": rounded_geometry,
            "effective": self.effective_time.isoformat(),
            "expires": self.expiry_time.isoformat(),
            "severity": self.severity,
            "threats": self.threats,
        }
