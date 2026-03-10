"""
NWS Alert Registry Module

Manages unique NWS alerts with deduplication and expiration tracking.
Stores alerts by unique ID and removes alerts not seen within a configurable TTL.

Architecture:
    - Single registry file (alerts_registry.json) stores all active alerts
    - Each alert tracked with first_seen, last_seen, and expires timestamps
    - TTL-based cleanup removes stale alerts (default 2 hours)
"""

import json
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional, Any
from decimal import Decimal
from util.io import IOManager

io_manager = IOManager("[AlertRegistry]")


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal types from NWS API."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class AlertRegistry:
    """
    Manages unique NWS alerts with deduplication and expiration tracking.
    
    Features:
    - Stores alerts by unique ID (extracted from feature['id'])
    - Tracks first_seen and last_seen timestamps
    - Removes alerts not seen within configurable TTL (default 2 hours)
    
    Note: This class is not thread-safe. For async usage, ensure only one
    coroutine accesses the registry at a time, or use external synchronization.
    
    Registry Structure:
    {
        "last_updated": "2026-02-23T03:40:00Z",
        "alerts": {
            "<alert_id>": {
                "id": "https://api.weather.gov/alerts/urn:oid:...",
                "first_seen": "2026-02-23T02:00:00Z",
                "last_seen": "2026-02-23T03:40:00Z",
                "expires": "2026-02-23T04:00:00Z",
                "feature": { ... }  # Full GeoJSON feature with Polygon
            }
        }
    }
    
    Usage:
        registry = AlertRegistry(fs.MRMS_NWS_DIR)
        
        # Process incoming alerts
        new_count, updated_count = registry.process_alerts(features, datetime.now(timezone.utc))
        
        # Cleanup expired alerts
        removed_count = registry.cleanup_expired(datetime.now(timezone.utc))
        
        # Save to disk
        registry.save()
        
        # Get active alerts
        alerts = registry.get_active_alerts()
        ids = registry.get_active_ids()
    """
    
    def __init__(self, registry_dir: Path, ttl_hours: float = 2.0):
        """
        Initialize the AlertRegistry.
        
        Args:
            registry_dir: Path to the base official alerts directory
            ttl_hours: Time-to-live in hours for alerts not seen (default 2.0)
        """
        self.registry_dir = Path(registry_dir)
        self.ids_dir = self.registry_dir / "ids"
        self.ts_dir = self.registry_dir / "timestamps"
        self.ttl_hours = ttl_hours
        
        self.ids_dir.mkdir(parents=True, exist_ok=True)
        self.ts_dir.mkdir(parents=True, exist_ok=True)
        
        self._registry = self._load_registry()
    
    def _load_registry(self) -> Dict[str, Any]:
        """
        Load existing registry from individual ID and timestamp files.
        """
        data = {
            "last_updated": None,
            "alerts": {}
        }
        
        try:
            # Load all alert files from ids directory
            for file_path in self.ids_dir.glob("*.json"):
                if file_path.is_file() and not file_path.name.startswith(".tmp"):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            alert_data = json.load(f)
                            if "id" in alert_data:
                                alert_id = self._extract_alert_id(alert_data.get("feature", {}))
                                if alert_id:
                                    data["alerts"][alert_id] = alert_data
                                else:
                                    data["alerts"][file_path.stem] = alert_data
                    except Exception as load_err:
                        pass
            
            # Find the most recent timestamp to get last_updated
            ts_files = sorted([f for f in self.ts_dir.glob("*.json") if not f.name.startswith(".tmp")])
            if ts_files:
                try:
                    with open(ts_files[-1], 'r', encoding='utf-8') as f:
                        ts_data = json.load(f)
                        data["last_updated"] = ts_data.get("timestamp")
                except Exception:
                    pass
                    
            return data
        except Exception as e:
            io_manager.write_warning(f"Failed to load registry: {e}")
            return data
    
    def save(self) -> None:
        """
        Persist registry to disk:
        1. Write individual alert JSONs to ids_dir
        2. Write a snapshot of active IDs to ts_dir
        """
        current_time_str = self._registry["last_updated"]
        if not current_time_str:
            return
            
        # Write timestamp snapshot
        try:
            dt = datetime.fromisoformat(current_time_str.replace('Z', '+00:00'))
            timestamp_filename = dt.strftime("%Y%m%d-%H%M%S") + ".json"
        except Exception:
            timestamp_filename = current_time_str.replace(":", "").replace("-", "").replace("T", "-").replace("Z", "") + ".json"
            
        snapshot_path = self.ts_dir / timestamp_filename
        
        snapshot_data = {
            "timestamp": current_time_str,
            "count": len(self._registry["alerts"]),
            "alerts": list(self._registry["alerts"].keys())
        }
        
        fd, temp_path = tempfile.mkstemp(dir=self.ts_dir, prefix=".tmp_ts_")
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(snapshot_data, f, indent=2)
            os.replace(temp_path, snapshot_path)
        except Exception as e:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            io_manager.write_error(f"Failed to save timestamp file: {e}")
            
        # Write individual ID files
        valid_filenames = set()
        for alert_id, alert_data in self._registry["alerts"].items():
            safe_id = alert_id.replace(":", "_").replace("/", "_") + ".json"
            valid_filenames.add(safe_id)
            alert_path = self.ids_dir / safe_id
            
            fd, temp_path = tempfile.mkstemp(dir=self.ids_dir, prefix=".tmp_id_")
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(alert_data, f, cls=DecimalEncoder, indent=2)
                os.replace(temp_path, alert_path)
            except Exception as e:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
        
        # Cleanup expired OR orphaned files
        for file_path in self.ids_dir.glob("*.json"):
            if not file_path.name.startswith(".tmp_") and file_path.name not in valid_filenames:
                try:
                    file_path.unlink()
                except OSError:
                    pass
    
    def _extract_alert_id(self, feature: Dict) -> Optional[str]:
        """
        Extract unique alert ID from a feature.
        
        NWS alerts have an 'id' field that is a URL like:
        https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0...
        
        We extract just the URN portion for a cleaner ID.
        
        Args:
            feature: GeoJSON feature from NWS API
            
        Returns:
            Alert ID string or None if not found
        """
        # Try feature['id'] first (standard location)
        alert_id = feature.get('id')
        
        if alert_id:
            # Extract URN from URL if present
            if isinstance(alert_id, str) and '/alerts/' in alert_id:
                return alert_id.split('/alerts/')[-1]
            return alert_id
        
        # Fallback to properties['id'] (some variations)
        props = feature.get('properties', {})
        alert_id = props.get('id')
        
        if alert_id:
            if isinstance(alert_id, str) and '/alerts/' in alert_id:
                return alert_id.split('/alerts/')[-1]
            return alert_id
        
        return None
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """
        Parse ISO format datetime string.
        
        Args:
            dt_str: ISO format datetime string
            
        Returns:
            datetime object or None
        """
        if not dt_str:
            return None
        
        try:
            # Handle various ISO formats
            if dt_str.endswith('Z'):
                dt_str = dt_str[:-1] + '+00:00'
            return datetime.fromisoformat(dt_str)
        except Exception:
            return None
    
    def process_alert(self, feature: Dict, current_time: datetime) -> Tuple[bool, Optional[str]]:
        """
        Process a single alert feature.
        
        Args:
            feature: GeoJSON feature from NWS API
            current_time: Current timestamp for last_seen tracking
            
        Returns:
            Tuple of (is_new: bool, alert_id: Optional[str])
        """
        alert_id = self._extract_alert_id(feature)
        
        if not alert_id:
            io_manager.write_warning("Alert missing ID, skipping")
            return False, None
        
        # Get expiration time from properties
        props = feature.get('properties', {})
        expires_str = props.get('expires')
        expires_dt = self._parse_datetime(expires_str)
        
        current_time_str = current_time.isoformat()
        
        if alert_id in self._registry["alerts"]:
            # Update existing alert
            self._registry["alerts"][alert_id]["last_seen"] = current_time_str
            if expires_dt:
                self._registry["alerts"][alert_id]["expires"] = expires_dt.isoformat()
            # Update feature data (may have changed)
            self._registry["alerts"][alert_id]["feature"] = feature
            # Update registry last_updated timestamp
            self._registry["last_updated"] = current_time_str
            return False, alert_id
        else:
            # Add new alert
            self._registry["alerts"][alert_id] = {
                "id": feature.get('id', alert_id),
                "first_seen": current_time_str,
                "last_seen": current_time_str,
                "expires": expires_dt.isoformat() if expires_dt else None,
                "feature": feature
            }
            # Update registry last_updated timestamp
            self._registry["last_updated"] = current_time_str
            return True, alert_id
    
    def process_alerts(self, features: List[Dict], current_time: datetime) -> Tuple[int, int]:
        """
        Process multiple alerts.
        
        Args:
            features: List of GeoJSON features from NWS API
            current_time: Current timestamp for last_seen tracking
            
        Returns:
            Tuple of (new_count: int, updated_count: int)
        """
        new_count = 0
        updated_count = 0
        
        for feature in features:
            is_new, alert_id = self.process_alert(feature, current_time)
            if alert_id:
                if is_new:
                    new_count += 1
                else:
                    updated_count += 1
        
        # Update last_updated timestamp
        self._registry["last_updated"] = current_time.isoformat()
        
        return new_count, updated_count
    
    def cleanup_expired(self, current_time: datetime) -> int:
        """
        Remove alerts not seen within TTL.
        
        Also removes alerts that have expired according to their 'expires' field.
        
        Args:
            current_time: Current timestamp for TTL calculation
            
        Returns:
            Count of removed alerts
        """
        ttl_delta = timedelta(hours=self.ttl_hours)
        cutoff_time = current_time - ttl_delta
        
        alerts_to_remove = []
        
        for alert_id, alert_data in self._registry["alerts"].items():
            # Check last_seen against TTL
            last_seen = self._parse_datetime(alert_data.get("last_seen"))
            if last_seen and last_seen < cutoff_time:
                alerts_to_remove.append(alert_id)
                continue
            
            # Also check if alert has expired
            expires = self._parse_datetime(alert_data.get("expires"))
            if expires and expires < current_time:
                alerts_to_remove.append(alert_id)
        
        # Remove expired alerts
        for alert_id in alerts_to_remove:
            del self._registry["alerts"][alert_id]
        
        if alerts_to_remove:
            io_manager.write_info(f"Cleaned up {len(alerts_to_remove)} expired alerts")
        
        return len(alerts_to_remove)
    
    def get_active_alerts(self) -> List[Dict]:
        """
        Return list of all active alert features.
        
        Returns:
            List of feature dictionaries
        """
        return [
            alert_data["feature"]
            for alert_data in self._registry["alerts"].values()
            if "feature" in alert_data
        ]
    
    def get_active_ids(self) -> List[str]:
        """
        Return list of active alert IDs only.
        
        Returns:
            List of alert ID strings
        """
        return list(self._registry["alerts"].keys())
    
    def get_alert_by_id(self, alert_id: str) -> Optional[Dict]:
        """
        Get a specific alert by ID.
        
        Args:
            alert_id: The alert ID to look up
            
        Returns:
            Alert feature dictionary or None if not found
        """
        alert_data = self._registry["alerts"].get(alert_id)
        if alert_data:
            return alert_data.get("feature")
        return None
    
    def get_registry_summary(self) -> Dict[str, Any]:
        """
        Get summary information about the registry.
        
        Returns:
            Dictionary with count, last_updated, and alert_ids
        """
        return {
            "count": len(self._registry["alerts"]),
            "last_updated": self._registry["last_updated"],
            "alert_ids": self.get_active_ids()
        }
    
    def get_full_registry(self) -> Dict[str, Any]:
        """
        Get the full registry data.
        
        Returns:
            Full registry dictionary
        """
        return self._registry
    
    @property
    def alert_count(self) -> int:
        """Return the number of active alerts."""
        return len(self._registry["alerts"])
    
    @property
    def last_updated(self) -> Optional[str]:
        """Return the last updated timestamp."""
        return self._registry["last_updated"]


# Module-level registry instance (lazy initialized)
_registry_instance: Optional[AlertRegistry] = None


def get_registry(registry_dir: Optional[Path] = None, ttl_hours: float = 2.0) -> AlertRegistry:
    """
    Get or create the singleton AlertRegistry instance.
    
    Args:
        registry_dir: Path to registry base directory (required on first call)
        ttl_hours: TTL in hours for expired alerts
        
    Returns:
        AlertRegistry instance
        
    Raises:
        ValueError: If registry_dir is not provided on first call
    """
    global _registry_instance
    
    if _registry_instance is None:
        if registry_dir is None:
            raise ValueError("registry_dir required for first initialization")
        _registry_instance = AlertRegistry(registry_dir, ttl_hours)
    elif registry_dir is not None:
        # Warn if called with different path than existing instance
        existing_path = _registry_instance.registry_dir
        if Path(registry_dir) != existing_path:
            io_manager.write_warning(
                f"get_registry called with different path ({registry_dir}); "
                f"using existing instance with path ({existing_path})"
            )
    
    return _registry_instance


def reset_registry():
    """Reset the singleton instance (useful for testing)."""
    global _registry_instance
    _registry_instance = None
