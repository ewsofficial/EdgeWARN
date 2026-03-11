import pytest
import json
from pathlib import Path
from datetime import datetime, timedelta

import util.file as fs
from EdgeWARN.core.alerts.schema import AlertPayload
from EdgeWARN.core.alerts.manager import AlertManager


@pytest.fixture
def override_alerts_dir(tmp_path):
    """Redirect EDGEWARN_ALERTS_DIR to a temporary directory for isolation."""
    original = getattr(fs, "EDGEWARN_ALERTS_DIR", None)
    fs.EDGEWARN_ALERTS_DIR = tmp_path / "EdgeWARN"
    yield fs.EDGEWARN_ALERTS_DIR
    if original is not None:
        fs.EDGEWARN_ALERTS_DIR = original


# ------------------------------------------------------------------
# AlertPayload tests
# ------------------------------------------------------------------

class TestAlertPayload:
    def test_to_dict_round_trip(self):
        effective = datetime(2026, 3, 4, 12, 0, 0)
        expiry = effective + timedelta(minutes=30)
        payload = AlertPayload(
            alert_type="severe_weather",
            source="StormCast",
            cell_id="cell_42",
            geometry=[(35.0, -97.0), (35.1, -97.0), (35.1, -97.1)],
            effective_time=effective,
            expiry_time=expiry,
            severity="warning",
            threats={"hail": True},
        )
        d = payload.to_dict()

        assert d["alert_type"] == "severe_weather"
        assert d["source"] == "StormCast"
        assert d["id"] == "id:severe_weather:2026.03.04.12.00.00"
        assert d["cell_id"] == "cell_42"
        assert d["severity"] == "warning"
        assert d["threats"] == {"hail": True}
        assert d["effective"] == effective.isoformat()
        assert d["expires"] == expiry.isoformat()
        assert d["geometry"] == [(35.0, -97.0), (35.1, -97.0), (35.1, -97.1)]


# ------------------------------------------------------------------
# AlertManager tests
# ------------------------------------------------------------------

class TestAlertManager:
    def test_publish_creates_file(self, override_alerts_dir):
        effective = datetime(2026, 3, 4, 12, 0, 0)
        alert = AlertPayload(
            alert_type="severe_weather",
            source="StormCast",
            cell_id="cell_99",
            geometry=[(35.0, -97.0), (35.1, -97.0)],
            effective_time=effective,
            expiry_time=effective + timedelta(minutes=30),
        )

        result = AlertManager.publish(alert)

        assert result is True
        alert_file = override_alerts_dir / "alert_StormCast_cell_99.json"
        assert alert_file.exists()

        with open(alert_file) as f:
            data = json.load(f)

        assert data["source"] == "StormCast"
        assert data["id"] == "id:severe_weather:2026.03.04.12.00.00"
        assert data["cell_id"] == "cell_99"
        assert data["alert_type"] == "severe_weather"
        assert data["geometry"] == [[35.0, -97.0], [35.1, -97.0]]

        # Check effective → expires = +30 min
        eff = datetime.fromisoformat(data["effective"])
        exp = datetime.fromisoformat(data["expires"])
        assert exp == eff + timedelta(minutes=30)

    def test_publish_rejects_empty_cell_id(self, override_alerts_dir):
        alert = AlertPayload(
            alert_type="flash_flood",
            source="FloodModule",
            cell_id="",
            geometry=[(35.0, -97.0)],
            effective_time=datetime.now(),
            expiry_time=datetime.now() + timedelta(minutes=60),
        )
        assert AlertManager.publish(alert) is False

    def test_publish_rejects_empty_geometry(self, override_alerts_dir):
        alert = AlertPayload(
            alert_type="flash_flood",
            source="FloodModule",
            cell_id="cell_1",
            geometry=[],
            effective_time=datetime.now(),
            expiry_time=datetime.now() + timedelta(minutes=60),
        )
        assert AlertManager.publish(alert) is False

    def test_publish_many(self, override_alerts_dir):
        now = datetime.now()
        alerts = [
            AlertPayload("severe_weather", "StormCast", f"cell_{i}",
                         [(35.0, -97.0)], now, now + timedelta(minutes=30))
            for i in range(3)
        ]
        count = AlertManager.publish_many(alerts)
        assert count == 3
        assert len(list(override_alerts_dir.glob("alert_*.json"))) == 3

    def test_no_collision_across_sources(self, override_alerts_dir):
        """Two modules alerting on the same cell_id must produce separate files."""
        now = datetime.now()
        a1 = AlertPayload("severe_weather", "StormCast", "cell_x",
                          [(35.0, -97.0)], now, now + timedelta(minutes=30))
        a2 = AlertPayload("flash_flood", "FloodModule", "cell_x",
                          [(36.0, -98.0)], now, now + timedelta(minutes=60))

        AlertManager.publish(a1)
        AlertManager.publish(a2)

        files = sorted(f.name for f in override_alerts_dir.glob("alert_*.json"))
        assert "alert_FloodModule_cell_x.json" in files
        assert "alert_StormCast_cell_x.json" in files

        # Contents are distinct
        with open(override_alerts_dir / "alert_StormCast_cell_x.json") as f:
            assert json.load(f)["source"] == "StormCast"
        with open(override_alerts_dir / "alert_FloodModule_cell_x.json") as f:
            assert json.load(f)["source"] == "FloodModule"

    # ------------------------------------------------------------------
    # Load tests
    # ------------------------------------------------------------------

    def test_load_existing_alert(self, override_alerts_dir):
        """load() should reconstruct an AlertPayload from disk."""
        effective = datetime(2026, 3, 4, 12, 0, 0)
        original = AlertPayload(
            "severe_weather", "StormCast", "cell_50",
            [(35.0, -97.0)], effective, effective + timedelta(minutes=30),
            threats={"hail": True},
        )
        AlertManager.publish(original)

        loaded = AlertManager.load("StormCast", "cell_50")
        assert loaded is not None
        assert loaded.cell_id == "cell_50"
        assert loaded.source == "StormCast"
        assert loaded.threats == {"hail": True}

    def test_load_nonexistent_returns_none(self, override_alerts_dir):
        assert AlertManager.load("StormCast", "does_not_exist") is None

    def test_load_all_returns_all_sources(self, override_alerts_dir):
        now = datetime.now()
        AlertManager.publish(AlertPayload(
            "severe_weather", "StormCast", "cell_y",
            [(35.0, -97.0)], now, now + timedelta(minutes=30)))
        AlertManager.publish(AlertPayload(
            "flash_flood", "FloodModule", "cell_y",
            [(36.0, -98.0)], now, now + timedelta(minutes=60)))

        all_alerts = AlertManager.load_all("cell_y")
        assert len(all_alerts) == 2
        sources = {a.source for a in all_alerts}
        assert sources == {"StormCast", "FloodModule"}

    def test_load_all_empty_dir(self, override_alerts_dir):
        assert AlertManager.load_all("nonexistent") == []

    def test_load_append_republish(self, override_alerts_dir):
        """Demonstrates the load → modify → republish workflow."""
        now = datetime(2026, 3, 4, 14, 0, 0)
        original = AlertPayload(
            "severe_weather", "StormCast", "cell_77",
            [(35.0, -97.0)], now, now + timedelta(minutes=30),
            threats={"hail": True},
        )
        AlertManager.publish(original)

        # Another module loads, appends a threat, and republishes
        loaded = AlertManager.load("StormCast", "cell_77")
        loaded.threats["tornado"] = True
        AlertManager.publish(loaded)

        # Verify the file now has both threats
        reloaded = AlertManager.load("StormCast", "cell_77")
        assert reloaded.threats == {"hail": True, "tornado": True}

    def test_cleanup_expired_removes_old_files(self, override_alerts_dir):
        """cleanup_expired should remove files with expires < now."""
        from datetime import timezone
        
        # 1. Active alert
        now = datetime.now(timezone.utc)
        active_eff = now - timedelta(minutes=10)
        active_exp = now + timedelta(minutes=20)
        a_active = AlertPayload(
            "severe_weather", "StormCast", "cell_active",
            [(35.0, -97.0)], active_eff, active_exp
        )
        
        # 2. Expired alert
        exp_eff = now - timedelta(minutes=40)
        exp_exp = now - timedelta(minutes=10)
        a_expired = AlertPayload(
            "severe_weather", "StormCast", "cell_expired",
            [(35.0, -97.0)], exp_eff, exp_exp
        )
        
        AlertManager.publish(a_active)
        AlertManager.publish(a_expired)
        
        # Verify both exist
        assert len(list(override_alerts_dir.glob("alert_*.json"))) == 2
        
        # Run cleanup
        count = AlertManager.cleanup_expired()
        
        # Verify only active remains
        assert count == 1
        remaining_files = list(override_alerts_dir.glob("alert_*.json"))
        assert len(remaining_files) == 1
        assert "cell_active" in remaining_files[0].name
