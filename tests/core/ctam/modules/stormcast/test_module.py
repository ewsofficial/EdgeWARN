from EdgeWARN.ctam.modules.StormCast import StormCastModule
from unittest.mock import patch
from datetime import datetime, timedelta, timezone


def _make_wind_field():
    levels = range(1000, 75, -25)
    wind_field = {}
    for idx, level in enumerate(levels):
        wind_field[f"u{level}"] = 8.0 + (idx * 0.1)
        wind_field[f"v{level}"] = 4.0 - (idx * 0.05)
    return wind_field


class HistoryCacheStub:
    def __init__(self, entries):
        self.entries = entries

    def get(self, cell_id):
        return self.entries


def test_run_prefers_current_duplicate_and_ignores_future_history():
    module = StormCastModule()
    storm_entry = {
        "id": 146904,
        "timestamp": "2026-03-22T00:22:38+00:00",
        "centroid": [34.4378, 277.7602],
        "bbox": [
            [34.3750, 277.7250],
            [34.4050, 277.6950],
            [34.4250, 277.7150],
            [34.4750, 277.7250],
            [34.4850, 277.7650],
            [34.4850, 277.7950],
            [34.4450, 277.7950],
            [34.4050, 277.7750],
            [34.3750, 277.7250],
        ],
        "dx": 2670.0,
        "dy": -80.0,
        "dt": 123.0,
        "modules": {},
        "properties": {
            "p100EchoTop30": 7.5,
            "EchoTop50": 4.0,
            "wind_field": _make_wind_field(),
        },
    }
    history_cache = HistoryCacheStub(
        [
            {
                "timestamp": "2026-03-22T00:26:32+00:00",
                "centroid": [34.52, 277.90],
                "properties": {"p100EchoTop30": 7.2, "EchoTop50": 3.9},
            },
            {
                "timestamp": "2026-03-22T00:22:38+00:00",
                "centroid": [34.43, 277.75],
                "properties": {"p100EchoTop30": 7.0, "EchoTop50": 3.8},
            },
            {
                "timestamp": "2026-03-22T00:18:00+00:00",
                "centroid": [34.36, 277.63],
                "properties": {"p100EchoTop30": 6.8, "EchoTop50": 3.5},
            },
        ]
    )

    module.run(storm_entry, history_cache=history_cache)

    result = storm_entry["modules"]["StormCast"]
    assert result["status"] == "success"
    assert result["can_generate_alerts"] is True
    assert result["polygon_0_30m"]


def test_alerts_returns_payload_for_eligible_cell():
    module = StormCastModule()
    storm_entry = {
        "id": 146904,
        "timestamp": "2026-03-22T00:22:38+00:00",
        "modules": {
            "StormCast": {
                "status": "success",
                "can_generate_alerts": True,
                "polygon_0_30m": [
                    (34.4, -82.2),
                    (34.5, -82.1),
                    (34.6, -82.0),
                    (34.4, -82.2),
                ],
            },
            "MorphoWind": {"severity_index": 0.7},
        },
    }

    with patch("EdgeWARN.ctam.modules.StormCast.AlertManager.load", return_value=None):
        alerts = module.alerts(storm_entry)

    assert alerts is not None
    assert len(alerts) == 1
    assert alerts[0].cell_id == 146904


def test_alerts_records_refresh_suppression_without_per_cell_log():
    module = StormCastModule()
    effective = datetime(2026, 3, 22, 0, 22, 38, tzinfo=timezone.utc)
    storm_entry = {
        "id": 146904,
        "timestamp": effective.isoformat(),
        "modules": {
            "StormCast": {
                "status": "success",
                "can_generate_alerts": True,
                "polygon_0_30m": [
                    (34.4, -82.2),
                    (34.5, -82.1),
                    (34.6, -82.0),
                    (34.4, -82.2),
                ],
            },
            "MorphoWind": {"severity_index": 0.7},
        },
    }
    previous_alert = type("PreviousAlert", (), {"effective_time": effective - timedelta(minutes=5)})()

    with patch("EdgeWARN.ctam.modules.StormCast.AlertManager.load", return_value=previous_alert):
        alerts = module.alerts(storm_entry)

    assert alerts is None
    result = storm_entry["modules"]["StormCast"]
    assert result["alert_outcome"] == "suppressed_refresh_spacing"
    assert result["next_alert_eligible_minutes"] == 10.0
