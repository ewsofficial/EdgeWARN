"""Phase 5 regression: a built-in forecast survives into the next track cycle."""
from __future__ import annotations

import copy

from EdgeWARN.ctam.builtins.stormcast import BuiltinStormCastAdapter
from EdgeWARN.process.detect.kalman import default_tracking_config
from EdgeWARN.process.detect.track import StormCellTracker


class _CycleService:
    def history(self, cell_id):
        return []

    def previous_alert(self, cell_id):
        return None

    def publish(self, alerts):
        return len(alerts)


class _IO:
    def write_info(self, message): pass
    def write_debug(self, message): pass
    def write_warning(self, message): pass
    def write_error(self, message): pass


def test_stormcast_cycle_n_history_drives_cycle_n_plus_1_tracker():
    """The published history entry retains StormCast velocity for tracking."""
    cycle_n = {
        "id": 901,
        "timestamp": "2026-08-05T12:00:00+00:00",
        "centroid": [35.25, 262.75],
        "bbox": [[35.2, 262.7], [35.3, 262.8]],
        "num_gates": 100,
        "max_refl": 55.0,
        "tracking_mode": "active",
        "prediction_count": 0,
        "confidence": 1.0,
        "dx": 500.0,
        "dy": 250.0,
        "dt": 300.0,
        "properties": {
            "p100EchoTop30": 10.0,
            "EchoTop50": 8.0,
            "wind_field": {
                "u850": 12.0, "v850": 4.0,
                "u700": 14.0, "v700": 5.0,
                "u500": 18.0, "v500": 7.0,
                "u250": 22.0, "v250": 9.0,
            },
        },
        "modules": {},
    }
    BuiltinStormCastAdapter(_CycleService()).run(cycle_n)
    assert cycle_n["modules"]["StormCast"]["status"] == "success"

    # This is the payload the integration publication coordinator writes to
    # ``data/cells/<id>.json`` at the end of cycle N.
    history_file_payload = [copy.deepcopy(cycle_n)]
    tracker = StormCellTracker(
        ps_old=None,
        ps_new=None,
        io_manager=_IO(),
        tracking_config=default_tracking_config(),
    )
    tracker.update_cells(
        entries=history_file_payload,
        updated_data=[
            {
                "id": 901,
                "centroid": [35.26, 262.76],
                "bbox": [[35.21, 262.71], [35.31, 262.81]],
                "num_gates": 101,
                "max_refl": 56.0,
            }
        ],
        timestamp="2026-08-05T12:05:00+00:00",
        dt_seconds=300.0,
    )

    kalman = tracker._kalman_filters[901]
    forecast = cycle_n["modules"]["StormCast"]
    assert kalman.state.u == forecast["u"]
    assert kalman.state.v == forecast["v"]
