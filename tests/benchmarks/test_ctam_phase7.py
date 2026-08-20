"""Measured regression budgets recorded in ``docs/ctam/performance-baseline.md``."""
from __future__ import annotations

import time
from unittest.mock import patch


def _cell():
    return {
        "id": 1,
        "timestamp": "2026-08-05T12:00:00+00:00",
        "centroid": [35.25, 262.75],
        "dx": 500.0,
        "dy": 250.0,
        "dt": 300.0,
        "properties": {"wind_field": {
            "u850": 12.0, "v850": 4.0,
            "u700": 14.0, "v700": 5.0,
            "u500": 18.0, "v500": 7.0,
            "u250": 22.0, "v250": 9.0,
        }},
        "modules": {},
    }


def test_stormcast_only_cycle_stays_within_phase7_latency_budget():
    from EdgeWARN.ctam.run import run_ctam

    started = time.monotonic()
    result = run_ctam([_cell()])
    elapsed = time.monotonic() - started

    assert result[0]["modules"]["StormCast"]["status"] in {"success", "skipped"}
    assert elapsed < 1.0


def test_disabled_ctam_starts_no_runner_or_loopback_server():
    from EdgeWARN.process.integrate.pipeline import _run_ctam_if_enabled

    cells = [_cell()]
    with patch("EdgeWARN.ctam.run.run_ctam") as run_ctam:
        assert _run_ctam_if_enabled(cells, "20260805-120000", True) is cells
    run_ctam.assert_not_called()
