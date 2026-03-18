"""
FLOHAR Engine Unit Tests

Tests for the scoring engine: normalization functions, pillar scoring,
composite threat scores, NaN handling, RQI quality control, and
severity classification.
"""

import numpy as np
import pytest

from EdgeWARN.ctam.modules.FLOHAR.engine import (
    compute_threat_grid,
    classify_severity_scalar,
    _normalize_ari,
    _normalize_ffg,
    _normalize_soil_sat,
    _rqi_weight,
    _sigmoid,
)


# ── Helpers ─────────────────────────────────────────────────────────

def _make_grid(value, shape=(10, 10)):
    """Create a uniform grid filled with `value`."""
    return np.full(shape, value, dtype=np.float64)


def _zeros(shape=(10, 10)):
    return np.zeros(shape, dtype=np.float64)


def _ones(shape=(10, 10)):
    return np.ones(shape, dtype=np.float64)


# ─────────────────────────────────────────────────────────────────────
# Test 1: All zeros → all pixels score 0
# ─────────────────────────────────────────────────────────────────────

def test_all_zeros():
    """All-zero inputs should produce near-zero threat scores.
    
    Note: sigmoid(0, x0=1.5, k=2.0) ≈ 0.047, so the hydro pillar
    contributes a tiny floor value even with zero streamflow. This
    produces scores of ~1 after rounding. This is correct behaviour.
    """
    z = _zeros()
    grids = {
        "ari_max": z.copy(), "ari_30m": z.copy(), "ari_01h": z.copy(),
        "crest_streamflow": z.copy(), "hp_streamflow": z.copy(),
        "soil_sat": z.copy(), "ffg_ratio": z.copy(), "rqi": _ones()
    }
    threat, r, h, f = compute_threat_grid(grids)
    assert np.all(threat <= 2), f"Expected near-zero, got max={threat.max()}"


# ─────────────────────────────────────────────────────────────────────
# Test 2: Uniform extreme values → score ≈ 100
# ─────────────────────────────────────────────────────────────────────

def test_uniform_extreme():
    """Extreme values across all indicators, with RQI=1, should score ≥ 90."""
    grids = {
        "ari_max": _make_grid(500),
        "ari_30m": _make_grid(500),
        "ari_01h": _make_grid(500),
        "crest_streamflow": _make_grid(10),
        "hp_streamflow": _make_grid(10),
        "soil_sat": _make_grid(95.0),
        "ffg_ratio": _make_grid(500.0),
        "rqi": _ones(),
    }
    threat, r, h, f = compute_threat_grid(grids)
    assert np.all(threat >= 90), f"Min threat score: {threat.min()}"


# ─────────────────────────────────────────────────────────────────────
# Test 3: ARI-only scenario
# ─────────────────────────────────────────────────────────────────────

def test_ari_only():
    """High ARI with zero streamflow/FFG → moderate rainfall score, low others."""
    grids = {
        "ari_max": _make_grid(200),
        "ari_30m": _make_grid(200),
        "ari_01h": _make_grid(200),
        "crest_streamflow": _zeros(),
        "hp_streamflow": _zeros(),
        "soil_sat": _zeros(),
        "ffg_ratio": _zeros(),
        "rqi": _ones(),
    }
    threat, r, h, f = compute_threat_grid(grids)
    # Rainfall pillar should be ~1.0 (ARI=200 → ceiling)
    assert np.all(r > 0.9), f"Rainfall pillar too low: {r.min()}"
    # FFG should be 0
    assert np.all(f == 0.0)
    # Final score should be moderate (only 40% weight from rainfall)
    assert np.all(threat > 0)
    assert np.all(threat < 80)


# ─────────────────────────────────────────────────────────────────────
# Test 4: FFG ratio edge cases
# ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("ratio,expected", [
    (0.5, 0.0),     # below ramp start
    (0.85, 0.0),    # at ramp start → 0
    (1.25, 0.5),    # at midpoint
    (2.0, 1.0),     # at ramp end
    (3.0, 1.0),     # above ramp end (clamped)
])
def test_ffg_edge_cases(ratio, expected):
    """FFG normalization edge cases match spec."""
    arr = _make_grid(ratio, shape=(1, 1))
    result = _normalize_ffg(arr)
    assert abs(result[0, 0] - expected) < 0.01, \
        f"FFG({ratio}) = {result[0, 0]}, expected {expected}"


# ─────────────────────────────────────────────────────────────────────
# Test 5: NaN handling — NaN pixels produce valid output
# ─────────────────────────────────────────────────────────────────────

def test_nan_handling():
    """Grids with NaN pixels should produce valid (non-NaN) threat scores."""
    shape = (5, 5)
    grid = _make_grid(100.0, shape)
    grid[2, 2] = np.nan  # inject NaN in one pixel

    grids = {
        "ari_max": grid.copy(), "ari_30m": grid.copy(), "ari_01h": grid.copy(),
        "crest_streamflow": grid.copy(), "hp_streamflow": grid.copy(),
        "soil_sat": _make_grid(50.0, shape), "ffg_ratio": grid.copy(),
        "rqi": _ones(shape)
    }
    threat, r, h, f = compute_threat_grid(grids)
    assert not np.any(np.isnan(threat)), "NaN leaked into threat grid"


# ─────────────────────────────────────────────────────────────────────
# Test 6: All NaN → score 0
# ─────────────────────────────────────────────────────────────────────

def test_all_nan():
    """All-NaN inputs should produce all-zero threat scores."""
    nans = _make_grid(np.nan)
    grids = {
        "ari_max": nans.copy(), "ari_30m": nans.copy(), "ari_01h": nans.copy(),
        "crest_streamflow": nans.copy(), "hp_streamflow": nans.copy(),
        "soil_sat": nans.copy(), "ffg_ratio": nans.copy(), "rqi": nans.copy()
    }
    threat, r, h, f = compute_threat_grid(grids)
    assert np.all(threat == 0), f"Expected all zeros, got max={threat.max()}"


# ─────────────────────────────────────────────────────────────────────
# Test 7: Soil saturation boost
# ─────────────────────────────────────────────────────────────────────

def test_soil_saturation_boost():
    """Score should be amplified when soil_sat > 0.85."""
    base_args = dict(
        ari_max=_make_grid(50),
        ari_30m=_make_grid(50),
        ari_01h=_make_grid(50),
        crest_streamflow=_make_grid(2.0),
        hp_streamflow=_make_grid(2.0),
        ffg_ratio=_make_grid(150.0), # 150% ratio
        rqi=_ones(),
    )
    grids_low = base_args.copy()
    grids_low["soil_sat"] = _make_grid(50.0)
    threat_low, _, _, _ = compute_threat_grid(grids_low)

    grids_high = base_args.copy()
    grids_high["soil_sat"] = _make_grid(95.0)
    threat_high, _, _, _ = compute_threat_grid(grids_high)

    # High soil sat should produce higher scores
    assert np.all(threat_high >= threat_low), \
        f"Boost failed: low={threat_low.mean()}, high={threat_high.mean()}"


# ─────────────────────────────────────────────────────────────────────
# Test 8: Severity tier boundaries
# ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("score,expected_tier", [
    (29, "none"),
    (30, "advisory"),
    (54, "advisory"),
    (55, "warning"),
    (79, "warning"),
    (80, "emergency"),
    (100, "emergency"),
    (0, "none"),
])
def test_severity_boundaries(score, expected_tier):
    """Severity tiers match spec boundaries."""
    assert classify_severity_scalar(score) == expected_tier


# ─────────────────────────────────────────────────────────────────────
# Test 9: RQI quality control
# ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("rqi_val,expected_weight", [
    (1.0, 1.0),
    (0.9, 1.0),
    (0.7, 1.0),     # Now acts as a binary mask, so 0.7 >= 0.5 threshold gets 1.0
    (0.5, 1.0),
    (0.2, 0.0),     # Below threshold -> hard mask
])
def test_rqi_quality_control(rqi_val, expected_weight):
    """RQI weighting matches spec."""
    arr = _make_grid(rqi_val, shape=(1, 1))
    result = _rqi_weight(arr)
    assert abs(result[0, 0] - expected_weight) < 0.05, \
        f"RQI({rqi_val}) = {result[0, 0]}, expected ~{expected_weight}"


# ─────────────────────────────────────────────────────────────────────
# Test 10: Sentinel values treated as NaN
# ─────────────────────────────────────────────────────────────────────

def test_sentinel_handling():
    """Sentinel values (-999, -9999) should be treated as missing data."""
    shape = (3, 3)
    sentinel = _make_grid(-999.0, shape)
    normal = _make_grid(100.0, shape)

    grids = {
        "ari_max": sentinel.copy(), "ari_30m": sentinel.copy(), "ari_01h": sentinel.copy(),
        "crest_streamflow": sentinel.copy(), "hp_streamflow": sentinel.copy(),
        "soil_sat": sentinel.copy(), "ffg_ratio": sentinel.copy(),
        "rqi": _ones(shape)
    }
    threat, _, _, _ = compute_threat_grid(grids)
    # Sentinel-filled grids → all zeros (treated as NaN)
    assert np.all(threat == 0)
