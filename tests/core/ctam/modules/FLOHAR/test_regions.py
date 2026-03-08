"""
FLOHAR Region Extraction Unit Tests

Tests for the region extraction pipeline: single regions, multiple
disjoint regions, area filtering, and polygon coordinate validity.
"""

import numpy as np
import pytest

from EdgeWARN.core.ctam.modules.FLOHAR.regions import (
    extract_regions,
    _pixel_area_km2,
    _compute_region_area_km2,
)


# ── Helpers ─────────────────────────────────────────────────────────

def _make_lat_lon(nrows=20, ncols=20, lat_start=40.0, lon_start=-95.0, step=0.01):
    """Create synthetic lat/lon coordinate arrays."""
    lats = np.linspace(lat_start, lat_start - (nrows - 1) * step, nrows)
    lons = np.linspace(lon_start, lon_start + (ncols - 1) * step, ncols)
    return lats, lons


def _make_pillar_grids(shape):
    """Create dummy pillar grids."""
    return {
        "rainfall": np.random.rand(*shape).astype(np.float64),
        "hydro": np.random.rand(*shape).astype(np.float64),
        "ffg": np.random.rand(*shape).astype(np.float64),
    }


# ─────────────────────────────────────────────────────────────────────
# Test 1: Single region extraction
# ─────────────────────────────────────────────────────────────────────

def test_single_region():
    """A single contiguous cluster above threshold should produce one region."""
    nrows, ncols = 20, 20
    lats, lons = _make_lat_lon(nrows, ncols)

    # Create a threat grid with one cluster in the centre
    threat = np.zeros((nrows, ncols), dtype=int)
    threat[8:13, 8:13] = 60  # 5x5 block of score 60

    pillars = _make_pillar_grids((nrows, ncols))

    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,  # disable area filter for this test
    )

    assert len(regions) == 1
    r = regions[0]
    assert r["peak_score"] == 60
    assert r["mean_score"] == 60.0
    assert r["severity"] == "warning"
    assert r["area_km2"] > 0
    assert len(r["geometry"]) >= 3  # valid polygon has at least 3 points
    assert "rainfall" in r["pillar_peaks"]
    assert "hydro" in r["pillar_peaks"]
    assert "ffg" in r["pillar_peaks"]


# ─────────────────────────────────────────────────────────────────────
# Test 2: Multiple disjoint regions
# ─────────────────────────────────────────────────────────────────────

def test_multiple_disjoint_regions():
    """Two separated clusters should produce two separate regions."""
    nrows, ncols = 30, 30
    lats, lons = _make_lat_lon(nrows, ncols)

    threat = np.zeros((nrows, ncols), dtype=int)
    # Cluster 1: top-left
    threat[2:5, 2:5] = 50
    # Cluster 2: bottom-right (well separated)
    threat[25:28, 25:28] = 80

    pillars = _make_pillar_grids((nrows, ncols))

    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,
    )

    assert len(regions) == 2
    # Should be sorted by area (both same size, but let's just check count)
    severities = {r["severity"] for r in regions}
    assert "warning" in severities
    assert "emergency" in severities


# ─────────────────────────────────────────────────────────────────────
# Test 3: Small region filtering
# ─────────────────────────────────────────────────────────────────────

def test_small_region_filtered():
    """Regions below min_area_km2 should be discarded."""
    nrows, ncols = 20, 20
    lats, lons = _make_lat_lon(nrows, ncols)

    threat = np.zeros((nrows, ncols), dtype=int)
    # Single pixel — area will be very small (~1 km²)
    threat[10, 10] = 50

    pillars = _make_pillar_grids((nrows, ncols))

    # With high min_area → should be filtered
    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=100.0,  # very high threshold
    )
    assert len(regions) == 0

    # With no min_area → should be kept
    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,
    )
    assert len(regions) == 1


# ─────────────────────────────────────────────────────────────────────
# Test 4: Polygon coordinates within grid bounds
# ─────────────────────────────────────────────────────────────────────

def test_polygon_bounds():
    """Output polygon coordinates should be within the lat/lon grid range."""
    nrows, ncols = 20, 20
    lat_start, lon_start = 40.0, -95.0
    step = 0.01
    lats, lons = _make_lat_lon(nrows, ncols, lat_start, lon_start, step)

    threat = np.zeros((nrows, ncols), dtype=int)
    threat[5:15, 5:15] = 70

    pillars = _make_pillar_grids((nrows, ncols))

    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,
    )

    assert len(regions) == 1

    # Check all polygon coords are within grid bounds (with half-pixel buffer)
    max_lat = lat_start + step
    min_lat = lat_start - (nrows) * step
    min_lon = lon_start - step
    max_lon = lon_start + (ncols) * step

    for lon, lat in regions[0]["geometry"]:
        assert min_lat <= lat <= max_lat, \
            f"Lat {lat} out of bounds [{min_lat}, {max_lat}]"
        assert min_lon <= lon <= max_lon, \
            f"Lon {lon} out of bounds [{min_lon}, {max_lon}]"


# ─────────────────────────────────────────────────────────────────────
# Test 5: Pixel area calculation
# ─────────────────────────────────────────────────────────────────────

def test_pixel_area():
    """Pixel area should be positive and decrease with higher latitude."""
    area_equator = _pixel_area_km2(0.0, 0.01, 0.01)
    area_mid = _pixel_area_km2(40.0, 0.01, 0.01)
    area_high = _pixel_area_km2(60.0, 0.01, 0.01)

    assert area_equator > 0
    assert area_mid > 0
    assert area_high > 0
    # Pixels get narrower at higher latitudes
    assert area_equator > area_mid > area_high


# ─────────────────────────────────────────────────────────────────────
# Test 6: Water body mask
# ─────────────────────────────────────────────────────────────────────

def test_water_body_mask():
    """Water body mask should suppress regions over permanent water."""
    nrows, ncols = 20, 20
    lats, lons = _make_lat_lon(nrows, ncols)

    threat = np.zeros((nrows, ncols), dtype=int)
    threat[5:15, 5:15] = 70

    pillars = _make_pillar_grids((nrows, ncols))

    # Without mask → region found
    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,
    )
    assert len(regions) == 1

    # With mask covering the entire threat area → no regions
    water_mask = np.zeros((nrows, ncols), dtype=bool)
    water_mask[5:15, 5:15] = True  # mask exactly where threat is

    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,
        water_body_mask=water_mask,
    )
    assert len(regions) == 0


# ─────────────────────────────────────────────────────────────────────
# Test 7: Empty grid → no regions
# ─────────────────────────────────────────────────────────────────────

def test_empty_grid():
    """An empty threat grid should produce no regions."""
    nrows, ncols = 10, 10
    lats, lons = _make_lat_lon(nrows, ncols)

    threat = np.zeros((nrows, ncols), dtype=int)
    pillars = _make_pillar_grids((nrows, ncols))

    regions = extract_regions(
        threat, lats, lons, pillars,
        threshold=25, min_area_km2=0.0,
    )
    assert len(regions) == 0
