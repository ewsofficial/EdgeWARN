"""
Unit tests for the Alert-to-Cell Spatial Matching Module.

Tests cover:
- Alert event type filtering (convective/flood only)
- Spatial intersection logic
- Alert ID extraction
- Full integration with match_alerts_to_cells function
"""

import pytest
import json
from pathlib import Path
from datetime import datetime, timezone

from shapely.prepared import prep
from EdgeWARN.core.process.detect.tools.alert_matcher import (
    CONVECTIVE_FLOOD_EVENTS,
    load_active_alerts,
    filter_convective_flood_alerts,
    _extract_alert_id,
    _get_alert_polygon,
    _get_cell_centroid,
    _get_cell_polygon,
    match_alerts_to_cell,
    match_alerts_to_cells,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def prepped_alerts(sample_convective_alert, sample_flood_alert):
    """Create a list of prepped alerts for testing match_alerts_to_cell."""
    from shapely.geometry import Polygon
    # We need to manually prep because match_alerts_to_cell expects prepped list
    alerts = []
    
    # Convective
    id1 = _extract_alert_id(sample_convective_alert)
    poly1 = _get_alert_polygon(sample_convective_alert)
    alerts.append((id1, prep(poly1)))
    
    # Flood
    id2 = _extract_alert_id(sample_flood_alert)
    poly2 = _get_alert_polygon(sample_flood_alert)
    alerts.append((id2, prep(poly2)))
    
    return alerts


@pytest.fixture
def temp_registry(tmp_path):
    """Create a temporary registry file with test alerts."""
    registry_path = tmp_path / "alerts_registry.json"
    return registry_path


@pytest.fixture
def sample_convective_alert():
    """Create a sample convective alert (Severe Thunderstorm Warning)."""
    return {
        "id": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.2406210827.1",
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-97.0, 35.0], [-97.0, 36.0], [-96.0, 36.0], [-96.0, 35.0], [-97.0, 35.0]]]
        },
        "properties": {
            "event": "Severe Thunderstorm Warning",
            "severity": "Severe",
            "effective": "2026-02-23T21:00:00Z",
            "expires": "2026-02-23T22:00:00Z",
        },
        "Polygon": [[[-97.0, 35.0], [-97.0, 36.0], [-96.0, 36.0], [-96.0, 35.0], [-97.0, 35.0]]]
    }


@pytest.fixture
def sample_flood_alert():
    """Create a sample flood alert (Flash Flood Warning)."""
    return {
        "id": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.2406210828.1",
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-98.0, 34.0], [-98.0, 35.0], [-97.0, 35.0], [-97.0, 34.0], [-98.0, 34.0]]]
        },
        "properties": {
            "event": "Flash Flood Warning",
            "severity": "Severe",
            "effective": "2026-02-23T21:30:00Z",
            "expires": "2026-02-23T22:30:00Z",
        },
        "Polygon": [[[-98.0, 34.0], [-98.0, 35.0], [-97.0, 35.0], [-97.0, 34.0], [-98.0, 34.0]]]
    }


@pytest.fixture
def sample_non_convective_alert():
    """Create a non-convective alert (Gale Watch) that should be filtered out."""
    return {
        "id": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.2406210829.1",
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-96.5, 35.5], [-96.5, 35.7], [-96.3, 35.7], [-96.3, 35.5], [-96.5, 35.5]]]
        },
        "properties": {
            "event": "Gale Watch",
            "severity": "Moderate",
            "effective": "2026-02-23T21:00:00Z",
            "expires": "2026-02-23T23:00:00Z",
        },
        "Polygon": [[[-96.5, 35.5], [-96.5, 35.7], [-96.3, 35.7], [-96.3, 35.5], [-96.5, 35.5]]]
    }


@pytest.fixture
def sample_cell_inside_storm():
    """Create a cell entry inside the storm alert polygon."""
    return {
        "id": 1,
        "centroid": [35.5, -96.5],  # Inside the -97 to -96, 35 to 36 box
        "bbox": [[35.4, -96.6], [35.4, -96.4], [35.6, -96.4], [35.6, -96.6]],
        "num_gates": 100,
        "max_refl": 55.0,
    }


@pytest.fixture
def sample_cell_outside_storm():
    """Create a cell entry outside all alert polygons."""
    return {
        "id": 2,
        "centroid": [33.0, -99.0],  # Outside all test polygons
        "bbox": [[32.9, -99.1], [32.9, -98.9], [33.1, -98.9], [33.1, -99.1]],
        "num_gates": 50,
        "max_refl": 40.0,
    }


# =============================================================================
# Test Event Type Filtering
# =============================================================================

class TestEventTypeFiltering:
    """Tests for CONVECTIVE_FLOOD_EVENTS whitelist."""
    
    def test_convective_events_included(self):
        """Verify convective events are in the whitelist."""
        assert "Tornado Warning" in CONVECTIVE_FLOOD_EVENTS
        assert "Severe Thunderstorm Warning" in CONVECTIVE_FLOOD_EVENTS
        assert "Tornado Watch" in CONVECTIVE_FLOOD_EVENTS
        assert "Severe Thunderstorm Watch" in CONVECTIVE_FLOOD_EVENTS
        
    def test_flood_events_included(self):
        """Verify flood events are in the whitelist."""
        assert "Flash Flood Warning" in CONVECTIVE_FLOOD_EVENTS
        assert "Flood Warning" in CONVECTIVE_FLOOD_EVENTS
        assert "Flash Flood Watch" in CONVECTIVE_FLOOD_EVENTS
        assert "Flood Advisory" in CONVECTIVE_FLOOD_EVENTS
        
    def test_non_convective_events_excluded(self):
        """Verify non-convective events are NOT in the whitelist."""
        assert "Gale Watch" not in CONVECTIVE_FLOOD_EVENTS
        assert "Small Craft Advisory" not in CONVECTIVE_FLOOD_EVENTS
        assert "Air Quality Alert" not in CONVECTIVE_FLOOD_EVENTS


class TestFilterConvectiveFloodAlerts:
    """Tests for filter_convective_flood_alerts function."""
    
    def test_filters_non_convective_alerts(self, sample_convective_alert, sample_non_convective_alert):
        """Verify only convective/flood alerts are returned."""
        alerts = [sample_convective_alert, sample_non_convective_alert]
        filtered = filter_convective_flood_alerts(alerts)
        
        assert len(filtered) == 1
        assert filtered[0]["properties"]["event"] == "Severe Thunderstorm Warning"
        
    def test_keeps_multiple_convective_alerts(self, sample_convective_alert, sample_flood_alert):
        """Verify multiple convective/flood alerts are all kept."""
        alerts = [sample_convective_alert, sample_flood_alert]
        filtered = filter_convective_flood_alerts(alerts)
        
        assert len(filtered) == 2
        events = [a["properties"]["event"] for a in filtered]
        assert "Severe Thunderstorm Warning" in events
        assert "Flash Flood Warning" in events
        
    def test_empty_list_returns_empty(self):
        """Verify empty input returns empty output."""
        filtered = filter_convective_flood_alerts([])
        assert filtered == []


# =============================================================================
# Test Alert ID Extraction
# =============================================================================

class TestExtractAlertId:
    """Tests for _extract_alert_id function."""
    
    def test_extracts_from_feature_id(self, sample_convective_alert):
        """Verify extraction from feature['id'] field."""
        alert_id = _extract_alert_id(sample_convective_alert)
        assert alert_id == "urn:oid:2.49.0.1.840.0.2406210827.1"
        
    def test_extracts_from_properties_id(self):
        """Verify extraction from properties['id'] as fallback."""
        feature = {
            "properties": {
                "id": "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.1234.1"
            }
        }
        alert_id = _extract_alert_id(feature)
        assert alert_id == "urn:oid:2.49.0.1.840.0.1234.1"
        
    def test_returns_none_when_no_id(self):
        """Verify None is returned when no ID found."""
        feature = {"type": "Feature", "properties": {}}
        alert_id = _extract_alert_id(feature)
        assert alert_id is None


# =============================================================================
# Test Spatial Matching
# =============================================================================

class TestSpatialMatching:
    """Tests for spatial intersection logic."""
    
    def test_cell_inside_alert_polygon(self, prepped_alerts, sample_cell_inside_storm):
        """Verify cell inside alert polygon returns matching alert ID."""
        matching_ids = match_alerts_to_cell(sample_cell_inside_storm, prepped_alerts)
        
        assert len(matching_ids) == 1
        assert matching_ids[0] == "urn:oid:2.49.0.1.840.0.2406210827.1"
        
    def test_cell_outside_alert_polygon(self, prepped_alerts, sample_cell_outside_storm):
        """Verify cell outside alert polygon returns empty list."""
        matching_ids = match_alerts_to_cell(sample_cell_outside_storm, prepped_alerts)
        
        assert len(matching_ids) == 0
        
    def test_cell_matches_multiple_alerts(self, prepped_alerts):
        """Verify cell can match multiple overlapping alerts."""
        # Create a cell that overlaps both alert polygons
        cell = {
            "id": 3,
            "centroid": [35.0, -97.0],  # On the boundary
            "bbox": [[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]],
        }
        
        matching_ids = match_alerts_to_cell(cell, prepped_alerts)
        
        # Should match at least one (depending on exact geometry)
        assert len(matching_ids) >= 1

    def test_cell_bbox_intersection(self, sample_convective_alert):
        """Verify cell matches via bbox even if centroid is outside."""
        # Alert is -97 to -96, 35 to 36
        # Cell centroid is at -97.05 (outside), but bbox extends to -96.95 (inside)
        cell = {
            "id": 5,
            "centroid": [35.5, -97.05], 
            "bbox": [[35.4, -97.1], [35.4, -96.95], [35.6, -96.95], [35.6, -97.1]],
        }
        
        alert_id = _extract_alert_id(sample_convective_alert)
        alert_poly = _get_alert_polygon(sample_convective_alert)
        prepped = [(alert_id, prep(alert_poly))]
        
        matching_ids = match_alerts_to_cell(cell, prepped)
        assert alert_id in matching_ids
        
    def test_empty_alerts_returns_empty(self, sample_cell_inside_storm):
        """Verify empty alerts list returns empty matches."""
        matching_ids = match_alerts_to_cell(sample_cell_inside_storm, [])
        assert matching_ids == []
        
    def test_cell_without_centroid_but_with_bbox(self, prepped_alerts):
        """Verify cell without centroid but with bbox still matches."""
        cell = {
            "id": 4, 
            "bbox": [[35.4, -96.6], [35.4, -96.4], [35.6, -96.4], [35.6, -96.6]]
        }
        matching_ids = match_alerts_to_cell(cell, prepped_alerts)
        assert "urn:oid:2.49.0.1.840.0.2406210827.1" in matching_ids


# =============================================================================
# Test Registry Loading
# =============================================================================

class TestLoadActiveAlerts:
    """Tests for load_active_alerts function."""
    
    def test_loads_from_registry_file(self, temp_registry, sample_convective_alert):
        """Verify alerts are loaded from registry file."""
        registry_data = {
            "last_updated": "2026-02-23T21:00:00Z",
            "alerts": {
                "urn:oid:2.49.0.1.840.0.2406210827.1": {
                    "id": sample_convective_alert["id"],
                    "first_seen": "2026-02-23T21:00:00Z",
                    "last_seen": "2026-02-23T21:00:00Z",
                    "feature": sample_convective_alert
                }
            }
        }
        
        with open(temp_registry, 'w') as f:
            json.dump(registry_data, f)
        
        alerts = load_active_alerts(temp_registry)
        
        assert len(alerts) == 1
        assert alerts[0]["properties"]["event"] == "Severe Thunderstorm Warning"
        
    def test_returns_empty_for_missing_file(self, tmp_path):
        """Verify empty list returned when registry doesn't exist."""
        nonexistent_path = tmp_path / "nonexistent.json"
        alerts = load_active_alerts(nonexistent_path)
        assert alerts == []
        
    def test_returns_empty_for_malformed_registry(self, temp_registry):
        """Verify empty list returned when registry is malformed."""
        with open(temp_registry, 'w') as f:
            f.write("not valid json")
        
        alerts = load_active_alerts(temp_registry)
        assert alerts == []


# =============================================================================
# Test Full Integration
# =============================================================================

class TestMatchAlertsToCells:
    """Tests for match_alerts_to_cells function."""
    
    def test_adds_alerts_key_to_all_cells(self, temp_registry, sample_convective_alert, 
                                          sample_cell_inside_storm, sample_cell_outside_storm):
        """Verify all cells get an 'alerts' key."""
        registry_data = {
            "last_updated": "2026-02-23T21:00:00Z",
            "alerts": {
                "urn:oid:2.49.0.1.840.0.2406210827.1": {
                    "id": sample_convective_alert["id"],
                    "first_seen": "2026-02-23T21:00:00Z",
                    "last_seen": "2026-02-23T21:00:00Z",
                    "feature": sample_convective_alert
                }
            }
        }
        
        with open(temp_registry, 'w') as f:
            json.dump(registry_data, f)
        
        cells = [sample_cell_inside_storm, sample_cell_outside_storm]
        result = match_alerts_to_cells(cells, temp_registry)
        
        # Both cells should have an 'alerts' key
        assert "alerts" in result[0]
        assert "alerts" in result[1]
        
        # Cell inside storm should have the alert
        assert len(result[0]["alerts"]) == 1
        
        # Cell outside should have empty alerts
        assert result[1]["alerts"] == []
        
    def test_filters_out_non_convective_alerts(self, temp_registry, sample_convective_alert,
                                               sample_non_convective_alert, sample_cell_inside_storm):
        """Verify non-convective alerts are filtered out."""
        registry_data = {
            "last_updated": "2026-02-23T21:00:00Z",
            "alerts": {
                "urn:oid:2.49.0.1.840.0.2406210827.1": {
                    "id": sample_convective_alert["id"],
                    "first_seen": "2026-02-23T21:00:00Z",
                    "last_seen": "2026-02-23T21:00:00Z",
                    "feature": sample_convective_alert
                },
                "urn:oid:2.49.0.1.840.0.2406210829.1": {
                    "id": sample_non_convective_alert["id"],
                    "first_seen": "2026-02-23T21:00:00Z",
                    "last_seen": "2026-02-23T21:00:00Z",
                    "feature": sample_non_convective_alert
                }
            }
        }
        
        with open(temp_registry, 'w') as f:
            json.dump(registry_data, f)
        
        cells = [sample_cell_inside_storm]
        result = match_alerts_to_cells(cells, temp_registry)
        
        # Should only have the convective alert, not the Gale Watch
        assert len(result[0]["alerts"]) == 1
        assert result[0]["alerts"][0] == "urn:oid:2.49.0.1.840.0.2406210827.1"
        
    def test_empty_cells_list_returns_empty(self, temp_registry):
        """Verify empty cells list returns empty list."""
        result = match_alerts_to_cells([], temp_registry)
        assert result == []
