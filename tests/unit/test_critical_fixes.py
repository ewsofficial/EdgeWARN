"""
Regression tests for critical audit fixes (C1, C2, C3).

C1: Merge processing KF key migration
C2: Dead _check_reacquisition removal
C3: Config default alignment and fallback warnings
"""

import logging
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.kalman import (
    KalmanFilter,
    TrackingConfig,
)
from EdgeWARN.core.process.detect.kalman.config import (
    KalmanConfig,
    AssignmentConfig,
)
from EdgeWARN.core.process.detect.lineage.events import (
    LineageResult,
    MergeEvent,
    LineageEvent,
)


@pytest.fixture
def mock_io():
    io = MagicMock()
    io.write_info = MagicMock()
    io.write_error = MagicMock()
    io.write_warning = MagicMock()
    io.write_debug = MagicMock()
    return io


@pytest.fixture
def tracker(mock_io):
    return StormCellTracker(None, None, mock_io)


def _make_cell(cell_id, lat=35.0, lon=-97.0, refl=55, gates=50):
    """Helper to create a cell dict."""
    return {
        "id": cell_id,
        "centroid": [lat, lon],
        "max_refl": refl,
        "num_gates": gates,
        "bbox": [[lat - 0.1, lon - 0.1], [lat + 0.1, lon + 0.1],
                  [lat + 0.1, lon - 0.1], [lat - 0.1, lon + 0.1]],
    }


# ============================================================================
# C1: Merge processing migrates KF from dominant_parent key to child_id key
# ============================================================================

class TestC1MergeKFMigration:
    """Verify that after a merge, the Kalman filter is keyed to the child ID."""

    def test_merge_migrates_kf_to_child_id(self, tracker):
        """KF should be reachable under child_id after merge, not dominant_parent."""
        # Setup: two old cells, one new (merge)
        old_100 = _make_cell(100, lat=35.0, lon=-97.0, refl=60)
        old_101 = _make_cell(101, lat=35.05, lon=-96.95, refl=45)
        new_200 = _make_cell(200, lat=35.02, lon=-97.0, refl=62)

        entries = [old_100, old_101]

        # Pre-create KFs so they have history
        kf_100 = KalmanFilter()
        kf_100.initialize_from_cell(old_100)
        kf_101 = KalmanFilter()
        kf_101.initialize_from_cell(old_101)
        tracker._kalman_filters[100] = kf_100
        tracker._kalman_filters[101] = kf_101

        # Build lineage result with a confirmed merge
        lineage = LineageResult()
        lineage.merges.append(MergeEvent(
            child_id=200,
            parent_ids=[100, 101],
            dominant_parent=100,
        ))
        # Mark parents as matched so they don't go through secondary assignment
        lineage.cell_events[200] = LineageEvent.MERGE

        result = tracker.update_cells(
            entries, [new_200],
            timestamp="2024-01-01T00:02:00",
            lineage=lineage,
        )

        # KF should now be under child_id 200
        assert 200 in tracker._kalman_filters, "KF not found under child_id after merge"
        # KF should NOT be under old dominant parent 100
        assert 100 not in tracker._kalman_filters, "KF still under old dominant_parent key"
        # The KF under 200 should be the SAME object that was under 100 (migrated, not new)
        assert tracker._kalman_filters[200] is kf_100, "KF was recreated instead of migrated"

    def test_merge_cleans_up_non_dominant_parent_kf(self, tracker):
        """Non-dominant parent KF entries should be cleaned up after merge."""
        old_100 = _make_cell(100, refl=60)
        old_101 = _make_cell(101, refl=45)
        old_102 = _make_cell(102, refl=40)
        new_200 = _make_cell(200, refl=62)

        entries = [old_100, old_101, old_102]

        # Pre-create KFs for all parents
        for cid, cell in [(100, old_100), (101, old_101), (102, old_102)]:
            kf = KalmanFilter()
            kf.initialize_from_cell(cell)
            tracker._kalman_filters[cid] = kf

        lineage = LineageResult()
        lineage.merges.append(MergeEvent(
            child_id=200,
            parent_ids=[100, 101, 102],
            dominant_parent=100,
        ))
        lineage.cell_events[200] = LineageEvent.MERGE

        tracker.update_cells(
            entries, [new_200],
            timestamp="2024-01-01T00:02:00",
            lineage=lineage,
        )

        # Non-dominant parents should be cleaned up
        assert 101 not in tracker._kalman_filters, "KF for non-dominant parent 101 not cleaned up"
        assert 102 not in tracker._kalman_filters, "KF for non-dominant parent 102 not cleaned up"


# ============================================================================
# C2: Dead _check_reacquisition method removed
# ============================================================================

class TestC2DeadCodeRemoval:
    """Verify that the dead _check_reacquisition method has been removed."""

    def test_check_reacquisition_removed(self):
        """StormCellTracker should no longer have _check_reacquisition."""
        assert not hasattr(StormCellTracker, '_check_reacquisition'), \
            "_check_reacquisition method still exists — should have been removed"


# ============================================================================
# C3: Config default matches YAML, warns on fallback
# ============================================================================

class TestC3ConfigDefaults:
    """Verify config defaults match YAML and fallbacks log warnings."""

    def test_tracking_config_default_matches_yaml(self):
        """Python default for max_prediction_time_minutes should be 6.0."""
        config = TrackingConfig()
        assert config.max_prediction_time_minutes == 6.0, \
            f"Expected 6.0, got {config.max_prediction_time_minutes}"

    def test_tracking_config_warns_on_missing_yaml(self, caplog):
        """from_yaml should log a warning when the YAML file doesn't exist."""
        with caplog.at_level(logging.WARNING):
            TrackingConfig.from_yaml(Path("/nonexistent/kalman.yaml"))
        assert any("not found" in msg for msg in caplog.messages), \
            "No warning logged for missing YAML file"

    def test_kalman_config_warns_on_missing_yaml(self, caplog):
        """KalmanConfig.from_yaml should warn on missing file."""
        with caplog.at_level(logging.WARNING):
            KalmanConfig.from_yaml(Path("/nonexistent/kalman.yaml"))
        assert any("not found" in msg for msg in caplog.messages)

    def test_assignment_config_warns_on_missing_yaml(self, caplog):
        """AssignmentConfig.from_yaml should warn on missing file."""
        with caplog.at_level(logging.WARNING):
            AssignmentConfig.from_yaml(Path("/nonexistent/kalman.yaml"))
        assert any("not found" in msg for msg in caplog.messages)
