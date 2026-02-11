import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker

# Sample Timestamps
TS_OLD = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_NEW = datetime(2023, 1, 1, 12, 2, 0, tzinfo=timezone.utc)
TS_NEWER = datetime(2023, 1, 1, 12, 4, 0, tzinfo=timezone.utc)
TS_FALLBACK = datetime(2023, 1, 1, 12, 6, 0, tzinfo=timezone.utc)

@pytest.fixture
def update_checker(mock_io_manager):
    """Fixture for initialized MRMSUpdateChecker."""
    with patch("EdgeWARN.core.schedule.scheduler.io_manager", mock_io_manager):
        yield MRMSUpdateChecker(verbose=True)

def test_fallback_when_no_intersection(update_checker, mocker):
    """Test that fallback is triggered when S3 has no common timestamps."""
    # Mock _get_modifier_times to return disjoint sets
    def side_effect(mod, ref_dt, trace_id=None, last_processed=None):
        if mod[1] == "Mod1":
            return {TS_OLD}
        else:
            return {TS_NEWER}
            
    mocker.patch.object(update_checker, "_get_modifier_times", side_effect=side_effect)
    
    # Mock check_https_fallback
    mocker.patch.object(update_checker, "check_https_fallback", return_value=TS_FALLBACK)
    
    modifiers = [("R", "Mod1", "D"), ("R", "Mod2", "D")]
    common = update_checker.latest_common_minute_1h(modifiers)
    
    # Ensure fallback was used
    update_checker.check_https_fallback.assert_called_once()
    assert common == TS_FALLBACK

def test_fallback_when_no_files_found(update_checker, mocker):
    """Test that fallback is triggered when S3 returns no files."""
    # Mock _get_modifier_times to return empty sets
    mocker.patch.object(update_checker, "_get_modifier_times", return_value=set())
    
    # Mock check_https_fallback
    mocker.patch.object(update_checker, "check_https_fallback", return_value=TS_FALLBACK)
    
    modifiers = [("R", "Mod1", "D"), ("R", "Mod2", "D")]
    common = update_checker.latest_common_minute_1h(modifiers)
    
    # Ensure fallback was used
    update_checker.check_https_fallback.assert_called_once()
    assert common == TS_FALLBACK

def test_s3_success_no_fallback(update_checker, mocker):
    """Test that fallback is NOT triggered when S3 succeeds."""
    # Mock _get_modifier_times to return intersecting sets
    mocker.patch.object(update_checker, "_get_modifier_times", return_value={TS_NEW})
    
    # Mock check_https_fallback
    mocker.patch.object(update_checker, "check_https_fallback", return_value=TS_FALLBACK)
    
    modifiers = [("R", "Mod1", "D"), ("R", "Mod2", "D")]
    common = update_checker.latest_common_minute_1h(modifiers)
    
    # Ensure fallback was NOT used
    update_checker.check_https_fallback.assert_not_called()
    assert common == TS_NEW
