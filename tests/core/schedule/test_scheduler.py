import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from EdgeWARN.core.schedule.scheduler import MRMSUpdateChecker

# Sample Timestamps
TS_OLD = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TS_NEW = datetime(2023, 1, 1, 12, 2, 0, tzinfo=timezone.utc)
TS_NEWER = datetime(2023, 1, 1, 12, 4, 0, tzinfo=timezone.utc)

@pytest.fixture
def update_checker(mock_io_manager):
    """Fixture for initialized MRMSUpdateChecker."""
    # We patch the module-level io_manager to suppress output during tests
    with patch("EdgeWARN.core.schedule.scheduler.io_manager", mock_io_manager):
        yield MRMSUpdateChecker(verbose=True)

def test_has_update_no_remote_files(update_checker, mocker):
    """Test behavior when no remote files are found."""
    # Mock FileFinder
    mock_finder = MagicMock()
    mock_finder.lookup_files.return_value = [] # No files
    mocker.patch("EdgeWARN.core.schedule.scheduler.FileFinder", return_value=mock_finder)
    
    result = update_checker.has_update(("CONUS", "Reflectivity", "/tmp"))
    assert result is False

def test_has_update_no_local_files(update_checker, mocker, mock_fs):
    """Test that update is flagged if no local files exist."""
    # Remote has files
    mock_finder = MagicMock()
    mock_finder.lookup_files.return_value = [("path/file", TS_NEW)]
    mocker.patch("EdgeWARN.core.schedule.scheduler.FileFinder", return_value=mock_finder)
    
    # Local has no files (mock_fs temp dir is empty for this)
    result = update_checker.has_update(("CONUS", "Reflectivity", str(mock_fs / "empty")))
    assert result is True

def test_has_update_remote_newer(update_checker, mocker, mock_fs):
    """Test update detected when remote is newer than local."""
    # Remote has TS_NEW
    mock_finder = MagicMock()
    mock_finder.lookup_files.return_value = [("path/new", TS_NEW)]
    mocker.patch("EdgeWARN.core.schedule.scheduler.FileFinder", return_value=mock_finder)
    
    # Local has TS_OLD
    # We mock Path.glob to return a file with TS_OLD in name
    # But for robustness, let's create a real file in mock_fs
    out_dir = mock_fs / "mrms"
    out_dir.mkdir()
    (out_dir / "MRMS_Reflectivity_00.50_20230101-120000.gz").touch() # TS_OLD
    
    # Patch extract_timestamp to return TS_OLD for this file
    mocker.patch("EdgeWARN.core.schedule.scheduler.extract_timestamp", return_value=TS_OLD)
    
    result = update_checker.has_update(("CONUS", "Reflectivity", str(out_dir)))
    assert result is True

def test_has_update_remote_older(update_checker, mocker, mock_fs):
    """Test no update when local is current."""
    # Remote has TS_OLD
    mock_finder = MagicMock()
    mock_finder.lookup_files.return_value = [("path/old", TS_OLD)]
    mocker.patch("EdgeWARN.core.schedule.scheduler.FileFinder", return_value=mock_finder)
    
    # Local has TS_OLD
    out_dir = mock_fs / "mrms_current"
    out_dir.mkdir()
    (out_dir / "file.gz").touch()
    mocker.patch("EdgeWARN.core.schedule.scheduler.extract_timestamp", return_value=TS_OLD)
    
    result = update_checker.has_update(("CONUS", "Reflectivity", str(out_dir)))
    assert result is False

def test_latest_common_minute_intersection(update_checker, mocker):
    """Test finding common timestamp across modifiers."""
    # Mock _get_modifier_times to return sets
    # Mod 1 has [OLD, NEW]
    # Mod 2 has [NEW, NEWER]
    # Common is NEW
    
    def side_effect(mod, ref_dt, trace_id=None, last_processed=None):
        if mod[1] == "Mod1":
            return {TS_OLD, TS_NEW}
        else:
            return {TS_NEW, TS_NEWER}
            
    mocker.patch.object(update_checker, "_get_modifier_times", side_effect=side_effect)
    
    modifiers = [("R", "Mod1", "D"), ("R", "Mod2", "D")]
    common = update_checker.latest_common_minute_1h(modifiers)
    
    assert common == TS_NEW

def test_latest_common_minute_no_intersection(update_checker, mocker):
    """Test behavior when no common timestamps exist."""
    def side_effect(mod, ref_dt, trace_id=None, last_processed=None):
        if mod[1] == "Mod1":
            return {TS_OLD}
        else:
            return {TS_NEWER} # Disjoint
            
    mocker.patch.object(update_checker, "_get_modifier_times", side_effect=side_effect)
    
    modifiers = [("R", "Mod1", "D"), ("R", "Mod2", "D")]
    common = update_checker.latest_common_minute_1h(modifiers)
    
    assert common is None
