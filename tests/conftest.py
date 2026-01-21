import sys
from pathlib import Path
import pytest
from unittest.mock import MagicMock

# Add src to pythonpath so we can import EdgeWARN
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

@pytest.fixture
def mock_io_manager():
    """Mock IOManager to capturing log outputs."""
    mock = MagicMock()
    # Configure common methods
    mock.write_info = MagicMock()
    mock.write_error = MagicMock()
    mock.write_warning = MagicMock()
    mock.write_debug = MagicMock()
    return mock

@pytest.fixture
def mock_fs(tmp_path):
    """
    Mock file system constants.
    Uses tmp_path to create a safe playground for file operations.
    """
    # Create the directory structure in tmp_path
    (tmp_path / "surface").mkdir()
    (tmp_path / "metar").mkdir()
    (tmp_path / "nws").mkdir()
    (tmp_path / "stormcell").mkdir()
    (tmp_path / "cell").mkdir()
    
    # We'll need to patch util.file within the tests usually, 
    # but we can provide the paths here for convenience
    return tmp_path

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Set mock environment variables for testing."""
    monkeypatch.setenv("EDGEWARN_ENV", "test")
