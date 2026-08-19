"""
Tests for IO utility module
"""

import pytest
import shutil
import sys
import yaml
from io import StringIO
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from common.config import loader as config_loader
from util.io import TimestampedOutput, QueueWriter, IOManager


class TestTimestampedOutput:
    """Tests for TimestampedOutput class"""

    def test_write_adds_timestamp(self):
        """Test that write adds timestamp to messages"""
        stream = StringIO()
        ts_output = TimestampedOutput(stream)
        
        ts_output.write("Test message\n")
        
        output = stream.getvalue()
        assert "Test message" in output
        # Should have ISO timestamp format
        assert "T" in output  # ISO format contains T

    def test_write_passthroughs_carriage_return_progress(self):
        stream = StringIO()
        ts_output = TimestampedOutput(stream)

        ts_output.write("\rProgress 10%")

        assert stream.getvalue() == "\rProgress 10%"

    def test_write_skips_empty_lines(self):
        """Test that empty lines don't get timestamped"""
        stream = StringIO()
        ts_output = TimestampedOutput(stream)
        
        ts_output.write("\n")
        ts_output.write("")
        ts_output.write("   ")
        
        output = stream.getvalue()
        # Should not have timestamps for whitespace-only lines
        lines = output.strip().split('\n')
        # All lines should be empty (no timestamp added)
        assert all(not line or line.isspace() for line in lines)

    def test_flush(self):
        """Test flush method"""
        stream = StringIO()
        ts_output = TimestampedOutput(stream)
        
        # Should not raise an error
        ts_output.flush()


class TestQueueWriter:
    """Tests for QueueWriter class"""

    def test_write_puts_to_queue(self):
        """Test that write puts raw message text to queue"""
        from queue import Queue
        queue = Queue()
        writer = QueueWriter(queue)
        
        writer.write("Test message\n")
        
        assert not queue.empty()
        message = queue.get()
        assert message == "Test message"

    def test_write_skips_empty_lines(self):
        """Test that empty lines are not queued"""
        from queue import Queue
        queue = Queue()
        writer = QueueWriter(queue)
        
        writer.write("\n")
        writer.write("")
        writer.write("   ")
        
        # Queue should be empty
        assert queue.empty()

    def test_flush_does_nothing(self):
        """Test that flush does nothing (no-op)"""
        from queue import Queue
        queue = Queue()
        writer = QueueWriter(queue)
        
        # Should not raise an error
        writer.flush()


class TestIOManager:
    """Tests for IOManager class"""

    def test_initialization(self):
        """Test IOManager initialization"""
        io = IOManager("[Test]")
        assert io.header == "[Test]"

    def test_write_info(self, capsys):
        """Test write_info method"""
        io = IOManager("[Test]")
        io.write_info("Information message")
        
        captured = capsys.readouterr()
        assert "[Test] INFO: Information message" in captured.out

    def test_write_debug(self, capsys):
        """Test write_debug method"""
        io = IOManager("[Test]")
        io.write_debug("Debug message")
        
        captured = capsys.readouterr()
        assert "[Test] DEBUG: Debug message" in captured.out

    def test_write_warning(self, capsys):
        """Test write_warning method"""
        io = IOManager("[Test]")
        io.write_warning("Warning message")
        
        captured = capsys.readouterr()
        assert "[Test] WARN: Warning message" in captured.out

    def test_write_error(self, capsys):
        """Test write_error method"""
        io = IOManager("[Test]")
        io.write_error("Error message")
        
        captured = capsys.readouterr()
        assert "[Test] ERROR: Error message" in captured.out

    def test_get_args_default_values(self, monkeypatch):
        """Test get_args with default values"""
        io = IOManager("[Test]")
        monkeypatch.delenv("EDGEWARN_BASE_DIR", raising=False)
        monkeypatch.delenv("BASE_DIR", raising=False)
        
        with patch.object(sys, 'argv', ['script']):
            args = io.get_args()
            
            assert args.lat_limits == [20, 55]
            assert args.lon_limits == [230, 300]
            assert args.base_dir == str(Path.home() / "EdgeWARN_input")

    def test_get_args_custom_lat_lon(self):
        """Test get_args with custom lat/lon limits"""
        io = IOManager("[Test]")
        
        with patch.object(sys, 'argv', [
            'script',
            '--lat_limits', '30', '40',
            '--lon_limits', '250', '270'
        ]):
            args = io.get_args()
            
            assert args.lat_limits == [30, 40]
            assert args.lon_limits == [250, 270]

    def test_get_args_longitude_conversion(self):
        """Test that longitude is converted to 0-360 range"""
        io = IOManager("[Test]")
        
        with patch.object(sys, 'argv', [
            'script',
            '--lon_limits', '-100', '-80'
        ]):
            args = io.get_args()
            
            # -100 should become 260, -80 should become 280
            assert args.lon_limits == [260, 280]

    def test_get_args_base_dir(self):
        """Test --base_dir argument"""
        io = IOManager("[Test]")
        
        with patch.object(sys, 'argv', ['script', '--base_dir', '/custom/path']):
            args = io.get_args()
            
            assert args.base_dir == '/custom/path'

    def test_get_args_base_dir_hyphen_alias(self):
        """Test --base-dir argument alias"""
        io = IOManager("[Test]")

        with patch.object(sys, 'argv', ['script', '--base-dir', '/custom/path']):
            args = io.get_args()

            assert args.base_dir == '/custom/path'

    def test_get_args_disable_component_flags(self):
        """Test disable flags for optional runtime components."""
        io = IOManager("[Test]")

        with patch.object(sys, 'argv', [
            'script',
            '--disable-ewmrs',
            '--disable-nws',
            '--disable-metar',
            '--disable-goes',
            '--disable-nexrad',
        ]):
            args = io.get_args()

            assert args.disable_ewmrs is True
            assert args.disable_nws is True
            assert args.disable_metar is True
            assert args.disable_goes is True
            assert args.disable_nexrad is True

    def test_get_args_invalid_lat_limits_count(self):
        """Test validation of lat_limits count"""
        io = IOManager("[Test]")
        
        with patch.object(sys, 'argv', ['script', '--lat_limits', '30']):
            with pytest.raises(SystemExit):
                io.get_args()

    def test_get_args_zero_limits(self):
        """Test validation rejects zero limits"""
        io = IOManager("[Test]")
        
        with patch.object(sys, 'argv', ['script', '--lat_limits', '0', '0']):
            with pytest.raises(SystemExit):
                io.get_args()

    def test_get_historical_args_default_values(self, monkeypatch):
        """Test historical args with default values."""
        io = IOManager("[Test]")
        monkeypatch.delenv("EDGEWARN_BASE_DIR", raising=False)
        monkeypatch.delenv("BASE_DIR", raising=False)

        with patch.object(sys, 'argv', ['script', '--start', '2024-01-01T00:00:00', '--end', '2024-01-01T01:00:00']):
            args = io.get_historical_args()

            assert args.start == '2024-01-01T00:00:00'
            assert args.end == '2024-01-01T01:00:00'
            assert args.lat == [20, 55]
            assert args.lon == [-130, -60]
            assert args.base_dir == str(Path.home() / "EdgeWARN_input")

    def test_get_historical_args_common_flags(self):
        """Test historical args reuse shared processing flags."""
        io = IOManager("[Test]")

        with patch.object(sys, 'argv', [
            'script',
            '--start', '2024-01-01T00:00:00',
            '--end', '2024-01-01T01:00:00',
            '--base-dir', '/custom/path',
            '--disable-ctam',
            '--disable-tracking',
            '--disable-polygon-expansion',
            '--refl-threshold', '40',
            '--min-seed-percentage', '0.01',
            '--drop-offset', '12',
        ]):
            args = io.get_historical_args()

            assert args.base_dir == '/custom/path'
            assert args.disable_ctam is True
            assert args.disable_tracking is True
            assert args.disable_polygon_expansion is True
            assert args.refl_threshold == 40.0
            assert args.min_seed_percentage == 0.01
            assert args.drop_offset == 12.0


@pytest.fixture
def alternate_config_dir(tmp_path):
    """A full copy of ``config/`` with distinguishable values, outside the repo."""
    destination = tmp_path / "alt_config"
    shutil.copytree(config_loader.config_root(), destination)

    runtime_path = destination / "runtime.yaml"
    document = yaml.safe_load(runtime_path.read_text(encoding="utf-8"))
    document["run"]["lat_limits"] = [21, 54]
    runtime_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return destination


class TestConfigDirPropagation:
    """``--config-dir`` must reach children that are spawned without argv."""

    def test_get_args_exports_resolved_config_root(self, alternate_config_dir):
        io = IOManager("[Test]")

        with patch.object(sys, 'argv', ['script', '--config-dir', str(alternate_config_dir)]):
            args = io.get_args()

        assert args.lat_limits == [21, 54]
        # A child resolving with no CLI argument now lands on the same root.
        assert config_loader.config_root() == alternate_config_dir

    def test_get_historical_args_exports_resolved_config_root(self, alternate_config_dir):
        io = IOManager("[Test]")

        with patch.object(sys, 'argv', [
            'script',
            '--start', '2024-01-01T00:00:00',
            '--end', '2024-01-01T01:00:00',
            '--config-dir', str(alternate_config_dir),
        ]):
            io.get_historical_args()

        assert config_loader.config_root() == alternate_config_dir

    def test_config_dir_without_runtime_yaml_fails_at_parse_time(self, tmp_path):
        io = IOManager("[Test]")
        empty = tmp_path / "empty"
        empty.mkdir()

        with patch.object(sys, 'argv', ['script', '--config-dir', str(empty)]):
            with pytest.raises(config_loader.ConfigError):
                io.get_args()
