"""
Tests for IO utility module
"""

import pytest
import sys
from io import StringIO
from datetime import datetime, timezone
from unittest.mock import patch
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
        """Test that write puts timestamped message to queue"""
        from queue import Queue
        queue = Queue()
        writer = QueueWriter(queue)
        
        writer.write("Test message\n")
        
        assert not queue.empty()
        message = queue.get()
        assert "Test message" in message
        assert "T" in message  # ISO timestamp

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

    def test_get_args_default_values(self):
        """Test get_args with default values"""
        io = IOManager("[Test]")
        
        with patch.object(sys, 'argv', ['script']):
            args = io.get_args()
            
            assert args.lat_limits == [20, 55]
            assert args.lon_limits == [230, 300]
            assert args.base_dir is None

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
        ]):
            args = io.get_args()

            assert args.disable_ewmrs is True
            assert args.disable_nws is True
            assert args.disable_metar is True
            assert args.disable_goes is True

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
