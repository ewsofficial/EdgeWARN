"""
Tests for Synoptic ingest main module
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from EdgeWARN.core.ingest.synoptic.main import download_rap


class TestDownloadRap:
    """Tests for download_rap function"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    def test_download_with_sync_context(self, mock_io):
        """Test download when no event loop exists"""
        with patch('EdgeWARN.core.ingest.synoptic.main.asyncio.get_running_loop', side_effect=RuntimeError):
            with patch('EdgeWARN.core.ingest.synoptic.main.asyncio.run') as mock_run:
                mock_run.return_value = "success"
                
                result = download_rap(datetime(2023, 10, 15, 14, 30))
                
                # Should use asyncio.run
                mock_run.assert_called_once()
                assert result == "success"

    def test_download_with_async_context(self, mock_io):
        """Test download when event loop exists"""
        mock_loop = MagicMock()
        mock_task = MagicMock()
        mock_loop.create_task.return_value = mock_task
        
        with patch('EdgeWARN.core.ingest.synoptic.main.asyncio.get_running_loop', return_value=mock_loop):
            result = download_rap(datetime(2023, 10, 15, 14, 30))
            
            # Should use loop.create_task
            mock_loop.create_task.assert_called_once()
            assert result == mock_task

    def test_download_cleans_old_files(self, mock_io):
        """Test that old files are cleaned"""
        with patch('EdgeWARN.core.ingest.synoptic.main.fs.clean_old_files') as mock_clean:
            with patch('EdgeWARN.core.ingest.synoptic.main.asyncio.run'):
                download_rap(datetime(2023, 10, 15, 14, 30))
                
                # Should call clean_old_files
                mock_clean.assert_called_once()

    def test_download_with_custom_datetime(self, mock_io):
        """Test download with specific datetime"""
        test_dt = datetime(2023, 10, 15, 14, 30, 0)
        
        with patch('EdgeWARN.core.ingest.synoptic.main._download_rap') as mock_download:
            mock_download.return_value = "test_file.nc"
            
            with patch('EdgeWARN.core.ingest.synoptic.main.asyncio.run') as mock_run:
                mock_run.return_value = "test_file.nc"
                result = download_rap(test_dt)
                
                # Should pass the datetime to _download_rap
                mock_run.assert_called_once()
                assert result == "test_file.nc"

    def test_download_handles_errors(self, mock_io):
        """Test error handling - function should raise exception on failure"""
        with patch('EdgeWARN.core.ingest.synoptic.main.asyncio.run') as mock_run:
            mock_run.side_effect = Exception("Download failed")
            
            # Should raise exception
            with pytest.raises(Exception, match="Download failed"):
                download_rap(datetime(2023, 10, 15, 14, 30))
