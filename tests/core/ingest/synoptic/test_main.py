"""
Tests for Synoptic ingest main module
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, call, patch, AsyncMock
from EdgeWARN.ingest.synoptic.main import download_rap, download_rap_async


class TestDownloadRapAsync:
    """Tests for download_rap_async function"""

    @pytest.mark.asyncio
    async def test_async_download_cleans_old_files(self):
        """Test that async download cleans old files"""
        with patch('EdgeWARN.ingest.synoptic.main.fs.async_clean_old_files') as mock_clean:
            mock_clean.return_value = None  # Not a coroutine, just returns None
            
            with patch('EdgeWARN.ingest.synoptic.main._download_rap') as mock_download:
                mock_download.return_value = "test_file.grib2"
                
                result = await download_rap_async(datetime(2023, 10, 15, 14, 30))
                
                # Should pre-clean to leave room for the new file, then enforce the final 3-file limit
                mock_clean.assert_has_awaits([
                    call(mock_clean.call_args_list[0].args[0], max_age_minutes=90, max_files=2),
                    call(mock_clean.call_args_list[1].args[0], max_age_minutes=90, max_files=3),
                ])
                assert result == "test_file.grib2"

    @pytest.mark.asyncio
    async def test_async_download_with_custom_datetime(self):
        """Test async download with specific datetime"""
        test_dt = datetime(2023, 10, 15, 14, 30, 0)
        
        with patch('EdgeWARN.ingest.synoptic.main.fs.async_clean_old_files'):
            with patch('EdgeWARN.ingest.synoptic.main._download_rap') as mock_download:
                mock_download.return_value = "test_file.grib2"
                
                result = await download_rap_async(test_dt)
                
                # Should pass the datetime to _download_rap
                mock_download.assert_called_once_with(test_dt)
                assert result == "test_file.grib2"


class TestDownloadRap:
    """Tests for download_rap function"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    def test_download_with_sync_context(self, mock_io):
        """Test download when no event loop exists"""
        with patch('EdgeWARN.ingest.synoptic.main.asyncio.get_running_loop', side_effect=RuntimeError):
            with patch('EdgeWARN.ingest.synoptic.main.fs.clean_old_files'):
                with patch('EdgeWARN.ingest.synoptic.main.asyncio.run') as mock_run:
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
        
        with patch('EdgeWARN.ingest.synoptic.main.asyncio.get_running_loop', return_value=mock_loop):
            result = download_rap(datetime(2023, 10, 15, 14, 30))
            
            # Should use loop.create_task
            mock_loop.create_task.assert_called_once()
            assert result == mock_task

    def test_download_cleans_old_files(self, mock_io):
        """Test that old files are cleaned"""
        with patch('EdgeWARN.ingest.synoptic.main.fs.clean_old_files') as mock_clean:
            with patch('EdgeWARN.ingest.synoptic.main.asyncio.run'):
                download_rap(datetime(2023, 10, 15, 14, 30))
                
                # Should pre-clean to leave room for the new file, then enforce the final 3-file limit
                mock_clean.assert_has_calls([
                    call(mock_clean.call_args_list[0].args[0], max_age_minutes=90, max_files=2),
                    call(mock_clean.call_args_list[1].args[0], max_age_minutes=90, max_files=3),
                ])

    def test_download_with_custom_datetime(self, mock_io):
        """Test download with specific datetime"""
        test_dt = datetime(2023, 10, 15, 14, 30, 0)
        
        with patch('EdgeWARN.ingest.synoptic.main._download_rap') as mock_download:
            mock_download.return_value = "test_file.nc"
            
            with patch('EdgeWARN.ingest.synoptic.main.asyncio.run') as mock_run:
                mock_run.return_value = "test_file.nc"
                result = download_rap(test_dt)
                
                # Should pass the datetime to _download_rap
                mock_run.assert_called_once()
                assert result == "test_file.nc"

    def test_download_handles_errors(self, mock_io):
        """Test error handling - function should raise exception on failure"""
        with patch('EdgeWARN.ingest.synoptic.main.asyncio.run') as mock_run:
            mock_run.side_effect = Exception("Download failed")
            
            # Should raise exception
            with pytest.raises(Exception, match="Download failed"):
                download_rap(datetime(2023, 10, 15, 14, 30))
