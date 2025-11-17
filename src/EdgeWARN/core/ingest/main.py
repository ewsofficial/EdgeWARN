from EdgeWARN.core.ingest.config import mrms_modifiers, check_modifiers, bucket
from EdgeWARN.core.ingest.download import FileFinder, FileDownloader
from EdgeWARN.core.ingest.parse import MRMSBucketParser
from util.io import IOManager
from datetime import datetime, timezone, timedelta

io_manager = IOManager("[Ingest]")

def download_modifier(region, modifier, outdir, dt, max_time, max_entries):

    # Enforce minute-precision dt
    dt = dt.replace(second=0, microsecond=0)

    finder = FileFinder(dt, bucket, max_time, max_entries, io_manager)
    downloader = FileDownloader(dt, bucket, io_manager)
    parser = MRMSBucketParser(dt)

    try:
        bucket_path = parser.parse_bucket_path(region, modifier)
        file_list = finder.lookup_files(bucket_path)

        if not file_list:
            io_manager.write_warning(f"No files found for {bucket_path} at {dt}")
            return
        
        # Download most recent file that matches the target minute
        downloaded = downloader.download_latest(file_list, outdir)
        if downloaded:
            downloader.decompress_file(downloaded)
        else:
            io_manager.write_error(f"Failed to download {bucket_path} file")
    
    except Exception as e:
        io_manager.write_error(f"Failed to process {bucket_path} - {e}")

if __name__ == "__main__":
    # Test downloading MRMS CompRef file for latest time
    dt = datetime.now(timezone.utc)
    download_modifier("CONUS", "MergedReflectivityQCComposite_00.50", "C:\\EdgeWARN_input\\", dt, 3600, 10)