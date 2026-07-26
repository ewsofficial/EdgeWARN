import aioboto3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from botocore import UNSIGNED
from botocore.client import Config
from util.io import IOManager
import util.file as fs
from common.ingest.aws_async_compat import ensure_aiobotocore_endpoint_compat
from common.ingest.synoptic.config import (
    RAP_BUCKET,
    RAP_DIR_PATTERN,
    RAP_FILE_PATTERN,
    get_rap_max_age_minutes,
)
from common.ingest.synoptic.s3_sync import SynopticFileDownloader
from common.ingest.synoptic.s3_async import AsyncSynopticFileDownloader

io_manager = IOManager("[DataIngestion]")


@dataclass(frozen=True)
class SynopticAttempt:
    analysis_time: datetime
    s3_key: str
    failure: str


class SynopticUnavailableError(RuntimeError):
    """Raised after every acceptable synoptic analysis has been exhausted."""

    def __init__(
        self,
        dataset_name: str,
        requested_time: datetime,
        max_age_minutes: int,
        attempts: list[SynopticAttempt],
    ):
        self.dataset_name = dataset_name
        self.requested_time = requested_time
        self.max_age_minutes = max_age_minutes
        self.attempts = tuple(attempts)
        checked = ", ".join(
            f"{attempt.s3_key}={attempt.failure}" for attempt in attempts
        ) or "none"
        super().__init__(
            f"{dataset_name} unavailable within {max_age_minutes}-minute analysis-age "
            f"limit for {requested_time.isoformat()}; checked: {checked}"
        )


def _log_synoptic_not_found(bucket, s3_key):
    io_manager.write_warning(f"Synoptic file not found on S3 (404): s3://{bucket}/{s3_key}")


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _eligible_analysis_times(dt: datetime, max_age_minutes: int):
    requested_time = _as_utc(dt)
    candidate = requested_time.replace(minute=0, second=0, microsecond=0)
    while (requested_time - candidate).total_seconds() / 60 <= max_age_minutes:
        yield candidate
        candidate -= timedelta(hours=1)


def _is_valid_local_file(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _failure_category(exc: Exception) -> str:
    text = str(exc).lower()
    if any(
        token in text
        for token in ("accessdenied", "forbidden", "credential", "signature")
    ):
        return "authentication"
    return "transport"


def _log_selected(dataset_name, requested_time, analysis_time, path, source):
    age_minutes = int((requested_time - analysis_time).total_seconds() / 60)
    io_manager.write_info(
        f"{dataset_name} selected analysis={analysis_time.isoformat()} "
        f"age_minutes={age_minutes} source={source} path={path}"
    )


def _build_synoptic_s3_params(dt, file_pattern, dir_pattern, out_dir):
    """
    Build the S3 key and local file path for a synoptic download.

    Args:
        dt: Target datetime.
        file_pattern (str): ``str.format``-compatible pattern for the filename
            (receives ``hour=<int>``).
        dir_pattern (str): ``str.format``-compatible pattern for the S3 directory
            (receives ``date=<str>``).
        out_dir (Path): Local output directory.

    Returns:
        tuple[str, Path]: ``(s3_key, local_path)``
    """
    date_str = dt.strftime("%Y%m%d")
    hour = dt.hour

    dir_name = dir_pattern.format(date=date_str)
    file_name = file_pattern.format(hour=hour)
    s3_key = f"{dir_name}/{file_name}"

    local_filename = f"RAP.{date_str}-{hour:02d}z.awp130pgrbf00.grib2"
    local_path = out_dir / local_filename

    return s3_key, local_path


async def download_synoptic_async(dt, bucket, file_pattern, dir_pattern, out_dir):
    """
    Attempt to download a synoptic file asynchronously.
    """
    s3_key, local_path = _build_synoptic_s3_params(dt, file_pattern, dir_pattern, out_dir)

    ensure_aiobotocore_endpoint_compat()
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as s3:
        downloader = AsyncSynopticFileDownloader(bucket, io_manager, s3_client=s3)
        return await downloader.async_download_file(s3_key, local_path)

def download_synoptic_sync(dt, bucket, file_pattern, dir_pattern, out_dir):
    """
    Attempt to download a synoptic file synchronously.
    """
    s3_key, local_path = _build_synoptic_s3_params(dt, file_pattern, dir_pattern, out_dir)

    downloader = SynopticFileDownloader(bucket, io_manager)
    return downloader.download_file(s3_key, local_path)


async def download_synoptic(
    dt,
    bucket,
    file_pattern,
    dir_pattern,
    out_dir,
    dataset_name="Synoptic",
    *,
    max_age_minutes=60,
):
    """
    Select the newest acceptable local or remote synoptic analysis.

    Definitive S3 404 responses advance to the next analysis hour. Other async
    failures receive one synchronous attempt for the same candidate.
    """
    requested_time = _as_utc(dt)
    attempts = []
    for current_dt in _eligible_analysis_times(requested_time, max_age_minutes):
        s3_key, local_path = _build_synoptic_s3_params(
            current_dt, file_pattern, dir_pattern, out_dir
        )
        age_minutes = int((requested_time - current_dt).total_seconds() / 60)

        if _is_valid_local_file(local_path):
            _log_selected(
                dataset_name, requested_time, current_dt, local_path, "local"
            )
            return local_path
        if local_path.exists():
            io_manager.write_warning(
                f"Ignoring invalid local {dataset_name} file: {local_path}"
            )

        if current_dt != requested_time.replace(minute=0, second=0, microsecond=0):
            io_manager.write_info(
                f"Attempting {dataset_name} fallback analysis: {current_dt} "
                f"(age_minutes={age_minutes}, s3://{bucket}/{s3_key})"
            )
        else:
            io_manager.write_info(
                f"Attempting {dataset_name} download: s3://{bucket}/{s3_key}"
            )

        async_failure = None
        try:
            result = await download_synoptic_async(
                current_dt, bucket, file_pattern, dir_pattern, out_dir
            )
            if result and _is_valid_local_file(Path(result)):
                _log_selected(
                    dataset_name, requested_time, current_dt, result, "s3_async"
                )
                return result
            async_failure = "local_invalid" if result else "transport"
        except FileNotFoundError:
            _log_synoptic_not_found(bucket, s3_key)
            attempts.append(SynopticAttempt(current_dt, s3_key, "not_found"))
            continue
        except Exception as exc:
            async_failure = _failure_category(exc)
            io_manager.write_warning(
                f"Async {dataset_name} download for {current_dt} failed: {exc}"
            )

        try:
            result = download_synoptic_sync(
                current_dt, bucket, file_pattern, dir_pattern, out_dir
            )
            if result and _is_valid_local_file(Path(result)):
                _log_selected(
                    dataset_name, requested_time, current_dt, result, "s3_sync"
                )
                return result
            failure = "local_invalid" if result else async_failure or "transport"
            attempts.append(SynopticAttempt(current_dt, s3_key, failure))
        except FileNotFoundError:
            _log_synoptic_not_found(bucket, s3_key)
            attempts.append(SynopticAttempt(current_dt, s3_key, "not_found"))
        except Exception as exc:
            failure = _failure_category(exc)
            attempts.append(SynopticAttempt(current_dt, s3_key, failure))
            io_manager.write_error(
                f"Sync {dataset_name} download for {current_dt} failed: {exc}"
            )

    error = SynopticUnavailableError(
        dataset_name, requested_time, max_age_minutes, attempts
    )
    io_manager.write_error(str(error))
    raise error


async def download_rap(dt):
    """
    Wrapper for RAP dataset download.
    """
    return await download_synoptic(
        dt,
        RAP_BUCKET,
        RAP_FILE_PATTERN,
        RAP_DIR_PATTERN,
        fs.RAP_DIR,
        dataset_name="RAP",
        max_age_minutes=get_rap_max_age_minutes(),
    )
