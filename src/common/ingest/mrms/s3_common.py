"""
Shared constants and utilities for MRMS S3 download modules.
"""
from common.ingest.mrms.timestamp_utils import round_to_nearest_even_minute


def select_target_file(dt, file_list, io_manager, context: str | None = None):
    """
    Select the file from *file_list* that best matches *dt*.

    Uses :func:`round_to_nearest_even_minute` for matching and emits a debug
    log when a non-exact (rounded) match is chosen.  Falls back to the most
    recent file in the list when no match is found.

    Args:
        dt: Target datetime (timezone-aware).
        file_list: Iterable of ``(s3_path, datetime_obj)`` tuples, sorted
            latest-first (as returned by
            :meth:`FileFinder.lookup_files` /
            :meth:`AsyncFileFinder.async_lookup_files`).
        io_manager: ``IOManager`` instance used for logging.
        context: Optional human-readable label included in log messages
            (e.g. the output directory) to aid debugging.

    Returns:
        str: S3 path of the best-matching file.
    """
    target_rounded = round_to_nearest_even_minute(dt)
    target_key = (
        target_rounded.year,
        target_rounded.month,
        target_rounded.day,
        target_rounded.hour,
        target_rounded.minute,
    )

    for s3_path, ts in file_list:
        ts_rounded = round_to_nearest_even_minute(ts)
        ts_key = (
            ts_rounded.year,
            ts_rounded.month,
            ts_rounded.day,
            ts_rounded.hour,
            ts_rounded.minute,
        )

        if ts_key == target_key:
            # Log if not an exact match (rounding was applied)
            if ts.minute != target_rounded.minute or ts.hour != target_rounded.hour:
                suffix = f" for {context}" if context else ""
                io_manager.write_debug(
                    f"Rounded match: {ts.strftime('%H:%M:%S')} → "
                    f"{target_rounded.strftime('%H:%M')}{suffix}"
                )
            return s3_path

    return file_list[0][0]
