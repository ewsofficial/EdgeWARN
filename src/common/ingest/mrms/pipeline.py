"""Shared orchestration helpers for MRMS and GOES ingestion."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Iterable, Sequence

from common.ingest.mrms.config import normalize_goes_modifier


CleanupFunc = Callable[..., Awaitable[None]]


def get_output_dirs(
    mrms_modifiers: Sequence[tuple],
    *,
    goes_modifiers: Sequence[tuple] | None = None,
    include_modifiers: Iterable[str | None] | None = None,
    exclude_modifiers: Iterable[str | None] | None = None,
    include_goes: bool = True,
) -> list:
    include_set = set(include_modifiers) if include_modifiers is not None else None
    exclude_set = set(exclude_modifiers) if exclude_modifiers is not None else None

    folders = []
    for _, modifier, outdir in mrms_modifiers:
        if include_set is not None and modifier not in include_set:
            continue
        if exclude_set is not None and modifier in exclude_set:
            continue
        folders.append(outdir)

    if include_goes and goes_modifiers:
        folders.extend(normalize_goes_modifier(spec).outdir for spec in goes_modifiers)

    return folders


async def run_ingestion_pipeline(
    *,
    io_manager,
    async_downloads: Sequence[Awaitable],
    cleanup_dirs: Sequence = (),
    cleanup_async: CleanupFunc | None = None,
    cleanup_message: str | None = None,
    cleanup_kwargs: dict | None = None,
) -> None:
    tasks = list(async_downloads)

    if cleanup_dirs and cleanup_async is not None:
        if cleanup_message:
            io_manager.write_debug(cleanup_message)

        cleanup_kwargs = cleanup_kwargs or {}
        tasks.extend(cleanup_async(folder, **cleanup_kwargs) for folder in cleanup_dirs)

    await asyncio.gather(*tasks)


def run_with_async_fallback(
    *,
    io_manager,
    async_runner: Callable[[], Awaitable[None]],
    sync_fallback: Callable[[], None],
    failure_prefix: str = "Async downloads failed",
    fallback_message: str = "Falling back to synchronous downloads...",
) -> None:
    try:
        asyncio.run(async_runner())
    except Exception as exc:
        io_manager.write_error(f"{failure_prefix}: {exc}")
        io_manager.write_info(fallback_message)
        sync_fallback()
