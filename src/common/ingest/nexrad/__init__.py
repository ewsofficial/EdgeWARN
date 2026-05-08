"""Shared NEXRAD Level-II ingest helpers."""

__all__ = [
    "NexradIngestService",
    "NexradScanCoordinator",
    "NexradChunkStore",
    "RadarStationCatalog",
    "VolumeVcpProber",
    "ingest_allowed_vcp_volume",
    "ingest_latest_allowed_vcp_scans",
    "ingest_latest_station_scans_async",
    "poll_latest_station_scans_forever_async",
]


def ingest_allowed_vcp_volume(*args, **kwargs):
    from .main import ingest_allowed_vcp_volume as _impl

    return _impl(*args, **kwargs)


def ingest_latest_allowed_vcp_scans(*args, **kwargs):
    from .main import ingest_latest_allowed_vcp_scans as _impl

    return _impl(*args, **kwargs)


async def ingest_latest_station_scans_async(*args, **kwargs):
    from .coordinator import ingest_latest_station_scans_async as _impl

    return await _impl(*args, **kwargs)


async def poll_latest_station_scans_forever_async(*args, **kwargs):
    from .coordinator import poll_latest_station_scans_forever_async as _impl

    return await _impl(*args, **kwargs)


def __getattr__(name):
    if name == "NexradIngestService":
        from .main import NexradIngestService as _impl

        return _impl
    if name == "NexradChunkStore":
        from .s3_chunks import NexradChunkStore as _impl

        return _impl
    if name == "NexradScanCoordinator":
        from .coordinator import NexradScanCoordinator as _impl

        return _impl
    if name == "VolumeVcpProber":
        from .vcp_probe import VolumeVcpProber as _impl

        return _impl
    if name == "RadarStationCatalog":
        from .weather_api import RadarStationCatalog as _impl

        return _impl
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
