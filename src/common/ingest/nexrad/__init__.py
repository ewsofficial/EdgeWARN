"""Shared NEXRAD Level-II ingest helpers."""

__all__ = [
    "NexradIngestService",
    "NexradScanCoordinator",
    "NexradRealtimeIngestionPipeline",
    "NexradChunkStore",
    "RadarStationCatalog",
    "VolumeVcpProber",
    "ingest_allowed_vcp_volume",
    "ingest_latest_allowed_vcp_scans",
    "run_realtime_ingestion_pipeline",
    "run_realtime_ingestion_pipeline_async",
]


def ingest_allowed_vcp_volume(*args, **kwargs):
    from .main import ingest_allowed_vcp_volume as _impl

    return _impl(*args, **kwargs)


def ingest_latest_allowed_vcp_scans(*args, **kwargs):
    from .main import ingest_latest_allowed_vcp_scans as _impl

    return _impl(*args, **kwargs)


async def run_realtime_ingestion_pipeline_async(*args, **kwargs):
    from .pipeline import run_realtime_ingestion_pipeline_async as _impl

    return await _impl(*args, **kwargs)


def run_realtime_ingestion_pipeline(*args, **kwargs):
    from .pipeline import run_realtime_ingestion_pipeline as _impl

    return _impl(*args, **kwargs)


def __getattr__(name):
    if name == "NexradIngestService":
        from .service import NexradIngestService as _impl

        return _impl
    if name == "NexradChunkStore":
        from .s3_chunks import NexradChunkStore as _impl

        return _impl
    if name == "NexradScanCoordinator":
        from .coordinator import NexradScanCoordinator as _impl

        return _impl
    if name == "NexradRealtimeIngestionPipeline":
        from .pipeline import NexradRealtimeIngestionPipeline as _impl

        return _impl
    if name == "VolumeVcpProber":
        from .vcp_probe import VolumeVcpProber as _impl

        return _impl
    if name == "RadarStationCatalog":
        from .weather_api import RadarStationCatalog as _impl

        return _impl
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
