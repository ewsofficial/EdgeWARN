"""Shared NEXRAD Level-II ingest helpers."""

from .main import NexradIngestService
from .s3_chunks import NexradChunkStore
from .vcp_probe import VolumeVcpProber
from .weather_api import RadarStationCatalog

__all__ = [
    "NexradIngestService",
    "NexradChunkStore",
    "RadarStationCatalog",
    "VolumeVcpProber",
    "ingest_allowed_vcp_volume",
    "ingest_latest_allowed_vcp_scans",
]


def ingest_allowed_vcp_volume(*args, **kwargs):
    from .main import ingest_allowed_vcp_volume as _impl

    return _impl(*args, **kwargs)


def ingest_latest_allowed_vcp_scans(*args, **kwargs):
    from .main import ingest_latest_allowed_vcp_scans as _impl

    return _impl(*args, **kwargs)
