"""Shared NEXRAD Level-II ingest helpers."""

__all__ = ["ingest_allowed_vcp_volume", "ingest_latest_allowed_vcp_scans"]


def ingest_allowed_vcp_volume(*args, **kwargs):
    from .main import ingest_allowed_vcp_volume as _impl

    return _impl(*args, **kwargs)


def ingest_latest_allowed_vcp_scans(*args, **kwargs):
    from .main import ingest_latest_allowed_vcp_scans as _impl

    return _impl(*args, **kwargs)
