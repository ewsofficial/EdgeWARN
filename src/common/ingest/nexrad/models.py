from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class VolumeKey:
    site: str
    volume_id: str


@dataclass(frozen=True)
class ChunkKey:
    site: str
    volume_id: str
    chunk_number: int
    chunk_type: str
    key: str


@dataclass(frozen=True)
class RadarStationVcp:
    site: str
    vcp: int | None
    raw_vcp: str | int | None
    rda_timestamp: str | None
    level_two_last_received_time: str | None
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VolumeProbe:
    site: str
    volume_id: str
    scan_name: str | None
    vcp: int | None
    dynamic_scan_type: str | None
    first_chunk_key: ChunkKey | None
    accepted: bool
    vcp_source: str


@dataclass(frozen=True)
class SweepInfo:
    index: int
    group_name: str
    fixed_angle: float
    waveform: str | None
    azimuth_count: int
    complete: bool
    supplemental: bool
    bucket: str


@dataclass(frozen=True)
class ParsedVolume:
    scan_name: str | None
    dynamic_scan_type: str | None
    sweeps: list[SweepInfo]
    datatree: Any = None
    source_bucket: str | None = None


@dataclass(frozen=True)
class NexradIngestResult:
    site: str
    volume_id: str
    vcp: int
    dynamic_scan_type: str | None
    low_path: Path | None
    high_path: Path | None
    manifest_path: Path | None
    chunks_downloaded: int
    complete: bool


@dataclass(frozen=True)
class NexradCoordinatorResult:
    site: str
    latest_scan_time: str | None
    vcp: int | None
    volume_id: str | None
    action: str
    chunks_downloaded: int = 0


class NexradCoordinatorRunResults(list[NexradCoordinatorResult]):
    def __init__(self, results=(), *, downloaded_sites=()):
        super().__init__(results)
        self.downloaded_sites = tuple(downloaded_sites)
