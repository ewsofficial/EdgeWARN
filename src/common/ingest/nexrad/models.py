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


@dataclass
class SweepRecord:
    index: int
    group_name: str
    fixed_angle: float
    waveform: str | None
    timestamp: str | None
    azimuth_count: int
    elevation_number: int | None = None


@dataclass
class ElevationGroup:
    elevation_id: str
    canonical_angle_deg: float
    members: list[SweepRecord] = field(default_factory=list)
    waveforms_present: set[str] = field(default_factory=set)
    first_sweep_index: int = 0
    last_sweep_index: int = 0
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    supplemental: bool = False
    complete: bool = False


@dataclass
class ElevationArtifact:
    site: str
    volume_id: str
    volume_timestamp: str | None
    scan_timestamp: str | None
    elevation: str
    elevation_timestamp: str | None
    first_sweep_index: int
    last_sweep_index: int
    first_sweep_timestamp: str | None
    last_sweep_timestamp: str | None
    member_group_names: list[str]
    member_sweeps: list[dict[str, Any]]
    waveforms_present: set[str]
    supplemental: bool
    netcdf_path: str | None = None
    ar2v_path: str | None = None
    download_started_at: str | None = None
    file_written_at: str | None = None
    parse_finished_at: str | None = None


@dataclass
class ScanStreamState:
    index: int
    volume_id: str
    scan_timestamp: str | None
    file_path: str
    bytes_written: int = 0
    seen_elevation_keys: set[str] = field(default_factory=set)
    saved_artifacts: list[ElevationArtifact] = field(default_factory=list)
    parse_errors: list[str] = field(default_factory=list)
    finalized: bool = False


@dataclass
class WorkerParseResult:
    visible_sweeps: int
    saved_sweep_count: int
    saved_elevations: list[ElevationArtifact]
    parse_error: str | None
    child_rss_kb: float | None = None
    buffer_trimmed: bool = False
    runtime_size: int | None = None


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
    volume_path: Path | None
    scan_timestamp: str | None
    low_path: Path | None
    high_path: Path | None
    manifest_path: Path | None
    chunks_downloaded: int
    complete: bool


@dataclass(frozen=True)
class NexradCompletionRecord:
    site: str
    volume_id: str
    scan_timestamp: str | None
    volume_path: Path | None
    manifest_path: Path | None


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


@dataclass
class RawSweepRange:
    index: int
    group_name: str
    elevation_number: int
    fixed_angle: float
    first_timestamp: str | None
    last_timestamp: str | None
    radial_count: int = 0
    waveform: str | None = None
    record_ranges: list[tuple[int, int]] = field(default_factory=list)
    complete: bool = False


@dataclass
class RawVolumeBuffer:
    volume_header: bytes
    site: str
    record_buffer: bytes
    metadata_ranges: list[tuple[int, int]] = field(default_factory=list)
    sweeps: list[RawSweepRange] = field(default_factory=list)
    trailing_bytes: bytes = b""
    compression_record_count: int = 0
