from dataclasses import dataclass

from common.ingest.nexrad.models import RadarStationVcp


@dataclass(frozen=True)
class PendingVolume:
    site: str
    volume_id: str
    station: RadarStationVcp
    latest_scan_time: str | None


@dataclass(frozen=True)
class VolumeDiscoveryResult:
    site: str
    station: RadarStationVcp
    volume_id: str | None
    chunks: tuple
    latest_scan_time: str | None
