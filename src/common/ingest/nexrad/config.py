CHUNKS_BUCKET = "unidata-nexrad-level2-chunks"
ARCHIVE_BUCKET = "unidata-nexrad-level2"
WEATHER_RADAR_STATIONS_URL = "https://api.weather.gov/radar/stations"

ALLOWED_VCPS = frozenset({12, 212, 215})
ANGLE_DEDUP_TOLERANCE_DEG = 0.1
HIGH_MAX_ANGLE_DEG = 4.0

WEATHER_API_TIMEOUT_SECONDS = 15
WEATHER_API_CACHE_TTL_SECONDS = 30

# Realtime pipeline deadlines.  These are deliberately application-level
# bounds: SDK/network defaults alone do not protect a scan cycle from a task
# that never resolves.
NEXRAD_VOLUME_DISCOVERY_TIMEOUT_SECONDS = 20.0
NEXRAD_CHUNK_LIST_TIMEOUT_SECONDS = 20.0
NEXRAD_INGEST_TIMEOUT_SECONDS = 120.0
NEXRAD_SCAN_TIMEOUT_SECONDS = 180.0
NEXRAD_CANCELLATION_GRACE_SECONDS = 2.0
NEXRAD_HEARTBEAT_STALE_SECONDS = 240.0
NEXRAD_HEARTBEAT_STARTUP_GRACE_SECONDS = 60.0


def format_perf_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    import time
    return (time.perf_counter() - started_at) * 1000
