CHUNKS_BUCKET = "unidata-nexrad-level2-chunks"
ARCHIVE_BUCKET = "unidata-nexrad-level2"
WEATHER_RADAR_STATIONS_URL = "https://api.weather.gov/radar/stations"

ALLOWED_VCPS = frozenset({12, 212, 215})
ANGLE_DEDUP_TOLERANCE_DEG = 0.1
HIGH_MAX_ANGLE_DEG = 4.0
MIN_VOLUME_FILE_CHUNKS = 25

WEATHER_API_USER_AGENT = "(EdgeWARN/2.5.2, ewsbackend@gmail.com)"
WEATHER_API_TIMEOUT_SECONDS = 15
WEATHER_API_CACHE_TTL_SECONDS = 30


def format_perf_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    import time
    return (time.perf_counter() - started_at) * 1000
