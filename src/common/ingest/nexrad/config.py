CHUNKS_BUCKET = "unidata-nexrad-level2-chunks"
ARCHIVE_BUCKET = "unidata-nexrad-level2"
WEATHER_RADAR_STATIONS_URL = "https://api.weather.gov/radar/stations"

ALLOWED_VCPS = frozenset({12, 212, 215})
LOW_BINS = (0.5, 0.9)
ANGLE_DEDUP_TOLERANCE_DEG = 0.1
HIGH_MAX_ANGLE_DEG = 4.0
LOW_MAX_ANGLE_DEG = 1.0

LOW_CHECKPOINT_HINT = 25
FIRST_SIX_CHECKPOINT_HINT = 37
HIGH_CHECKPOINT_HINTS = {
    12: 61,
    212: 61,
    215: 61,
}

WEATHER_API_USER_AGENT = "(EdgeWARN/2.5.2, ewsbackend@gmail.com)"
WEATHER_API_TIMEOUT_SECONDS = 15
WEATHER_API_CACHE_TTL_SECONDS = 30

EXPECTED_HIGH_BINS = {
    12: (1.2, 1.8, 2.4, 3.1, 4.0),
    212: (1.3, 1.8, 2.4, 3.1, 4.0),
    215: (1.2, 1.8, 2.4, 3.0, 4.0),
}


def format_perf_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    import time
    return (time.perf_counter() - started_at) * 1000
