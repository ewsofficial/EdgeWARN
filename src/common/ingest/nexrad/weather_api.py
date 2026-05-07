import threading
import time

import requests

from common.ingest.nexrad.config import (
    WEATHER_API_CACHE_TTL_SECONDS,
    WEATHER_API_TIMEOUT_SECONDS,
    WEATHER_API_USER_AGENT,
    WEATHER_RADAR_STATIONS_URL,
)
from common.ingest.nexrad.models import RadarStationVcp

_cache_lock = threading.Lock()
_station_cache: dict[str, RadarStationVcp] = {}
_station_cache_expires_at = 0.0


def normalize_weather_vcp(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value

    text = str(value).strip().upper()
    if not text:
        return None
    if text.startswith("VCP-"):
        text = text[4:]
    if text.startswith("R"):
        text = text[1:]
    return int(text) if text.isdigit() else None


def _build_station_record(feature: dict) -> RadarStationVcp | None:
    properties = feature.get("properties") or {}
    site = properties.get("id")
    if not site:
        return None

    rda_properties = ((properties.get("rda") or {}).get("properties") or {})
    raw_vcp = rda_properties.get("volumeCoveragePattern")
    return RadarStationVcp(
        site=str(site).upper(),
        vcp=normalize_weather_vcp(raw_vcp),
        raw_vcp=raw_vcp,
        rda_timestamp=rda_properties.get("timestamp"),
        level_two_last_received_time=properties.get("timeOfLastLevelTwoVolumeScan"),
        properties=properties,
    )


def fetch_radar_station_vcps(session=None) -> dict[str, RadarStationVcp]:
    headers = {
        "User-Agent": WEATHER_API_USER_AGENT,
        "Accept": "application/geo+json",
    }

    if session is None:
        response = requests.get(
            WEATHER_RADAR_STATIONS_URL,
            headers=headers,
            timeout=WEATHER_API_TIMEOUT_SECONDS,
        )
    else:
        response = session.get(
            WEATHER_RADAR_STATIONS_URL,
            headers=headers,
            timeout=WEATHER_API_TIMEOUT_SECONDS,
        )

    response.raise_for_status()
    payload = response.json()
    features = payload.get("features") or []
    stations = {}
    for feature in features:
        station = _build_station_record(feature)
        if station is not None:
            stations[station.site] = station
    return stations


def get_station_vcp(site, *, cache_ttl_seconds=WEATHER_API_CACHE_TTL_SECONDS, session=None) -> RadarStationVcp | None:
    global _station_cache_expires_at

    site = str(site).upper()
    now = time.time()

    with _cache_lock:
        if _station_cache and now < _station_cache_expires_at:
            return _station_cache.get(site)

    stations = fetch_radar_station_vcps(session=session)
    expires_at = now + max(cache_ttl_seconds, 0)
    with _cache_lock:
        _station_cache.clear()
        _station_cache.update(stations)
        _station_cache_expires_at = expires_at
        return _station_cache.get(site)


def reset_station_vcp_cache():
    global _station_cache_expires_at
    with _cache_lock:
        _station_cache.clear()
        _station_cache_expires_at = 0.0
