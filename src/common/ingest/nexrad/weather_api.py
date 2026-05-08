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

    latency_properties = properties.get("latency") or {}
    rda = properties.get("rda") or {}
    rda_properties = ((properties.get("rda") or {}).get("properties") or {})
    raw_vcp = rda_properties.get("volumeCoveragePattern")
    return RadarStationVcp(
        site=str(site).upper(),
        vcp=normalize_weather_vcp(raw_vcp),
        raw_vcp=raw_vcp,
        rda_timestamp=rda.get("timestamp") or rda_properties.get("timestamp"),
        level_two_last_received_time=(
            latency_properties.get("levelTwoLastReceivedTime")
            or properties.get("timeOfLastLevelTwoVolumeScan")
        ),
        properties=properties,
    )


class RadarStationCatalog:
    def __init__(
        self,
        *,
        url=WEATHER_RADAR_STATIONS_URL,
        user_agent=WEATHER_API_USER_AGENT,
        timeout_seconds=WEATHER_API_TIMEOUT_SECONDS,
        cache_ttl_seconds=WEATHER_API_CACHE_TTL_SECONDS,
    ):
        self.url = url
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache_lock = threading.Lock()
        self._station_cache: dict[str, RadarStationVcp] = {}
        self._station_cache_expires_at = 0.0

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept": "application/geo+json",
        }

    def fetch_radar_station_vcps(self, *, session=None) -> dict[str, RadarStationVcp]:
        headers = self._headers()
        timeout = self.timeout_seconds
        if session is None:
            response = requests.get(self.url, headers=headers, timeout=timeout)
        else:
            response = session.get(self.url, headers=headers, timeout=timeout)

        response.raise_for_status()
        payload = response.json()
        features = payload.get("features") or []
        stations = {}
        for feature in features:
            station = _build_station_record(feature)
            if station is not None:
                stations[station.site] = station
        return stations

    def get_station_vcp(self, site, *, session=None, cache_ttl_seconds=None) -> RadarStationVcp | None:
        site = str(site).upper()
        now = time.time()
        ttl = self.cache_ttl_seconds if cache_ttl_seconds is None else cache_ttl_seconds

        with self._cache_lock:
            if self._station_cache and now < self._station_cache_expires_at:
                return self._station_cache.get(site)

        stations = self.fetch_radar_station_vcps(session=session)
        expires_at = now + max(ttl, 0)
        with self._cache_lock:
            self._station_cache.clear()
            self._station_cache.update(stations)
            self._station_cache_expires_at = expires_at
            return self._station_cache.get(site)

    def reset_station_vcp_cache(self):
        with self._cache_lock:
            self._station_cache.clear()
            self._station_cache_expires_at = 0.0


_DEFAULT_CATALOG = RadarStationCatalog()


def fetch_radar_station_vcps(session=None) -> dict[str, RadarStationVcp]:
    return _DEFAULT_CATALOG.fetch_radar_station_vcps(session=session)


def get_station_vcp(site, *, cache_ttl_seconds=WEATHER_API_CACHE_TTL_SECONDS, session=None) -> RadarStationVcp | None:
    return _DEFAULT_CATALOG.get_station_vcp(site, session=session, cache_ttl_seconds=cache_ttl_seconds)


def reset_station_vcp_cache():
    _DEFAULT_CATALOG.reset_station_vcp_cache()
