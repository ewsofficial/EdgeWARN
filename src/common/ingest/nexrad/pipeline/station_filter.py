import asyncio

from common.ingest.nexrad.config import ALLOWED_VCPS
from common.ingest.nexrad.weather_api import fetch_radar_station_vcps
from util.io import IOManager

io_manager = IOManager("[NEXRAD-PIPE]")


class NexradStationFilter:
    def __init__(self, *, station_fetcher=None):
        self.station_fetcher = station_fetcher or fetch_radar_station_vcps

    async def fetch_allowed_stations(self, *, sites=None, weather_session=None):
        stations = await asyncio.to_thread(self.station_fetcher, session=weather_session)
        requested_sites = None if sites is None else {str(site).upper() for site in sites}
        allowed = []
        skipped_missing = 0
        skipped_non_us = 0
        skipped_vcp = 0

        for site, station in sorted(stations.items()):
            site = str(site).upper()
            if requested_sites is not None and site not in requested_sites:
                continue
            if not site.startswith("K"):
                skipped_non_us += 1
                continue
            if station.vcp not in ALLOWED_VCPS:
                skipped_vcp += 1
                continue
            allowed.append((site, station))

        if requested_sites is not None:
            matched_sites = {site for site, _station in allowed}
            skipped_missing = len(requested_sites - matched_sites - {
                site for site in requested_sites if site in stations and not site.startswith("K")
            } - {
                site
                for site in requested_sites
                if site in stations and stations[site].vcp not in ALLOWED_VCPS
            })

        io_manager.write_info(
            "[STATIONS] fetched=%s requested=%s allowed=%s skipped_missing=%s skipped_non_us=%s skipped_vcp=%s"
            % (
                len(stations),
                len(requested_sites) if requested_sites is not None else "all",
                len(allowed),
                skipped_missing,
                skipped_non_us,
                skipped_vcp,
            )
        )
        return allowed
