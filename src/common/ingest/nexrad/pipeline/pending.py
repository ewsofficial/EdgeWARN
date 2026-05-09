from common.ingest.nexrad.pipeline.models import PendingVolume


class NexradPendingVolumeTracker:
    def __init__(self):
        self.pending: dict[tuple[str, str], PendingVolume] = {}

    def upsert(self, pending_volume: PendingVolume):
        key = (pending_volume.site, pending_volume.volume_id)
        self.pending[key] = pending_volume

    def remove(self, site: str, volume_id: str):
        self.pending.pop((str(site).upper(), str(volume_id)), None)

    def drop_stale_for_site(self, site: str, keep_volume_id: str | None):
        site = str(site).upper()
        stale_keys = [
            key for key in self.pending if key[0] == site and key[1] != keep_volume_id
        ]
        for key in stale_keys:
            self.pending.pop(key, None)

    def items(self):
        return list(self.pending.items())
