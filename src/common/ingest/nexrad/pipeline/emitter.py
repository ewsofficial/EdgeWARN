from common.ingest.nexrad.models import NexradCompletionRecord
from util.io import IOManager

io_manager = IOManager("[NEXRAD-PIPE]", include_timestamps=True)


class NexradDownloadEmitter:
    def __init__(self, callback=None):
        self.callback = callback

    @staticmethod
    def _record_key(record: NexradCompletionRecord):
        return (
            str(record.site).upper(),
            str(record.scan_timestamp or ""),
            str(record.volume_id),
            str(record.manifest_path or ""),
            str(record.volume_path or ""),
        )

    def emit_downloaded_sites(self, records):
        deduped = {}
        for record in records:
            if record is None:
                continue
            normalized = NexradCompletionRecord(
                site=str(record.site).upper(),
                volume_id=str(record.volume_id),
                scan_timestamp=None if record.scan_timestamp is None else str(record.scan_timestamp),
                volume_path=record.volume_path,
                manifest_path=record.manifest_path,
            )
            deduped[self._record_key(normalized)] = normalized

        ordered_records = tuple(deduped[key] for key in sorted(deduped))
        if not ordered_records:
            return
        io_manager.write_info(
            f"Downloaded scans: {[f'{record.site}/{record.volume_id}' for record in ordered_records]}"
        )
        if self.callback is not None:
            self.callback(ordered_records)
