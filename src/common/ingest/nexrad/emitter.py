from util.io import IOManager

io_manager = IOManager("[NEXRAD-PIPE]", include_timestamps=True)


class NexradDownloadEmitter:
    def __init__(self, callback=None):
        self.callback = callback

    def emit_downloaded_sites(self, sites):
        ordered_sites = tuple(sorted({str(site).upper() for site in sites if site}))
        if not ordered_sites:
            return
        io_manager.write_info(f"Downloaded sites: {list(ordered_sites)}")
        if self.callback is not None:
            self.callback(ordered_sites)
