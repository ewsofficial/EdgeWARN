from util.runtime import background


def test_accessory_children_configure_parent_death_signal(monkeypatch):
    configured = []
    monkeypatch.setattr(
        background, "_configure_process_runtime", configured.append
    )
    monkeypatch.setattr(
        background, "section", lambda name: {
            "goes_coordination": {
                "pause_ingest_during_render": False,
                "poll_seconds": 1,
                "render_pause_poll_seconds": 1,
                "render_pause_poll_interval_seconds": 1,
                "poll_interval_seconds": 1,
            },
            "background_intervals": {
                "metar_boundary_minutes": 5,
                "boundary_wait_interval_seconds": 1,
                "nws_seconds": 1,
                "nws_interval_seconds": 1,
                "wpc_boundary_minutes": 5,
            },
        }[name])
    monkeypatch.setattr(background, "sleep_until_boundary", lambda *_args: (_ for _ in ()).throw(KeyboardInterrupt()))
    def interrupt_async(coroutine):
        coroutine.close()
        raise KeyboardInterrupt()

    monkeypatch.setattr(background.asyncio, "run", interrupt_async)

    class Event:
        def is_set(self):
            return False

        def set(self):
            pass

        def clear(self):
            pass

    background.goes_loop(Event(), Event())
    background.metar_loop()
    background.nws_loop()
    background.wpc_loop()

    assert configured == ["GOES-Ingest", "METAR-Ingest", "NWS-Ingest", "WPC-Ingest"]


class FakeQueue:
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


def test_nexrad_ingest_loop_restarts_after_exception(monkeypatch):
    log_queue = FakeQueue()
    calls = []

    def _fake_run_realtime_ingestion_pipeline(base_dir=None, **_kwargs):
        calls.append(base_dir)
        if len(calls) == 1:
            raise RuntimeError("boom")
        raise KeyboardInterrupt()

    monkeypatch.setattr(background, "run_realtime_ingestion_pipeline", _fake_run_realtime_ingestion_pipeline)
    monkeypatch.setattr(background, "sleep_for", lambda *_args, **_kwargs: None)

    background.nexrad_ingest_loop(log_queue, "/tmp/nexrad-base")

    assert calls == ["/tmp/nexrad-base", "/tmp/nexrad-base"]
    assert any("Starting NEXRAD ingest pipeline" in item for item in log_queue.items)
    assert any("NEXRAD ingest pipeline crashed: boom" in item for item in log_queue.items)


def test_nexrad_ingest_loop_skips_restart_when_shutdown_requested(monkeypatch):
    log_queue = FakeQueue()
    calls = []

    def _fake_run_realtime_ingestion_pipeline(base_dir=None, **_kwargs):
        calls.append(base_dir)
        background._SHUTDOWN_REQUESTED = True
        return None

    monkeypatch.setattr(background, "run_realtime_ingestion_pipeline", _fake_run_realtime_ingestion_pipeline)
    monkeypatch.setattr(background, "sleep_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(background, "_configure_process_runtime", lambda _name: None)
    background._SHUTDOWN_REQUESTED = False

    background.nexrad_ingest_loop(log_queue, "/tmp/nexrad-base")

    assert calls == ["/tmp/nexrad-base"]
    assert not any("restarting in 5s" in item for item in log_queue.items)
