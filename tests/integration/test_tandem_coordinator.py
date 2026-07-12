from datetime import datetime, timezone
import asyncio

from common.pipeline.coordinator import run_tandem_ingest_cycle
import common.pipeline.coordinator as coordinator


def test_run_tandem_ingest_cycle_preserves_staged_readiness(monkeypatch):
    call_order = []
    callbacks = []

    async def fake_detection(dt, max_entries=10, remove_old_files=True):
        await asyncio.sleep(0.01)
        call_order.append("detection")

    async def fake_mrms_integration(dt, max_entries=10, remove_old_files=True):
        await asyncio.sleep(0.03)
        call_order.append("mrms_integration")

    async def fake_goes(dt, max_entries=10, hour_lookback=3):
        await asyncio.sleep(0.04)
        call_order.append("goes")

    async def fake_rap(dt):
        await asyncio.sleep(0.05)
        call_order.append("rap")
        return "rap.grib2"

    monkeypatch.setattr(coordinator.mrms_ingest, "download_detection_files_async", fake_detection)
    monkeypatch.setattr(coordinator.mrms_ingest, "download_integration_files_async", fake_mrms_integration)
    monkeypatch.setattr(coordinator, "download_all_goes_files_async", fake_goes)
    monkeypatch.setattr(coordinator, "download_rap_async", fake_rap)
    monkeypatch.setattr(coordinator, "_run_rap_uint16_conversion", lambda *args: asyncio.sleep(0, result=True))

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)

    state = asyncio.run(
        run_tandem_ingest_cycle(
            dt,
            lambda msg: None,
            on_detection_ready=lambda current_state: callbacks.append((
                "detection",
                current_state.detection_inputs_ready,
                current_state.ewmrs_mrms_inputs_ready,
                current_state.ewmrs_goes_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
            on_ewmrs_mrms_ready=lambda current_state: callbacks.append((
                "ewmrs_mrms",
                current_state.detection_inputs_ready,
                current_state.ewmrs_mrms_inputs_ready,
                current_state.ewmrs_goes_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
            on_ewmrs_goes_ready=lambda current_state: callbacks.append((
                "ewmrs_goes",
                current_state.detection_inputs_ready,
                current_state.ewmrs_mrms_inputs_ready,
                current_state.ewmrs_goes_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
            on_edgewarn_integration_ready=lambda current_state: callbacks.append((
                "integration",
                current_state.detection_inputs_ready,
                current_state.ewmrs_mrms_inputs_ready,
                current_state.ewmrs_goes_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
        )
    )

    assert call_order == ["detection", "mrms_integration", "goes", "rap"]
    assert [name for name, *_ in callbacks] == ["detection", "ewmrs_mrms", "ewmrs_goes", "integration"]
    assert callbacks[0] == ("detection", True, False, False, False)
    assert callbacks[1] == ("ewmrs_mrms", True, True, False, False)
    assert callbacks[2] == ("ewmrs_goes", True, True, True, True)
    assert callbacks[3] == ("integration", True, True, True, True)
    assert state.detection_inputs_ready is True
    assert state.ewmrs_mrms_inputs_ready is True
    assert state.ewmrs_goes_inputs_ready is True
    assert state.edgewarn_integration_inputs_ready is True


def test_run_tandem_ingest_cycle_can_skip_goes_readiness(monkeypatch):
    call_order = []

    async def fake_detection(dt, max_entries=10, remove_old_files=True):
        await asyncio.sleep(0.01)
        call_order.append("detection")

    async def fake_mrms_integration(dt, max_entries=10, remove_old_files=True):
        await asyncio.sleep(0.02)
        call_order.append("mrms_integration")

    async def fake_goes(dt, max_entries=10, hour_lookback=3):
        call_order.append("goes")

    async def fake_rap(dt):
        await asyncio.sleep(0.03)
        call_order.append("rap")
        return "rap.grib2"

    monkeypatch.setattr(coordinator.mrms_ingest, "download_detection_files_async", fake_detection)
    monkeypatch.setattr(coordinator.mrms_ingest, "download_integration_files_async", fake_mrms_integration)
    monkeypatch.setattr(coordinator, "download_all_goes_files_async", fake_goes)
    monkeypatch.setattr(coordinator, "download_rap_async", fake_rap)
    monkeypatch.setattr(coordinator, "_run_rap_uint16_conversion", lambda *args: asyncio.sleep(0, result=True))

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)
    state = asyncio.run(
        run_tandem_ingest_cycle(
            dt,
            lambda msg: None,
            include_goes=False,
        )
    )

    assert call_order == ["detection", "mrms_integration", "rap"]
    assert state.detection_inputs_ready is True
    assert state.ewmrs_mrms_inputs_ready is True
    assert state.ewmrs_goes_inputs_ready is False
    assert state.edgewarn_integration_inputs_ready is False
