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

    async def fake_ewmrs(dt, max_entries=10, remove_old_files=True):
        await asyncio.sleep(0.02)
        call_order.append("ewmrs")

    async def fake_goes(dt, max_entries=10, hour_lookback=3):
        await asyncio.sleep(0.04)
        call_order.append("goes")

    async def fake_rap(dt):
        await asyncio.sleep(0.05)
        call_order.append("rap")

    monkeypatch.setattr(coordinator.mrms_ingest, "download_detection_files_async", fake_detection)
    monkeypatch.setattr(coordinator.mrms_ingest, "download_integration_files_async", fake_mrms_integration)
    monkeypatch.setattr(coordinator.mrms_ingest, "download_ewmrs_files_async", fake_ewmrs)
    monkeypatch.setattr(coordinator, "download_all_goes_files_async", fake_goes)
    monkeypatch.setattr(coordinator, "download_rap_async", fake_rap)

    dt = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)

    state = asyncio.run(
        run_tandem_ingest_cycle(
            dt,
            lambda msg: None,
            on_detection_ready=lambda current_state: callbacks.append((
                "detection",
                current_state.detection_inputs_ready,
                current_state.ewmrs_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
            on_ewmrs_ready=lambda current_state: callbacks.append((
                "ewmrs",
                current_state.detection_inputs_ready,
                current_state.ewmrs_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
            on_edgewarn_integration_ready=lambda current_state: callbacks.append((
                "integration",
                current_state.detection_inputs_ready,
                current_state.ewmrs_inputs_ready,
                current_state.edgewarn_integration_inputs_ready,
            )),
        )
    )

    assert call_order == ["detection", "ewmrs", "mrms_integration", "goes", "rap"]
    assert [name for name, *_ in callbacks] == ["detection", "ewmrs", "integration"]
    assert callbacks[0] == ("detection", True, False, False)
    assert callbacks[1] == ("ewmrs", True, True, False)
    assert callbacks[2] == ("integration", True, True, True)
    assert state.detection_inputs_ready is True
    assert state.ewmrs_inputs_ready is True
    assert state.edgewarn_integration_inputs_ready is True
