"""Phase 4 EWMRS record-consumer tests.

Covers the consumption rules from
plans/realtime-runner-decomposition-plan.md: ordered processing of committed
records using exact paths, checkpoints advancing only after validated artifact
publication, restart recovery (start after primary / before primary), explicit
backlog abandonment, and render failures retrying without advancing.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

import EWMRS.pipeline as ewmrs_pipeline
from common.ingest.manifest import CycleInputManifest, StagedInput
from util.runtime.handoff import (
    PhaseRecordPublisher,
    canonical_cycle_id,
    phase_record_path,
    read_phase_record,
)
from util.runtime.ewmrs_consumer import EwmrsRecordConsumer


CYCLE_DT = datetime(2026, 3, 17, 20, 0, tzinfo=timezone.utc)


@pytest.fixture()
def fake_render(tmp_path, monkeypatch):
    calls = {"mrms": [], "rap": []}

    def fake_run_mrms(dt, max_entries=None, input_manifest=None):
        assert input_manifest is not None
        calls["mrms"].append((dt, input_manifest))
        return {"CompRefQC": "gui/CompRefQC/x.png"}

    def fake_run_rap(rap_file, dt=None):
        calls["rap"].append((rap_file, dt))
        return {"CAPE": "gui/RAP/CAPE/data.u16"}

    def fake_layer_list():
        return [
            {"name": product, "filepath": str(tmp_path / "mrms" / product)}
            for product in ("Detection", "Integration")
        ]

    monkeypatch.setattr(ewmrs_pipeline, "run_mrms_render_pipeline", fake_run_mrms)
    monkeypatch.setattr(ewmrs_pipeline, "run_rap_uint16_pipeline", fake_run_rap)
    import EWMRS.render.config as ewmrs_render_config

    monkeypatch.setattr(ewmrs_render_config, "get_mrms_file_list", fake_layer_list)
    return calls


def _staged(tmp_path, product, dt, family="mrms"):
    path = tmp_path / family / product / f"MRMS_{product}_{dt:%Y%m%d-%H%M%S}.grib2"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"data")
    return StagedInput(
        product=product,
        path=str(path),
        analysis_time=dt,
        source="test",
        family=family,
    )


def _commit(tmp_path, dt, *, with_rap=True):
    inputs = [_staged(tmp_path, "Detection", dt), _staged(tmp_path, "Integration", dt)]
    manifest = CycleInputManifest(cycle_time=dt, inputs=tuple(inputs))
    publisher = PhaseRecordPublisher(tmp_path)
    publisher.publish("mrms-ready", manifest)

    if with_rap:
        rap = _staged(tmp_path, "RAP", dt, family="rap")
        rap_manifest = CycleInputManifest(cycle_time=dt, inputs=(rap,))
        publisher.publish("rap-ready", rap_manifest)
    return canonical_cycle_id(dt)


def test_consumes_committed_records_after_primary(tmp_path, fake_render, monkeypatch):
    """EWMRS starting AFTER the primary drains already-committed cycles."""
    cycle_id = _commit(tmp_path, CYCLE_DT)
    consumer = EwmrsRecordConsumer(tmp_path)
    processed, skipped = consumer.process_pending_once()

    assert processed == 2  # one mrms-ready + one rap-ready
    assert skipped == 0
    rendered_dt, rendered_manifest = fake_render["mrms"][0]
    assert rendered_dt == CYCLE_DT
    # The render receives the exact pinned paths from the record.
    staged_products = {s.product for s in rendered_manifest.inputs}
    assert {"Detection", "Integration"} <= staged_products
    assert fake_render["rap"] == [(fake_render["rap"][0][0], CYCLE_DT)]
    # Checkpoint advanced only after validated publication.
    assert consumer.checkpoint_for("mrms-ready").last_processed_cycle_id == cycle_id
    assert consumer.checkpoint_for("rap-ready").last_processed_cycle_id == cycle_id


def test_consumer_started_before_primary_is_a_noop_then_picks_up(tmp_path, fake_render):
    """EWMRS starting BEFORE the primary idles, then consumes the next cycle."""
    consumer = EwmrsRecordConsumer(tmp_path)
    assert consumer.process_pending_once() == (0, 0)

    _commit(tmp_path, CYCLE_DT)
    processed, skipped = consumer.process_pending_once()
    assert (processed, skipped) == (2, 0)


def test_backlog_excess_marked_unrecoverable_without_rendering(tmp_path, fake_render):
    for hour in range(5):
        _commit(tmp_path, CYCLE_DT.replace(hour=hour))
    consumer = EwmrsRecordConsumer(tmp_path, max_backlog=2)
    processed, skipped = consumer.process_pending_once()

    # Oldest three abandoned without rendering; newest two rendered per phase.
    assert skipped == 6  # 3 abandoned cycles x both phases
    rendered_hours = sorted(dt.hour for dt, _ in fake_render["mrms"])
    assert rendered_hours == [3, 4]
    assert [dt.hour for _, dt in fake_render["rap"]] == [3, 4]
    expected = canonical_cycle_id(CYCLE_DT.replace(hour=4))
    assert consumer.checkpoint_for("mrms-ready").last_processed_cycle_id == expected
    assert consumer.checkpoint_for("rap-ready").last_processed_cycle_id == expected


def test_missing_exact_input_marks_cycle_unrecoverable(tmp_path, fake_render):
    """Cleanup overlap: deleted exact inputs never silently render newer files."""
    _commit(tmp_path, CYCLE_DT)
    # Delete every exact input behind both records.
    for phase in ("mrms-ready", "rap-ready"):
        record = read_phase_record(phase_record_path(tmp_path, canonical_cycle_id(CYCLE_DT), phase))
        for staged in record.inputs:
            staged.local_path.unlink()

    consumer = EwmrsRecordConsumer(tmp_path)
    processed, skipped = consumer.process_pending_once()
    assert processed == 0
    assert skipped == 2
    assert consumer.checkpoint_for("mrms-ready").last_processed_cycle_id == canonical_cycle_id(CYCLE_DT)
    assert consumer.checkpoint_for("rap-ready").last_processed_cycle_id == canonical_cycle_id(CYCLE_DT)


def test_render_failure_retries_without_advancing(tmp_path, fake_render, monkeypatch):
    _commit(tmp_path, CYCLE_DT)

    def failing_mrms(dt, max_entries=None, input_manifest=None):
        raise RuntimeError("renderer exploded")

    monkeypatch.setattr(ewmrs_pipeline, "run_mrms_render_pipeline", failing_mrms)
    consumer = EwmrsRecordConsumer(tmp_path)
    processed, _skipped = consumer.process_pending_once()

    # The mrms phase contributed nothing (failed, stays pending for retry);
    # the rap phase drained independently.
    assert len(fake_render["mrms"]) == 0
    assert processed == 1 and fake_render["rap"]
    # Per-phase checkpoints: the failed mrms cycle stays pending for retry...
    assert consumer.checkpoint_for("mrms-ready") is None
    # ...while the successfully rendered rap phase advanced its own cursor.
    assert consumer.checkpoint_for("rap-ready").last_processed_cycle_id == canonical_cycle_id(CYCLE_DT)


def test_malformed_record_stops_drain_preserving_order(tmp_path, fake_render):
    later = CYCLE_DT.replace(hour=21)
    _commit(tmp_path, CYCLE_DT)
    _commit(tmp_path, later)
    target = phase_record_path(tmp_path, canonical_cycle_id(CYCLE_DT), "mrms-ready")
    target.write_text("{not json")

    consumer = EwmrsRecordConsumer(tmp_path)
    processed, skipped = consumer.process_pending_once()

    # The malformed record blocks the mrms drain entirely; nothing newer may
    # be rendered under an older timestamp's identity.
    assert all(dt.hour != 21 for dt, _ in fake_render["mrms"])
    # rap-ready records are unaffected by the malformed mrms file.
    assert [dt.hour for _, dt in fake_render["rap"]] == [20, 21]
    assert processed >= 0 and skipped == 0
