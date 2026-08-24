"""EWMRS phase-record consumer (decomposition Phase 4).

Consumes committed ``mrms-ready`` and ``rap-ready`` phase records published by
the primary service, rendering from the exact pinned paths in each record.
Correctness rules (plans/realtime-runner-decomposition-plan.md):

- Records are processed in cycle-timestamp order; a malformed record stops
  the drain so newer cycles are never rendered under older timestamps.
- A consumer checkpoint advances only after validated artifact publication;
  render failures are retried on the next poll without advancing.
- Cycles beyond ``cycle.max_backlog_cycles`` are marked unrecoverable
  explicitly and skipped without rendering; processing resumes at the oldest
  still-valid record.
- Validation failures (missing or misaligned exact inputs) mark that cycle
  unrecoverable explicitly rather than blocking the backlog forever.

This module runs only inside the EWMRS service process tree; importing it is
allowed to load the EWMRS render stack lazily inside its methods.
"""

import signal

from util.io import QueueWriter
from util.runtime.config import section
from util.runtime.handoff import (
    ConsumerCheckpointStore,
    PhaseRecord,
    select_pending_records,
    shadow_validate_phase_record,
)
from util.runtime.logging import queue_log
from util.runtime.timing import sleep_for

CONSUMER_NAME = "ewmrs"


class EwmrsRecordConsumer:
    """Ordered, checkpointed consumer of primary-published phase records.

    Each phase keeps its own durable checkpoint so a failure in one phase can
    never be masked by the other phase's progress.
    """

    def __init__(self, base_dir, *, log=None, max_backlog=None):
        self.base_dir = base_dir
        self._log = log if log is not None else print
        self._max_backlog = (
            int(section("cycle")["max_backlog_cycles"])
            if max_backlog is None
            else int(max_backlog)
        )

    def checkpoint_for(self, phase: str):
        return ConsumerCheckpointStore(
            self.base_dir, f"{CONSUMER_NAME}-{phase}"
        ).load()

    def process_pending_once(self) -> tuple[int, int]:
        """Drain both phases once; returns ``(processed, skipped)`` counts."""
        processed = skipped = 0
        for phase in ("mrms-ready", "rap-ready"):
            phase_processed, phase_skipped = self._drain(
                phase,
                self._handle_mrms_ready if phase == "mrms-ready" else self._handle_rap_ready,
            )
            processed += phase_processed
            skipped += phase_skipped
        return processed, skipped

    def _drain(self, phase: str, handler) -> tuple[int, int]:
        checkpoint_store = ConsumerCheckpointStore(
            self.base_dir, f"{CONSUMER_NAME}-{phase}"
        )
        pending = select_pending_records(
            self.base_dir,
            phase,
            checkpoint=checkpoint_store.load(),
            max_backlog=self._max_backlog,
        )
        processed = skipped = 0
        for cycle_id, record, status in pending:
            if record is None:
                # Malformed evidence must stay visible: stop here instead of
                # silently rendering a newer file under an older timestamp.
                self._log(
                    f"[EWMRS] {phase} record for {cycle_id} is missing or "
                    "malformed; stopping this drain until it is repaired"
                )
                break
            try:
                if status == "abandoned-backlog":
                    self._log(
                        f"[EWMRS] {phase} cycle {cycle_id} exceeded the "
                        "backlog cap; recorded as unrecoverable without rendering"
                    )
                    checkpoint_store.record(cycle_id)
                    skipped += 1
                    continue
                problems = shadow_validate_phase_record(
                    record,
                    layers=(self._mrms_layers() if phase == "mrms-ready" else None),
                )
                if problems:
                    self._log(
                        f"[EWMRS] {phase} cycle {cycle_id} is unrecoverable; "
                        f"validation problems: {list(problems)}"
                    )
                    checkpoint_store.record(cycle_id)
                    skipped += 1
                    continue
                handler(record)
                checkpoint_store.record(cycle_id)
                processed += 1
                self._log(f"[EWMRS] Consumed {phase} cycle {cycle_id}")
            except Exception as exc:
                # Render failure: do not advance the checkpoint; the record is
                # retried on the next poll.
                self._log(f"[EWMRS] {phase} cycle {cycle_id} failed: {exc}")
                break
        return processed, skipped

    def _mrms_layers(self):
        from EWMRS.render.config import get_mrms_file_list

        return get_mrms_file_list()

    def _handle_mrms_ready(self, record: PhaseRecord):
        from EWMRS.pipeline import mrms_required_layer_failures, run_mrms_render_pipeline

        results = run_mrms_render_pipeline(
            record.analysis_time,
            input_manifest=record.to_manifest(),
        )
        failed_required, failed_optional = mrms_required_layer_failures(results)
        for optional_layer in failed_optional:
            self._log(
                f"[EWMRS] Optional MRMS layer failed to render: {optional_layer}"
            )
        if not results or failed_required:
            raise RuntimeError(
                "MRMS render did not produce the complete required layer set"
                + (f": {', '.join(failed_required)}" if failed_required else "")
            )

    def _handle_rap_ready(self, record: PhaseRecord):
        from EWMRS.pipeline import run_rap_uint16_pipeline

        rap_inputs = [
            staged for staged in record.inputs if getattr(staged, "family", "") == "rap"
        ]
        if not rap_inputs:
            raise RuntimeError("rap-ready record pins no RAP input")
        results = run_rap_uint16_pipeline(rap_inputs[0].path, record.analysis_time)
        incomplete = [name for name, path in results.items() if path is None]
        if not results or len(incomplete) == len(results):
            raise RuntimeError("RAP Uint16 conversion produced no usable layers")


def ewmrs_consumer_loop(base_dir, log_queue):
    """Supervised child target: consume committed records until stopped."""
    from util.runtime.process_identity import set_process_name

    set_process_name("EWMRS-Consumer")
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def _stop(_signum, _frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _stop)

    consumer = EwmrsRecordConsumer(base_dir, log=lambda msg: queue_log(log_queue, str(msg)))
    intervals = section("background_intervals")
    sleep_seconds = float(intervals["ewmrs_consumer_seconds"])
    poll_interval = float(intervals["ewmrs_consumer_interval_seconds"])
    while True:
        try:
            processed, skipped = consumer.process_pending_once()
            if processed or skipped:
                queue_log(
                    log_queue,
                    f"INFO: EWMRS consumer pass: {processed} record(s) rendered, "
                    f"{skipped} marked unrecoverable",
                )
        except KeyboardInterrupt:
            return
        except Exception as exc:
            queue_log(log_queue, f"ERROR: EWMRS consumer pass failed: {exc}")
        sleep_for(sleep_seconds, interval=poll_interval)
