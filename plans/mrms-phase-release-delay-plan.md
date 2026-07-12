# MRMS Phase-Release Delay Remediation Plan

**Scope:** real-time tandem-cycle readiness, worker lifecycle, MRMS stage
validation, prerequisite concurrency, and producer-time phase observability.

**Goal:** release EdgeWARN detection and EWMRS MRMS rendering immediately after
their validated inputs are ready, without changing artifacts, public CLI
behavior, or EdgeWARN integration prerequisites.

This plan is intentionally separate from
[Expanded-Gate Detection Optimization Plan](detection-expanded-gates-optimization-plan.md),
so scheduling gains and detection-algorithm gains can be measured independently.

## Evidence and diagnosis

The supplied operational log showed the following elapsed time after the three
detection MRMS products completed:

| Milestone | Time | Delay after detection MRMS |
| --- | ---: | ---: |
| Detection MRMS ingest complete | 20:51:40.102 | 0.0 s |
| Integration MRMS ingest complete | 20:51:42.405 | 2.3 s |
| RAP download and Uint16 conversion complete | 20:51:46.103 | 6.0 s |
| Inline scan-time GLM merge complete | 20:51:58.673 | 18.6 s |
| Background GOES ABI ingest complete | 20:52:00.877 | 20.8 s |

The problem is release placement, not process-start cost. In
`src/util/runtime/cycle.py`, `run_tandem_cycle_once()` currently awaits the
shared ingest coordinator to completion, runs scan-time GLM download/merge
synchronously, then constructs and starts both workers and sets all phase
events together.

```text
Current
  detection MRMS -> integration MRMS/RAP/RAP Uint16 -> inline GLM
  -> start workers -> set all events -> detection/render begin

Target
  start workers in their waiting state
  detection MRMS -> validate and release EdgeWARN detection
  both MRMS groups -> validate and release EWMRS MRMS rendering
  integration MRMS + raw RAP + scan-time GLM -> release EdgeWARN integration
  RAP Uint16 and GOES ABI continue independently
```

Background GOES ABI does not gate worker release today, but it can delay parent
queue draining. Queue records are plain strings, and the parent applies
`TimestampedOutput` when it drains them; queued worker and coordinator logs
therefore show drain time rather than producer time. The 18.6-second estimate
must be validated with phase telemetry after implementation.

## Verified constraints

| Area | Current behavior | Required outcome |
| --- | --- | --- |
| `util/runtime/cycle.py` | Starts workers only after ingest and inline GLM. | Own early worker start, phase publication, and failure-safe teardown. |
| `common/pipeline/coordinator.py` | Starts detection MRMS, integration MRMS, and RAP concurrently, but waits for RAP Uint16 before returning. | Preserve staged order while exposing raw RAP readiness before Uint16 completes. |
| MRMS stage wrappers | Discard `DownloadBatchResult`; a logged partial failure can still look ready. | Validate all required products before publishing any event. |
| MRMS sync fallback | Both phase fallbacks call full `download_all_files()`, which downloads all MRMS plus GOES. | Use phase-scoped fallback so a detection fallback does not erase the phase boundary. |
| EdgeWARN worker | Waits for detection, runs detection, then waits for integration. | Preserve this ordering and stormcell/cell schemas. |
| EWMRS tandem worker | Waits only for MRMS; its GOES event argument is unused. | Release MRMS rendering only; keep GOES rendering decoupled. |
| GLM versus ABI | Scan-time GLM is an integration input; ABI is a render input. | Never substitute ABI readiness for GLM readiness. |

Non-negotiable readiness rules:

- Detection requires the existing three detection products only.
- EWMRS MRMS rendering retains the current conservative gate: validated
  detection and integration MRMS groups. Using the narrower render subset is a
  separate optimization.
- EdgeWARN integration requires both MRMS groups, the original RAP GRIB2 file,
  and scan-time GLM when GOES is enabled. It must not wait for RAP Uint16 or
  background ABI readiness.
- State and errors must be written to the manager-backed shared state before
  the event that releases a worker is set.
- Every success, failure, interruption, and partial-start path must set all
  relevant events, allowing waiters to make their existing skip decision and
  terminate.
- Preserve `--disable-ewmrs`, `--disable-goes`, MRMS product grouping,
  timestamp matching, S3/HTTPS fallbacks, and runtime output paths.

## Implementation plan

## Execution checklist

### Step 1 — Lock down readiness contracts

- [ ] Propagate `DownloadBatchResult` from detection and integration ingest
  wrappers to the coordinator.
- [ ] Define and test stage success as all required products staged for the
  requested cycle timestamp.
- [ ] Replace full-ingest fallback with detection- and integration-scoped
  fallback helpers.
- [ ] Add explicit raw-RAP and integration-MRMS state independent of RAP Uint16.

### Step 2 — Add deterministic safety coverage

- [ ] Add cycle-level tests with controlled events, fake processes, and a fake
  manager before changing worker timing.
- [ ] Cover detection, integration MRMS, RAP, GLM, RAP Uint16, callback,
  process-start, and interruption failures.
- [ ] Assert state is published before every event and every waiter is released.

### Step 3 — Publish staged readiness and prestart workers

- [ ] Initialize terminal-false shared state and all events before worker start.
- [ ] Start/register EdgeWARN and optional EWMRS workers before ingest begins.
- [ ] Wire validated detection and EWMRS-MRMS callbacks to the phase publisher.
- [ ] Add the raw-RAP/base-integration transition before RAP Uint16 completion.
- [ ] Start GLM only after worker startup and aggregate it with base integration
  readiness before setting `integration_ready_event`.
- [ ] Remove the unused EWMRS tandem GOES event from its real-time interface.

### Step 4 — Decouple and observe non-critical work

- [ ] Ensure RAP Uint16 and ABI polling cannot delay detection or EWMRS MRMS
  release.
- [ ] Prevent or defer concurrent MRMS/GOES GUI index cleanup conflicts.
- [ ] Replace plain runtime queue messages with backward-compatible structured
  records and support legacy strings during draining.
- [ ] Render producer timestamps exactly once and emit monotonic phase timings.

### Step 5 — Verify operationally and roll out

- [ ] Run the targeted test command in this plan, then related tests and the
  complete Python suite.
- [ ] Compare five or more warm cycles and exact artifacts against baseline.
- [ ] Confirm release latency is at most 0.5 seconds for detection and EWMRS
  MRMS transitions.
- [ ] Confirm no orphaned process, false-ready stage, material RSS regression,
  or ABI/RAP-Uint16 critical-path regression.

### 1. Make MRMS readiness truthful before releasing early

`download_all_files_async_internal()` returns a `DownloadBatchResult`, but
`run_ingestion_pipeline()` and the detection/integration wrappers discard it.
`_safe_ingest()` therefore accepts a no-exception result even when one or more
products failed. Correct this contract first.

1. Propagate the detection and integration `DownloadBatchResult` to the
   coordinator, including expected, downloaded, and failed labels.
2. Give `_safe_ingest()` an explicit stage-success predicate. A required
   product failure is a failed readiness stage, not a successful coroutine.
3. Add stage-scoped sync fallback helpers parameterized by the detection or
   integration modifier set. Preserve the current async-first/S3/HTTPS behavior
   without fetching unrelated MRMS or GOES inputs.
4. Add explicit state for integration MRMS and raw RAP readiness. Do not infer
   these solely from the absence of an error key; retain RAP Uint16 as an
   independent EWMRS-derived result.
5. Validate that staged files match the requested cycle before release. Current
   detection, integration, and EWMRS consumers select latest-by-mtime files, so
   a merely completed download can otherwise release a worker onto stale data.

### 2. Expose true phase transitions from the coordinator

Keep detection MRMS, integration MRMS, and RAP download concurrent; they are
already created as asyncio tasks. Publish their validated transitions without
waiting for derived work.

- Retain `on_detection_ready`, firing it after validated detection state is
  recorded.
- Retain `on_ewmrs_mrms_ready`, firing it after both validated MRMS groups.
- Add a raw-RAP/base-integration transition once both MRMS groups and the RAP
  source file are ready. It must fire before RAP Uint16 conversion is awaited.
- Start and track RAP Uint16 after raw RAP succeeds. It may be awaited before
  overall cycle completion, but cannot block any readiness release.
- Keep the coordinator's general GOES compatibility path backward-compatible.
  The real-time caller uses `include_goes=False`, so its new integration
  transition must not inherit the coordinator's deliberately false ABI result.

Use explicit state names (for example `mrms_integration_inputs_ready`,
`rap_inputs_ready`, `glm_inputs_ready`, and `ewmrs_goes_inputs_ready`) rather
than an overloaded GOES flag. This prevents GLM, ABI, and RAP Uint16 from being
reported as the same dependency.

### 3. Prestart workers and release each phase independently

Refactor `run_tandem_cycle_once()` around one parent-owned, idempotent phase
publisher.

1. Initialize all shared readiness fields as false, initialize an empty error
   map, and create all worker-consumed events.
2. Construct and start the EdgeWARN worker and, when enabled, EWMRS worker via
   `StartedProcessRegistry` before starting ingest. Register every successful
   start before proceeding.
3. Pass coordinator callbacks that publish the complete state/error snapshot,
   then set exactly one matching event.
4. Start scan-time GLM ingestion after workers have been started and alongside
   the coordinator, initially via `asyncio.to_thread` if needed. Mark GLM ready
   only after download/merge/write completes and local readiness is validated.
5. Combine base integration readiness with GLM readiness in one parent-owned
   aggregator, then publish and set `integration_ready_event` exactly once.
6. In a top-level `finally`, publish terminal unavailable state and set any
   remaining event before draining queues and terminating/joining every started
   process. Cover worker-start failure, ingest exception, callback exception,
   GLM error, and `KeyboardInterrupt`.

Remove the unused `ewmrs_goes_ready_event` from the EWMRS tandem-worker
signature and real-time handoff. Retain any separate GOES state needed by the
decoupled GOES scheduler.

### 4. Keep optional work and cleanup off the critical path

- Scan-time GLM runs concurrently but remains only an integration prerequisite.
- RAP Uint16 conversion remains tracked as an EWMRS artifact, never an input to
  EdgeWARN integration or EWMRS MRMS rendering.
- Continue draining cycle and GOES-render queues while polling bounded ABI
  readiness, or run that polling as an auxiliary task. ABI polling must not
  suppress live worker visibility.
- EWMRS MRMS cleanup currently touches MRMS and GOES GUI indexes while the
  decoupled GOES renderer can update those indexes. Earlier release widens this
  race. Before rollout, either constrain MRMS cleanup/index updates to its own
  layers or defer them until the GOES renderer cannot conflict.
- Capture CPU/RSS while detection arrays, EWMRS rendering, GLM merge, and RAP
  conversion overlap. Concurrency is the intended latency win, not a waiver
  for resource regressions.

### 5. Make phase timing producer-authoritative

Add a backward-compatible runtime queue-record shape with message text, UTC
emission time, monotonic emission time, optional cycle timestamp, and phase or
source. It must cover `queue_log`, worker-local direct queue puts, and
`QueueWriter`.

- Make `drain_log_queue()` accept both structured records and legacy strings.
- Add a timestamp-aware output path so a record prints with its producer time
  and is not stamped again by `TimestampedOutput` at drain time.
- Drain with `get_nowait()` and `queue.Empty`, not `Queue.empty()`.
- Log monotonic phase durations for worker start, MRMS ready/released, raw RAP
  ready, GLM ready, integration release, and optional-work completion.

This is lifecycle observability, not the low-noise cleanup covered by
[Logging Remediation Plan](logging-remediation-plan.md). Preserve one visible
timestamp per rendered record.

## Test and verification plan

Add deterministic cycle-level tests for `run_tandem_cycle_once()` using
controlled futures/events, fake processes, and a fake manager. No live data,
real sleeps, or process scheduling may determine correctness.

- Workers start before the detection ingest can complete.
- Detection releases while integration MRMS, RAP, GLM, RAP Uint16, and ABI are
  blocked.
- EWMRS MRMS releases after both MRMS groups while GLM, RAP Uint16, and ABI are
  blocked.
- Integration releases only after integration MRMS, raw RAP, and required GLM;
  it omits GLM only with `--disable-goes`.
- Required MRMS failure, RAP failure, GLM failure, RAP Uint16 failure, ingest
  exception, callback exception, partial process start, and interruption all
  release waiters and clean up processes.
- Delayed drain preserves producer ordering; legacy queue strings still work.

Update `tests/integration/test_tandem_coordinator.py`,
`tests/core/test_tandem_coordinator.py`,
`tests/integration/test_ewmrs_pipeline.py`, MRMS-ingest tests, and
`tests/util/test_io.py`. Add focused EdgeWARN worker barrier coverage, which is
currently absent. Update `docs/core/ingestion.md` and `docs/core/README.md` to
describe workers waiting during staged ingest rather than being created after it.

Run targeted tests first in `EdgeWARN-dev`:

```bash
python -m pytest \
  tests/integration/test_tandem_coordinator.py \
  tests/core/test_tandem_coordinator.py \
  tests/integration/test_ewmrs_pipeline.py \
  tests/integration/test_goes_readiness.py \
  tests/core/ingest/test_mrms_ewmrs.py \
  tests/util/test_io.py \
  tests/util/test_runtime.py \
  tests/util/test_runtime_background.py
```

Then run the related detection/integration tests, the full Python suite, and at
least five warm operational cycles over the same input window. Compare detection
rasters, stormcells, cells, alerts, and EWMRS MRMS artifacts exactly.

## Acceptance criteria

- EdgeWARN and, when enabled, EWMRS are alive and waiting before detection MRMS
  completes.
- Validated detection and EWMRS-MRMS transitions release their workers within
  **0.5 seconds**.
- A partial MRMS batch never reports ready, and its fallback never downloads
  unrelated MRMS/GOES inputs.
- Integration never begins without validated integration MRMS, raw RAP, and
  required GLM. RAP Uint16 and ABI never delay detection or EWMRS MRMS rendering.
- Every terminal path releases waiters and leaves no orphan process.
- Artifacts are unchanged for identical inputs, with no material RSS regression.
- Producer timestamps and phase durations prove the intended event ordering.

The expected benefit for the supplied run is removal of nearly all of the
measured **18.6-second** detection-release delay by overlap; the plan does not
claim to make RAP or GLM processing itself faster.

## Post-validation cleanup

- Remove the temporary parent-process `[PhaseTelemetry]` prints in
  `src/util/runtime/cycle.py` after producer-time structured queue records are
  implemented and operational phase-release timing has been validated. The
  temporary markers are intentionally direct-print instrumentation, not the
  permanent logging design.
