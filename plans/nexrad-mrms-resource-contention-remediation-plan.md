# NEXRAD / MRMS Resource-Contention Remediation Plan

**Scope:** real-time NEXRAD ingest and rendering concurrency while an EdgeWARN
and EWMRS tandem MRMS cycle is active.

**Goal:** protect the latency-sensitive MRMS tandem cycle from NEXRAD CPU,
memory, and filesystem contention without dropping required NEXRAD products,
interrupting artifact writes, changing MRMS outputs, or permanently disabling
NEXRAD.

This plan is intentionally separate from
[MRMS Phase-Release Delay Remediation Plan](mrms-phase-release-delay-plan.md).
That work fixes when validated MRMS phases are released. This work controls
the background load that competes with those released phases.

## Root cause

Persistent NEXRAD ingest and rendering run independently of the latency-sensitive
MRMS tandem cycle. They continue admitting CPU-, memory-, and filesystem-heavy
work while EdgeWARN detection/integration, EWMRS rendering, and CTAM are active.
The scheduler has no coordination mechanism that lets the tandem cycle reserve
host resources or ask NEXRAD workers to reach a safe idle boundary.

The contention path is visible in the current implementation:

- `src/run.py` starts persistent NEXRAD ingest and render processes before the
  scheduler begins tandem work.
- NEXRAD ingest can scan 24 sites, permit 64 concurrent chunk downloads, and
  use a four-process parser pool.
- NEXRAD rendering polls independently and can use eight render threads.
- Neither NEXRAD process observes tandem-cycle activity.
- NEXRAD parsing/export and rendering therefore overlap MRMS download/decompression,
  EdgeWARN detection/integration, EWMRS rendering, and CTAM large-grid work.

Operational observations are consistent with this root cause: a NEXRAD-enabled
cycle took about 97 seconds, while a NEXRAD-disabled cycle took about 44 seconds;
CTAM fell from 44.5 seconds to 13.987 seconds. Those unmatched live runs are not a precise speedup
measurement, but the remediation does not depend on attributing a percentage to
each NEXRAD component. The issue to fix is the uncoordinated admission of
expensive NEXRAD work during an active tandem cycle.

## Constraints

- Preserve NEXRAD Level-II artifact contents, manifests, GUI artifacts, site
  selection, VCP filtering, and retention behavior.
- Never terminate a parser, truncate a runtime volume, or interrupt an atomic
  artifact/index write merely because a tandem cycle starts.
- Preserve MRMS readiness contracts and the early phase releases implemented
  by the MRMS phase-release work.
- `--disable-nexrad` must continue to prevent both NEXRAD processes from
  starting.
- A failed, interrupted, or skipped tandem cycle must always release the
  NEXRAD pause request.
- Pausing must be bounded and observable. NEXRAD may finish already-started
  units of work, but it must stop admitting new expensive work promptly.
- Resume latest-first and bound catch-up work so a deferred backlog does not
  create a burst that overlaps the next MRMS cycle.
- Use the configured runtime base directory; do not add repository-local
  production output paths.
- Do not infer performance from queue-drain timestamps. Use producer-time UTC
  and monotonic telemetry.

## Target lifecycle

```text
MRMS update detected
  -> parent sets NEXRAD pause request
  -> NEXRAD ingest stops admitting new site/volume and parse work
  -> NEXRAD render stops admitting new artifact batches
  -> already-started atomic work drains (bounded acknowledgement wait)
  -> EdgeWARN/EWMRS tandem cycle runs with protected resources
  -> parent clears pause request in finally
  -> NEXRAD resumes with newest eligible work first and a bounded budget
```

The pause request protects the entire tandem worker lifetime, not only MRMS
download readiness. The largest observed contention was in EWMRS rendering,
integration after MRMS inputs were already ready.

## Execution checklist

### Step 1 — Add cooperative pause primitives

- [ ] Create one parent-owned `multiprocessing.Event` for the NEXRAD pause
  request and separate ingest/render quiescence acknowledgement events.
- [ ] Pass the events from `src/run.py` into `nexrad_ingest_loop()` and
  `nexrad_render_loop()`.
- [ ] Set the pause immediately before calling `run_tandem_cycle_once()` and
  clear it in a surrounding `finally` block.
- [ ] Wait only a configurable, short interval for acknowledgements; log a
  timeout and continue the tandem cycle rather than blocking MRMS indefinitely.
- [ ] Make shutdown override pause waits and terminate cleanly.
- [ ] Initially guard the behavior with an environment switch for controlled
  operational comparison.

### Step 2 — Make NEXRAD ingest pause-safe

- [ ] Check the pause request before station discovery, pending-volume checks,
  site admission, volume admission, and parser submission.
- [ ] Track active expensive units explicitly rather than inferring idleness
  from task or queue emptiness.
- [ ] When pause is requested, stop creating new site tasks and parser jobs;
  allow an active chunk write or parse/export operation to finish safely.
- [ ] Set the ingest-quiescent acknowledgement only when no new work can enter
  and all admitted expensive work has reached a safe boundary.
- [ ] Replace one-shot `asyncio.gather()` admission of every site with bounded,
  pause-aware scheduling so a pause can prevent not-yet-started sites from
  becoming active.
- [ ] On resume, rediscover current volumes and prefer the newest eligible
  volume instead of blindly draining stale deferred work.

### Step 3 — Make NEXRAD rendering pause-safe

- [ ] Check the pause request before directory scans and before starting a
  render batch.
- [ ] Submit render artifacts incrementally in small bounded batches instead
  of submitting the complete pending list to the thread pool at once.
- [ ] Stop admitting new artifacts when pause is requested; allow active
  serialization/index writes to finish.
- [ ] Set the render-quiescent acknowledgement after active artifacts drain.
- [ ] Recheck the pause between artifacts and before cleanup/index maintenance.
- [ ] Sort resumed work newest-first and impose a configurable per-poll artifact
  or wall-time budget.

### Step 4 — Add resource budgets and observability

- [ ] Retain `NEXRAD_WORKER_POOL_SIZE` and add validated configuration for site
  concurrency, chunk concurrency, render workers, and resume budget.
- [ ] Start with conservative operational defaults based on target-host capacity
  and refine them from protected-cycle and NEXRAD-freshness telemetry; do not
  merely expose the current constants as knobs.
- [ ] Emit producer-time records for pause requested, ingest quiescent, render
  quiescent, acknowledgement timeout, resume, active parser count, active
  render count, and deferred-work count.
- [ ] Capture tandem CPU/RSS/I/O summaries alongside MRMS phase durations so a
  future regression can distinguish release delay from host contention.
- [ ] Drain the NEXRAD log queue during tandem execution or use structured
  producer timestamps so log visibility is not deferred until the cycle ends.

### Step 5 — Validate, enable, and roll back safely

- [ ] Add deterministic unit and integration coverage for pause, resume,
  acknowledgement, interruption, exception, and shutdown paths.
- [ ] Run several operational shadow cycles with the pause policy enabled and
  compare producer-time phase/resource telemetry with retained unrestricted
  baseline observations.
- [ ] Verify NEXRAD freshness remains within an explicitly accepted service
  level and that no permanent backlog accumulates.
- [ ] Enable cooperative pause by default only after the acceptance criteria
  below are met.
- [ ] Retain an environment-level rollback to unrestricted NEXRAD scheduling
  and preserve `--disable-nexrad` as the existing full-disable control.

## Detailed implementation plan

### 1. Introduce a parent-owned pause contract

Add three shared events in `src/run.py`:

- `NEXRAD_PAUSE_REQUESTED`
- `NEXRAD_INGEST_QUIESCENT`
- `NEXRAD_RENDER_QUIESCENT`

Pass the request and matching acknowledgement into each background loop. The
parent should set the request immediately after deciding that a new MRMS cycle
will run, before constructing tandem workers or starting shared ingest. It may
wait briefly for enabled NEXRAD components to acknowledge a safe boundary, but
that wait must have a small configurable timeout and must not become a new MRMS
critical-path stall.

Wrap the complete `run_tandem_cycle_once()` call:

```text
set pause request
wait bounded time for enabled acknowledgements
try:
    run tandem cycle
finally:
    clear pause request
```

Acknowledgement timeout means “continue with possible residual contention,”
not “fail the weather cycle.” Log which component did not quiesce and its
active-work count.

### 2. Pause NEXRAD ingest at safe admission boundaries

The current realtime pipeline gathers all allowed-site coroutines together.
Although a semaphore limits active site work, every site is already represented
by a scheduled task. Refactor site execution around a bounded worker/iterator
or equivalent pause-aware admission loop.

When the request is clear, admit work up to the configured limit. Once it is
set:

- Do not start new station scans, pending checks, site-volume downloads, or
  parser submissions.
- Permit a chunk currently being written to finish and flush.
- Permit a parser/export future already running to complete; do not cancel a
  process performing NetCDF/AR2V or manifest writes.
- Persist enough runtime state to resume without redownloading completed
  chunks or corrupting a partial volume.
- Acknowledge only after admitted expensive work has drained to the defined
  safe boundary.

Pause checks must be frequent enough to prevent the next 24-site wave from
starting, but they must not be inserted inside low-level atomic file writes.

### 3. Pause NEXRAD rendering without partial artifacts

The renderer currently discovers every pending artifact and submits all of
them to an eight-thread executor. Change it to a bounded producer/consumer
loop or small batches so the pause request can stop admission.

An active artifact serialization may finish. New artifacts must not be
submitted after pause is observed. Index and cleanup operations must either
finish atomically or wait until resume. On resume, rediscover files because
the ingest process may have produced newer artifacts before it quiesced.

Use newest-first ordering and a bounded resume budget. If another tandem cycle
starts, the pause request immediately wins over the catch-up budget.

### 4. Configure rather than hard-code contention limits

Keep the existing parser-pool environment setting and add narrowly scoped,
validated settings such as:

- pause-during-tandem enablement;
- quiescence acknowledgement timeout;
- maximum active NEXRAD sites;
- maximum chunk downloads;
- NEXRAD render workers;
- maximum artifacts or seconds per resume pass.

Initial defaults should be conservative for the target host and then be tuned
from producer-time tandem latency, NEXRAD freshness, backlog, CPU, RSS, and I/O
telemetry. Invalid, zero, or negative values must fail clearly or clamp
consistently; they must not silently create an unbounded executor.

### 5. Add producer-time observability

Follow the producer-time structured-record direction in the MRMS phase-release
plan. At minimum, add monotonic timestamps at the producer for:

- tandem pause request;
- last ingest admission;
- last parser completion;
- ingest acknowledgement;
- last render completion;
- render acknowledgement;
- tandem start/completion;
- NEXRAD resume and first resumed admission.

Outer timestamps applied when the parent drains plain strings are not valid for
overlap analysis. Operational records should include pause configuration,
producer-time phase durations, quiescence latency, deferred work, NEXRAD
freshness, and peak resource summaries.

## Test plan

### Deterministic unit tests

Extend `tests/util/test_runtime_background.py` and add focused NEXRAD pipeline
tests covering:

- Pause already set when ingest/render starts: no expensive work is admitted
  and the component acknowledges quiescence.
- Pause arrives between two site admissions: the active site reaches its safe
  boundary and no later site starts.
- Pause arrives with a parser future active: it finishes, state is persisted,
  and acknowledgement follows completion.
- Pause arrives during a render batch: active artifacts finish and pending
  artifacts remain for resume.
- Resume clears acknowledgements, rediscovers newest inputs, and restarts work.
- A second pause preempts catch-up work.
- Shutdown while paused exits without waiting forever.
- Exceptions clear active-work accounting and cannot suppress acknowledgement.

### Cycle-level tests

Add fake-process/event tests around the scheduler handoff:

- The pause request is set before `run_tandem_cycle_once()` begins.
- Only enabled NEXRAD components are awaited.
- A missing acknowledgement times out without preventing the MRMS cycle.
- Success, exception, `KeyboardInterrupt`, and worker-start failure all clear
  the request.
- `--disable-nexrad` starts no NEXRAD process and waits for no acknowledgement.

### Operational verification

- Run targeted NEXRAD, runtime-background, tandem coordinator, EWMRS, and I/O
  tests in the `EdgeWARN-dev` environment.
- Run the complete Python test suite.
- Compare NEXRAD manifests/artifacts and MRMS-derived artifact schemas and
  contents before and after the scheduling change.
- Compare several pause-enabled cycles with retained unrestricted baseline
  telemetry, using producer timestamps and noting differences in inputs or cell
  counts rather than presenting unmatched live runs as precise speedups.
- Run long enough to span several MRMS and NEXRAD update periods and check for
  backlog, stale products, orphaned workers, or growing RSS.

## Acceptance criteria

- Detection validation-to-release remains at or below the existing 0.5-second
  target from the MRMS phase-release plan.
- Protected tandem cycles show materially lower overlap with NEXRAD parser and
  render work, and pause acknowledgement latency remains within the configured
  bounded wait or emits an explicit timeout record.
- No MRMS or NEXRAD output parity failure occurs.
- No parser/export or render artifact is left partial after pause, resume,
  exception, or shutdown testing.
- The pause request is cleared on every terminal path.
- NEXRAD catches up to the accepted freshness target before the next sustained
  backlog window; the target must be agreed and recorded before default-on
  rollout.
- No material peak-RSS regression, swap activity, orphaned process, or
  unbounded deferred-work growth is introduced.

## Rollout and rollback

1. Land producer-time pause/resource telemetry with the cooperative pause
   contract behind an environment flag, default off.
2. Add pause-safe ingest and render admission boundaries.
3. Run targeted tests and pause-enabled operational shadow cycles.
4. Refine conservative concurrency and resume-budget defaults from tandem
   latency, resource, backlog, and freshness evidence.
5. Enable pause by default after output parity, latency, and freshness criteria
   pass.
6. Roll back by disabling pause scheduling if NEXRAD freshness or correctness
   regresses; retain the producer-time telemetry.

## Non-goals

- Permanently disabling NEXRAD in production.
- Changing NEXRAD VCP eligibility, radar coverage, scientific parsing, or
  rendered product semantics.
- Reversing early MRMS phase release to avoid concurrency.
- Optimizing detection algorithms, CTAM algorithms, MRMS field statistics, or
  EWMRS rendering internals in this change.
- Claiming a precise speedup from unmatched live cycles.
