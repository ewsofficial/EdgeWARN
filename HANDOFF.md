# EdgeWARN Runtime / Phase-Release Handoff

## Current repository state

- Branch: `yuchen-wei3667/improve-logging-system`
- Last committed change: `fa05b0c FIX: release tandem MRMS phases when inputs are ready`
- Do **not** discard the current uncommitted tracked changes:
  - `src/common/pipeline/coordinator.py`
    - Fixes `_safe_ingest()` when a nominally sync fallback returns an awaitable
      because it was called from an active event loop. The result is now
      awaited before it is treated as a RAP file path.
  - `src/run.py`, `src/util/io.py`, `tests/util/test_io.py`
    - Adds `--disable-nexrad`, which prevents both NEXRAD ingest and NEXRAD
      rendering processes from starting.
- User-owned/untracked files present before/during this work:
  - `LOGGING_AUDIT.md`
  - `plans/detection-expanded-gates-optimization-plan.md`
  - `plans/logging-remediation-plan.md`
  - `plans/mrms-phase-release-delay-plan.md`
- This handoff file is new and untracked. Do not assume the user wants it
  committed without asking.

## What was implemented and committed

Commit `fa05b0c` changes real-time phase scheduling:

1. EdgeWARN and EWMRS workers start before shared ingest, then wait on events.
2. Detection is released after the validated three-product detection MRMS batch.
3. EWMRS MRMS rendering is released after validated detection + integration MRMS.
4. EdgeWARN integration is released after both MRMS groups, raw RAP, and GLM
   when GOES is enabled. RAP Uint16 is not an integration prerequisite.
5. Detection/integration MRMS wrappers now propagate `DownloadBatchResult` and
   reject partial batches. Their sync fallbacks are product-phase scoped.
6. EWMRS no longer accepts the unused tandem GOES event.
7. Detection cleanup is now started concurrently but does not delay detection
   readiness.
8. Temporary direct `[PhaseTelemetry]` prints were added in
   `src/util/runtime/cycle.py`; they bypass the delayed runtime log queue.

The related plan includes a note to remove the temporary telemetry after
permanent producer-time structured queue records are implemented:
`plans/mrms-phase-release-delay-plan.md`.

## Important telemetry limitation

Worker/coordinator queue records are plain strings and are timestamped when
the parent drains them, not when a producer emits them. Do **not** compare
their outer timestamps directly to direct parent-process logs.

For phase timing, use `[PhaseTelemetry]` UTC/monotonic values. It currently
emits:

- `edgewarn_worker_started`
- `ewmrs_worker_started`
- `detection_mrms_validated`
- `detection_released`
- `ewmrs_mrms_released`
- `integration_released`

## Measured phase-release results

### Validation-to-release correctness

One validated cycle showed:

```text
detection_mrms_validated  monotonic=2069771.711297
detection_released        monotonic=2069771.711459
gap = 0.000162 seconds (0.162 ms)
```

Another cycle showed a 0.238 ms validation-to-release gap. This establishes
that detection is released immediately after its critical batch validates.

### Typical MRMS readiness timing

For a representative current cycle:

```text
detection MRMS batch (3 products):       ~3.28 s from worker start
both MRMS groups ready (3 + 25 products): ~4.21 s from worker start
```

The detection and integration MRMS groups run concurrently. A representative
gap from detection batch completion to integration batch completion was
0.934 seconds.

## Full pipeline performance findings

### NEXRAD enabled: completed 00:54Z cycle

This cycle completed in approximately **97.0 seconds** from worker start to
the tandem completion record. It processed roughly 240 cells.

Observed breakdown (queue-drain timestamps mean these are approximate except
the explicit module timers):

```text
MRMS readiness:         ~10 s
EdgeWARN detection:     ~16.3 s
Integration before CTAM:~29.0 s
CTAM total:             44.5 s
Total tandem:           97.0 s
```

### NEXRAD disabled: completed 01:30Z cycle

Run command used:

```bash
conda run --no-capture-output -n EdgeWARN-dev python src/run.py \
  --lat_limits 20 55 --lon_limits 230 300 --disable-nexrad
```

The log confirms:

```text
[Scheduler] NEXRAD ingest and rendering disabled via --disable-nexrad
```

That cycle completed in approximately **44.0 seconds** from worker start to
tandem completion, with 197 cells. Its CTAM timing was:

```text
CTAM total:        13.987 s
```

The total-cycle reduction versus the earlier enabled-NEXRAD cycle was about
53 seconds. This is not a perfectly controlled benchmark because the runs had
different cell counts (240 vs 197) and different live inputs, but it is strong
evidence that NEXRAD ingest/render was causing substantial CPU, RAM, and disk
I/O contention with CTAM/EWMRS.

## Current diagnosis / next priorities

1. NEXRAD currently starts a non-daemon ingest process using a
   `ProcessPoolExecutor` plus a separate render process. It competes with
   EWMRS rendering and CTAM's large array work.
3. Integration still performs expensive multi-field, per-cell statistics;
   AzShear low/mid are recurring slow fields.
4. A reasonable next design is to pause/cap NEXRAD ingest/render while a
   tandem cycle is active, then resume it afterward. Do not simply remove it
   from production without an explicit product decision.
5. For a true historical performance comparison, replay the same archived
   MRMS/RAP/GLM input set against both versions in their matching dependency
   environments. Live-data/version comparisons were invalid.

## Version 2.5.0 experiments (not comparable)

Two attempts were made at commit `e9ff473` (2.5.0): first using the default
Python environment, then using `EdgeWARN-dev`.

They are not usable benchmarks because 2.5.0's async AWS path conflicts with
the currently installed client stack:

```text
ClientArgsCreator.compute_endpoint_resolver_builtin_defaults()
missing required argument: s3_disable_express_session_auth
```

Other 2.5.0 run issues:

- Requested RAP files returned 404; fallback mixed older/latest files.
- The default Python environment lacked `cv2`; `EdgeWARN-dev` has OpenCV
  4.13.0 and fixes only that specific issue.
- The old EWMRS path produced RGB/RGBA broadcast errors for some layers:
  `could not broadcast input array from shape (...,3) into shape (...,4)`.
- Legacy background workers outlived the bounded parent command in some
  attempts. A process check was performed afterward; no benchmark process
  remained from the checked current-worktree command.

Relevant temporary logs:

```text
/tmp/edgewarn-250-output.log
/tmp/edgewarn-250-live-output.log
/tmp/edgewarn-250-normal-runtime.log
/tmp/edgewarn-250-conda-runtime.log
/tmp/edgewarn-phase-benchmark.log
/tmp/edgewarn-phase-benchmark-2.log
/tmp/edgewarn-no-nexrad.log
```

The detached temporary worktree remains at `/tmp/edgewarn-core-250`.

## Validation already run

Passed after the phase-release implementation:

```text
58 passed in 4.74s
```

That set covered tandem coordinator, EWMRS pipeline, GOES readiness, MRMS
ingest, I/O, and runtime tests.

After the later changes:

```text
python -m pytest tests/core/test_tandem_coordinator.py \
  tests/integration/test_tandem_coordinator.py -q
# 7 passed

python -m pytest tests/util/test_io.py tests/util/test_runtime.py -q
# 24 passed
```

## Cautions

- Several experimental commands used the normal runtime directory
  `~/EdgeWARN_input` at the user's explicit request. It now contains artifacts
  from live test runs, including older-version attempts.
- The log has shown stale RAP fallback use (for example a prior-hour RAP file)
  and occasional stale product reuse. This remains a correctness issue to
  resolve independently from timing.
- Do not claim a precise percentage speedup from the NEXRAD experiment: live
  inputs and cell count changed. Report the raw 97 s vs 44 s observation with
  this caveat.
