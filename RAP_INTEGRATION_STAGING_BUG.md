# Bug: EdgeWARN integration is skipped when both current and previous RAP cycles are unavailable

## Summary

EdgeWARN's real-time pipeline cannot stage integration inputs when the requested
RAP cycle and the immediately preceding RAP cycle both return HTTP 404 from the
NOAA RAP S3 bucket. The synoptic downloader only checks those two hours and then
returns `None`. Because RAP is a mandatory integration prerequisite, the
coordinator marks integration unavailable and skips the entire cell-integration
phase even though MRMS and GLM inputs staged successfully.

This is especially visible after startup with a fresh runtime directory, because
there is no previously downloaded RAP file that can satisfy the request locally.

## Environment

- EdgeWARN: v2.7.0
- Branch observed in the source checkout: `version-test/3.0.0`
- Runtime: real-time tandem pipeline
- Observation time: 2026-07-26 13:06-13:09 UTC
- Affected cycles: 13:02, 13:04, and 13:06 UTC
- Source log: `ewrn.2026-07-26_13.log`

## Observed behavior

For the 13:02 UTC cycle, the RAP downloader tried:

```text
[2026-07-26T13:06:15.031130+00:00] [DataIngestion] INFO: Attempting RAP download: s3://noaa-rap-pds/rap.20260726/rap.t13z.awp130pgrbf00.grib2
[2026-07-26T13:06:15.246197+00:00] [DataIngestion] WARN: Synoptic file not found on S3 (404): s3://noaa-rap-pds/rap.20260726/rap.t13z.awp130pgrbf00.grib2
[2026-07-26T13:06:15.246476+00:00] [DataIngestion] INFO: Attempting RAP fallback to previous hour: 2026-07-26 12:02:00+00:00 (s3://noaa-rap-pds/rap.20260726/rap.t12z.awp130pgrbf00.grib2)
[2026-07-26T13:06:15.432626+00:00] [DataIngestion] WARN: Synoptic file not found on S3 (404): s3://noaa-rap-pds/rap.20260726/rap.t12z.awp130pgrbf00.grib2
```

The coordinator subsequently reported:

```text
[2026-07-26T13:06:20.090137+00:00] [PhaseTelemetry] ... phase=integration_released status=unavailable
[2026-07-26T13:06:32.146883+00:00] WARN: Async RAP ingestion failed: RAP ingestion did not return a staged file path. Falling back to sync.
[2026-07-26T13:06:32.146931+00:00] ERROR: Both async and sync ingestion failed for RAP: RAP sync fallback did not return a staged file path
[2026-07-26T13:06:32.149391+00:00] ERROR: EdgeWARN integration inputs were not staged successfully; skipping integration
```

The same sequence recurred for the 13:04 cycle and RAP was again unavailable
for the 13:06 cycle.

Other integration inputs were healthy:

```text
INFO: Local GLM readiness satisfied by <base-dir>/data/GLM/OR_GLM-L2-LCFA_merged_20260726-130200.nc
INFO: Async MRMS Integration ingestion successful
INFO: GOES ingest is decoupled from this cycle; integration readiness does not wait for GOES availability
```

## Expected behavior

A short publication delay or isolated missing RAP cycle should not disable the
entire EdgeWARN integration phase when a sufficiently recent RAP analysis is
available. The pipeline should search backward within an explicit and
configurable staleness limit, use the newest acceptable local or remote RAP
cycle, and log the selected cycle and its age.

If no RAP cycle satisfies the staleness policy, the pipeline should still
report that condition clearly as a RAP availability failure, including every
hour checked and the configured age limit.

## Steps to reproduce

1. Start the real-time tandem pipeline with RAP integration enabled and an empty
   RAP cache.
2. Arrange for both the target-hour and previous-hour RAP S3 objects to be
   unavailable or return 404.
3. Allow MRMS and GLM ingestion to complete successfully.
4. Observe that `download_synoptic()` returns `None`.
5. Observe `integration_released status=unavailable` and
   `EdgeWARN integration inputs were not staged successfully; skipping integration`.

## Technical analysis

The failure follows directly from the current readiness path:

1. `src/common/ingest/synoptic/downloader.py::download_synoptic()` iterates over
   only `[dt, dt - timedelta(hours=1)]`.
2. When both objects return 404, it returns `None`.
3. `src/common/pipeline/coordinator.py::_safe_ingest()` is called with
   `require_result=True`, so a false result becomes a failed RAP ingest.
4. `src/util/runtime/cycle.py` requires `cycle_state.rap_inputs_ready` unless
   `mrms_core_only` is enabled.
5. `src/EdgeWARN/pipeline.py` skips integration after the terminal unavailable
   state is published.

The current RAP cache cleanup policy also uses `RAP_MAX_AGE_MINUTES = 90`.
Fallback behavior and cache retention should share one staleness policy so the
downloader does not search for files that cleanup rejects, or reject files the
downloader considers valid.

The runtime used `<base-dir>/opdata`, while older successful logs used
`<base-dir>/op-data`. That path change may be deployment-specific, but it
removed any effective local-cache fallback during this startup and should be
verified separately.

## Proposed fix

- Replace the fixed two-element hour list with a bounded backward search driven
  by a single RAP maximum-age setting.
- Check a matching local RAP file before each network request.
- Stop at the first valid file and log its analysis time and age.
- Keep the maximum-age policy consistent with RAP cache cleanup.
- Record attempted S3 keys and distinguish `404/no acceptable RAP cycle` from
  transport, authentication, and parsing failures.
- Avoid repeating the same complete async-plus-sync search at the coordinator
  level after the downloader has already exhausted its source-level fallbacks,
  unless that retry has a defined delay or materially different behavior.

## Suggested regression tests

1. Current hour missing, previous hour available: previous hour is staged.
2. Current and previous hours missing, second previous hour within the allowed
   age: second previous hour is staged.
3. Candidate exists but exceeds maximum age: integration remains unavailable
   with a clear staleness error.
4. Fresh cache and transient S3 404s: all permitted hours are checked exactly
   once per ingest attempt.
5. Month/day rollover: fallback constructs the correct S3 directory and key.
6. A valid local fallback avoids a network download.
7. MRMS and GLM success with RAP failure: readiness error identifies RAP as the
   sole missing prerequisite.

## Impact

- Cell integration, CTAM inputs that depend on integrated cells, and downstream
  products are skipped for every affected scan.
- Detection and EWMRS rendering continue, which can make the system appear
  mostly healthy while integrated products silently stop updating.
- The failure repeats on each scheduler cycle until one of the two checked RAP
  hours becomes available or a valid file appears in the active runtime cache.
