# Runtime Correctness and Race-Condition Remediation Plan

**Audit baseline:** commit `28beff7` on `version-test/3.0.0`  
**Package version:** `2.7.0`  
**Status:** planning only; no remediation in this document has been implemented  
**Scope:** runtime-triggerable correctness defects, races, publication hazards,
retry gaps, and operational failure modes in the Python pipelines and Node API
services.

## Objective

Make every real-time and historical cycle either publish one coherent,
validated set of artifacts or report a retryable failure. Prevent partial,
stale, cross-timestamp, or falsely successful work from becoming visible to
downstream processing or APIs.

The implementation must preserve:

- Public CLI flags and API response contracts unless a correction is explicitly
  documented.
- The configured runtime base directory as the sole production artifact root.
- Existing MRMS, GOES, RAP, NEXRAD, stormcell, cell, alert, and GUI schemas.
- Disabled-stage behavior such as `--disable-ewmrs`, `--disable-goes`,
  `--disable-nexrad`, and `--mrms-core-only`.
- Latest-first real-time operation without silently dropping failed scans.

## Double-check summary

Every issue below was checked again against the current tree. The second pass
confirmed the original findings and added two details:

1. MRMS batch readiness is reported as successful when gzip decompression
   returns `None`.
2. The coordinator's legacy/default `include_goes=True` path interprets a
   `None` returned after both async and sync failures as success. The current
   real-time tandem runner passes `include_goes=False`, so this affects other
   or future default callers rather than the primary real-time path.

Direct reproductions on the audit baseline confirmed:

- A required MRMS source can be empty while the scheduler still returns a
  "common" timestamp.
- An EdgeWARN worker that skips because required inputs are unavailable exits
  with process exit code `0`.
- Single-frame mode can make zero detector calls, republish a previous cell,
  and omit the new timestamp from `stormcell_index.json`.
- A decompression result of `None` still produces `(modifier, True)`.
- WPC keeps its imported default path after `util.file` is reconfigured.
- Expired `wpc_sfc_*.geojson` files survive the cleanup helper.
- RAP Uint16 conversion reports success when `0/N` layers succeed.
- The legacy/default GOES coordinator path treats its `None` terminal failure
  as ready.
- HTTPS fuzzy matching returns no match for timezone-aware cycle timestamps.

Existing regression suites remained green during the audit:

- Targeted Python tests: `59 passed`
- Node/Jest tests: `204 passed`

Those suites do not currently exercise the confirmed failure branches.

## Confirmed runtime findings

### Critical

| ID | Finding | Trigger and runtime impact | Evidence |
| --- | --- | --- | --- |
| C1 | Failed tandem cycles are marked processed before success is known. Worker failures and unavailable readiness can also exit with code `0`. | Any ingest, detection, integration, or render failure can permanently drop a scan. A genuine nonzero child exit also leaves `last_processed` advanced. | `src/run.py:200-215`; `src/util/runtime/cycle.py:266-280`; `src/EdgeWARN/pipeline.py:192-233`; `src/EWMRS/pipeline.py:1112-1128` |
| C2 | Scheduler intersection silently omits required sources whose timestamp set is empty. | One missing MRMS source can still produce a "common" timestamp from the remaining sources. C1 then prevents a later retry when the source arrives. | `src/EdgeWARN/schedule/scheduler.py:160-188,205-248` |

### High

| ID | Finding | Trigger and runtime impact | Evidence |
| --- | --- | --- | --- |
| H1 | Single-frame mode reuses prior stormcell features instead of detecting the available radar scan. | Startup/recovery with one frame and an existing stormcell snapshot republishes stale cells under a current timestamp. If any one of the new radar, ProbSevere, or precipitation-type inputs is absent, the available new radar is also ignored. | `src/EdgeWARN/process/detect/main.py:132-250` |
| H2 | Single-frame output returns before updating the stormcell API index. | A valid output file exists but `/api/v2/features/timestamps` does not advertise it. | `src/EdgeWARN/process/detect/main.py:238-250,369-386` |
| H3 | Readiness does not pin exact staged paths or analysis timestamps. Consumers independently choose latest-by-mtime files. | Concurrent/background ingest, restored files, or future cached files can mix different scan times across detection, integration, RAP, GLM, and EWMRS rendering. EdgeWARN integration ignores the RAP path selected by source ingestion and chooses latest RAP again. | `src/common/ingest/mrms/downloader.py:34-38`; `src/common/pipeline/coordinator.py:121-198`; `src/EdgeWARN/pipeline.py:84-103`; `src/EdgeWARN/process/integrate/pipeline.py:35-151`; `src/EWMRS/pipeline.py:210,285-289`; `src/util/file.py:211-256` |
| H4 | Downloads and decompression write final filenames directly; partial files remain eligible. MRMS ignores decompression failure. | Connection loss, cancellation, disk-full, or process death can leave truncated GRIB/gzip data that future cycles accept. A decompression returning `None` still marks the modifier successful. | `src/common/ingest/mrms/s3_async.py:128-156,221-252`; `src/common/ingest/mrms/https_client.py:240-266`; `src/common/ingest/synoptic/s3_async.py:31-56`; `src/common/ingest/synoptic/downloader.py:70-74,168-176`; `src/common/ingest/mrms/downloader.py:287-318` |
| H5 | Shared JSON, indexes, metadata, and binary artifacts are rewritten in place while independent API processes read them. | Readers can receive transient parse failures or partial binary responses. A crash can leave persistent corruption. Cell-history parse failure resets history to an empty list and the next write can destroy accumulated history. | `src/EdgeWARN/api_integration/index_manager.py:59-67,106-142`; `src/EdgeWARN/process/integrate/history.py:51-94`; `src/common/ingest/metar.py:428-445`; `src/EWMRS/render/render.py:281-319`; `src/EWMRS/rap/uint16_pipeline.py:62-96,259-280`; `src/EdgeWARN/api/utils/fileReader.js:101-119` |
| H6 | NEXRAD GUI completion is inferred from the existence of any matching final `.bin.gz`, which is written non-atomically. | Renderer death can leave a truncated gzip that is considered complete forever and is served by the API. | `src/EWMRS/render/nexrad.py:130-141`; `src/EWMRS/pipeline.py:503-510,544-560`; `src/EWMRS/api/routes/nexrad/index.js:86-117` |
| H7 | WPC modules capture default filesystem paths before runtime initialization. | `--base-dir` causes the configured API and WPC ingest to use different roots; WPC data is written under the default `~/EdgeWARN_input`. | `src/common/ingest/wpc/config.py:3-10`; `src/common/ingest/wpc/downloader.py:10-11`; `src/common/ingest/wpc/main.py:7`; `src/util/runtime/background.py:9-15`; `src/run.py:7-37` |
| H8 | Historical alert matching defaults to the newest snapshot when no snapshot exists at or before the target. | Reprocessing a time older than retained NWS history attaches current/future warnings to old storm cells. | `src/EdgeWARN/process/detect/tools/alert_matcher.py:66-101` |
| H9 | NEXRAD process-pool creation, recycling, and shutdown mutate global state without synchronization. | Concurrent site parser threads can create/orphan multiple pools, submit to a pool another thread is shutting down, or reset the volume counter incorrectly. | `src/common/ingest/nexrad/worker_pool.py:188-235`; `src/common/ingest/nexrad/service.py:676-693,739-769` |
| H10 | Historical processing advances its processed timestamp regardless of pipeline return or exception. | A failed historical minute is never retried. | `src/process_historical.py:75-106` |

### Medium

| ID | Finding | Trigger and runtime impact | Evidence |
| --- | --- | --- | --- |
| M1 | WPC fallback returns content without the actual fallback analysis time. | During WPC publication lag, previous-cycle content is stored and labeled as the requested/current analysis. | `src/common/ingest/wpc/downloader.py:63-133`; `src/common/ingest/wpc/main.py:37-74` |
| M2 | RAP Uint16 conversion returns success for `0/N` converted layers. | Missing GRIB messages or per-layer failures do not set `ewmrs_rap_uint16`; monitoring reports a completed conversion with no usable products. | `src/common/pipeline/coordinator.py:84-97` |
| M3 | WPC timestamped-file retention is unused and matches the wrong filename prefix. | `wpc_sfc_*.geojson` accumulates indefinitely. | `src/common/ingest/wpc/main.py:95-134`; `src/util/runtime/background.py:213-223` |
| M4 | Detection cleanup starts concurrently with download and is not awaited before worker release. | An existing/cached target with old mtime, or one removed by the max-file cap, can be reported staged and then deleted before the worker opens it. | `src/common/ingest/mrms/pipeline.py:39-70`; `src/common/ingest/mrms/main.py:61-83`; `src/util/file.py:290-345` |
| M5 | `multiprocessing.Queue.empty()` controls blocking reads and cycle termination. | Feeder timing can make the parent block in `get()`, terminate before delayed records arrive, or keep a cycle alive on stale queue state. | `src/util/runtime/logging.py:5-7`; `src/util/runtime/cycle.py:266-277` |
| M6 | HTTPS fuzzy timestamp matching subtracts a timezone-aware request from a naive filename datetime and swallows the exception. | Exact-minute misses cannot use the intended ±2 minute fallback during normal aware-UTC runtime. | `src/common/ingest/mrms/https_client.py:197-238` |
| M7 | Accessory child processes are started once and are not supervised by the parent. | OOM, native-library crash, or unexpected child exit permanently disables METAR, NWS, WPC, GOES, GOES render, or NEXRAD render until the complete scheduler restarts. Abrupt GOES death can also leave shared activity events set. | `src/run.py:109-151`; `src/util/runtime/processes.py:23-34` |
| M8 | The coordinator's legacy/default GOES result contract maps terminal `None` failure to success. | A caller using the default `include_goes=True` can release integration/GOES readiness after both async and sync ingestion throw. The primary real-time tandem runner currently avoids this path with `include_goes=False`. | `src/common/pipeline/coordinator.py:39-67,144-156,200-209`; `src/util/runtime/cycle.py:144-147` |

### Low

| ID | Finding | Trigger and runtime impact | Evidence |
| --- | --- | --- | --- |
| L1 | NWS temporary files are removed only on the success path. | Repeated download, parse, reconciliation, or registry-save failures leak files in the system temporary directory. | `src/common/ingest/nws/main.py:112-133,177-212` |
| L2 | EdgeWARN uses process-local default rate-limit stores in a four-worker cluster. | A client can receive approximately four times the configured service-wide rate, depending on cluster distribution. | `src/EdgeWARN/api/server.js:130-168,259-284` |
| L3 | EWMRS tile coordinates use permissive `parseInt`. | Inputs such as `0junk` are accepted as tile coordinate `0`, violating the documented integer contract. | `src/EWMRS/api/routes/renders.js:346-362` |
| L4 | WPC disables TLS certificate and hostname validation. | A network attacker or compromised proxy can alter operational WPC input without detection. | `src/common/ingest/wpc/downloader.py:77-82,124-129` |

## Remediation principles

1. **Success must be explicit.** Absence of an exception or a zero process exit
   code is insufficient when a worker can skip or catch its own failure.
2. **A cycle owns an immutable input manifest.** Every consumer must receive
   exact paths plus parsed analysis timestamps selected for that cycle.
3. **Final filenames are commit points.** Writers must use a sibling temporary
   file, validate it, flush it as appropriate, then atomically replace the
   destination.
4. **Indexes are published last.** APIs must never discover an artifact before
   all of its payload and metadata are complete.
5. **Cleanup cannot race active ownership.** Retention runs after consumers
   release their input manifest, or skips explicitly leased paths.
6. **Historical time is authoritative.** Historical processing may not borrow
   future inputs, alerts, or success state.
7. **Process and pool lifecycle is synchronized and observable.**

## Execution plan

### Phase 0 — Add failure-reproduction tests before implementation

- [ ] Add a scheduler test where one required modifier returns an empty set and
  the others share a timestamp; assert no timestamp is selected.
- [ ] Add cycle tests for unavailable readiness, caught worker exceptions,
  nonzero worker exits, and retry behavior.
- [ ] Add a single-frame test with an existing prior stormcell snapshot; assert
  the current radar detector runs and the API index is updated.
- [ ] Add an MRMS downloader test where download succeeds but decompression
  returns `None`; assert the batch fails.
- [ ] Add mid-stream disconnect tests for MRMS S3, MRMS HTTPS, synoptic/RAP, and
  gzip decompression; assert no final filename remains.
- [ ] Add historical tests for a pipeline returning no artifact, a thrown
  exception, and an alert target before the oldest retained snapshot.
- [ ] Add WPC tests for custom base directory, fallback analysis timestamp,
  cleanup naming/invocation, and certificate verification.
- [ ] Add concurrent NEXRAD pool create/submit/recycle/shutdown tests.
- [ ] Add atomic-publication tests with a reader loop for mutable JSON and
  binary artifacts.
- [ ] Add a default-coordinator GOES test where both ingestion paths throw;
  assert readiness remains false.

### Phase 1 — Make cycle outcome and retry semantics truthful

Files:

- `src/run.py`
- `src/util/runtime/cycle.py`
- `src/EdgeWARN/pipeline.py`
- `src/EWMRS/pipeline.py`
- `src/process_historical.py`

Tasks:

- [ ] Move `last_processed` advancement after a validated terminal cycle
  result.
- [ ] Define a structured `CycleOutcome` containing stage status, produced
  artifacts, retryability, errors, and worker exit status.
- [ ] Make workers return or publish explicit `completed`, `disabled`,
  `unavailable`, and `failed` states.
- [ ] Allow caught worker exceptions to produce a non-success process result or
  an authoritative shared-state failure consumed by the parent.
- [ ] Treat partial rendering as failure when the configured required set is
  not produced; define optional-layer behavior separately.
- [ ] Add bounded retry/backoff for the same scan. Do not advance to a newer
  timestamp until policy explicitly abandons and records the failed scan.
- [ ] Persist enough failure state for restart observability without treating a
  failed timestamp as successfully processed.
- [ ] Apply the same success rule to historical processing; only advance after
  its requested output is validated.

Acceptance:

- A failed or unavailable scan is retried.
- Disabled stages do not make an otherwise valid cycle fail.
- Every "finished" log corresponds to validated required outputs.
- A process restart can distinguish last successful from last attempted scan.

### Phase 2 — Pin a coherent per-cycle input manifest

Files:

- `src/common/pipeline/coordinator.py`
- `src/common/ingest/mrms/downloader.py`
- `src/EdgeWARN/pipeline.py`
- `src/EdgeWARN/process/integrate/pipeline.py`
- `src/EWMRS/pipeline.py`
- relevant GOES/RAP readiness helpers

Tasks:

- [ ] Replace label-only `DownloadBatchResult.downloaded` data with structured
  staged records containing product, exact path, parsed analysis timestamp,
  source, and validation status.
- [ ] Build one immutable `CycleInputManifest` and validate required timestamp
  alignment before setting readiness events.
- [ ] Pass pinned detection paths directly to detection; never rescan the
  directories at worker execution time.
- [ ] Pass pinned integration MRMS, ProbSevere, GLM, and RAP paths directly to
  integration.
- [ ] Pass pinned MRMS and GOES paths directly to EWMRS render functions.
- [ ] Reject cross-timestamp inputs outside an explicitly documented tolerance.
- [ ] Stop using filesystem mtime as analysis freshness. Keep mtime only for
  cache/retention mechanics where appropriate.
- [ ] Return explicit GOES success/failure from `_safe_ingest`; remove the
  `None is not False` compatibility rule.

Acceptance:

- Logs and artifact metadata identify every exact input used by a cycle.
- Adding or touching a newer file after readiness cannot change that cycle's
  inputs.
- RAP conversion and EdgeWARN RAP integration consume the same selected GRIB.

### Phase 3 — Make ingest and publication transactional

Files:

- `src/common/ingest/mrms/s3_async.py`
- `src/common/ingest/mrms/https_client.py`
- `src/common/ingest/synoptic/s3_async.py`
- `src/common/ingest/mrms/downloader.py`
- `src/EdgeWARN/process/detect/main.py`
- `src/EdgeWARN/api_integration/index_manager.py`
- `src/EdgeWARN/process/integrate/history.py`
- `src/EdgeWARN/alerts/manager.py`
- `src/common/ingest/metar.py`
- `src/common/ingest/wpc/converter.py`
- `src/EWMRS/render/render.py`
- `src/EWMRS/render/nexrad.py`
- `src/EWMRS/rap/uint16_pipeline.py`

Tasks:

- [ ] Add shared atomic write helpers that create sibling temporary files,
  flush/close, optionally `fsync`, and call `os.replace`.
- [ ] Stream downloads to `.part` files and validate content length where the
  source provides it.
- [ ] Decompress to a temporary destination and verify gzip EOF/integrity before
  replacing the uncompressed target.
- [ ] Delete/quarantine stale `.part` files safely on startup and retry.
- [ ] Return decompressed output paths from MRMS staging and fail the product
  when decompression returns `None`.
- [ ] Add lightweight format validation before readiness: gzip integrity,
  nontrivial GRIB structure/size, JSON parse, NEXRAD magic/count/length, and RAP
  Uint16 expected byte length.
- [ ] Atomically publish mutable JSON and indexes.
- [ ] For multi-file artifacts, publish payload and metadata first, then update
  the index as the final commit.
- [ ] Make NEXRAD completion validate the full required product set, not one
  matching filename.
- [ ] Preserve an existing valid artifact if replacement fails.

Acceptance:

- Killing a writer at any byte offset leaves either the old complete artifact
  or no advertised new artifact.
- APIs never parse partial JSON or serve advertised partial binary data.
- A failed decompression cannot produce a ready batch.

### Phase 4 — Correct detection and historical semantics

Files:

- `src/EdgeWARN/process/detect/main.py`
- `src/EdgeWARN/api_integration/index_manager.py`
- `src/EdgeWARN/process/detect/tools/alert_matcher.py`
- `src/process_historical.py`
- `src/EdgeWARN/pipeline.py`

Tasks:

- [ ] Redefine single-frame mode so the available radar scan is always
  detected. Previous stormcells are history/vector context only.
- [ ] Do not make a missing new ProbSevere or precipitation-type file cause a
  newer valid radar file to be discarded; define optional/degraded behavior
  explicitly.
- [ ] Route single- and dual-frame persistence through one save-and-index helper.
- [ ] Return a structured detection result that distinguishes no cells,
  unavailable input, invalid input, and successful artifact creation.
- [ ] Return no active alerts when no snapshot exists at or before the
  historical target.
- [ ] Validate historical outputs per processed timestamp rather than checking
  one generic path that may have existed from a previous iteration.
- [ ] Record failed historical timestamps separately and support bounded retry.

Acceptance:

- Single-frame output reflects the current scan.
- Every saved stormcell timestamp is either indexed or the complete save is
  rolled back.
- Historical output contains no future-derived alerts or inputs.

### Phase 5 — Synchronize NEXRAD lifecycle and repair WPC ownership

#### NEXRAD

- [ ] Protect `_POOL`, `_POOL_SIZE`, and `_VOLUME_COUNT` with one lifecycle
  lock.
- [ ] Introduce pool generations so a timeout only retires the generation that
  experienced the failure.
- [ ] Prevent new submissions once retirement starts; wait for or explicitly
  account for in-flight submissions.
- [ ] Make pool shutdown idempotent and safe under concurrent timeout/recycle
  calls.
- [ ] Atomically publish each GUI gzip and validate it before completion checks.
- [ ] Require all configured/advertised variables for a timestamp before
  marking an elevation rendered.

#### WPC

- [ ] Replace imported path constants with runtime lookups through
  `import util.file as fs`.
- [ ] Return `(content, actual_analysis_time)` from the WPC downloader and use
  the actual time for GeoJSON metadata and timestamped filenames.
- [ ] Restore normal TLS certificate and hostname validation.
- [ ] Call retention from `wpc_loop` after successful publication.
- [ ] Match the actual `wpc_sfc_*.geojson` naming scheme and retain
  `latest.geojson`.
- [ ] Publish `latest.geojson` and timestamped files atomically.

Acceptance:

- Custom base-directory runs place all WPC artifacts under that base.
- WPC fallback content is labeled with its real analysis time.
- Concurrent NEXRAD timeouts/recycling do not break unrelated site submissions.
- Partial NEXRAD GUI artifacts are retried, not advertised.

### Phase 6 — Operational hardening

- [ ] Replace queue `empty()` loops with nonblocking `get_nowait()` draining
  that terminates on `queue.Empty`; track worker liveness independently.
- [ ] Add a parent supervisor for accessory processes with bounded restart,
  backoff, crash-loop detection, and event cleanup.
- [ ] Put NWS temporary-file removal in `finally` blocks.
- [ ] Decide whether API limits are per-worker or service-wide. If service-wide,
  use a shared store; otherwise expose/document effective clustered limits.
- [ ] Require exact integer strings for EWMRS tile coordinates.
- [ ] Emit health/readiness state for every accessory process and most recent
  successful product timestamp.

## Regression and fault-injection matrix

### Python

Run in the `EdgeWARN-dev` environment:

```bash
python -m pytest tests/core/schedule/
python -m pytest tests/core/test_tandem_coordinator.py tests/integration/test_tandem_coordinator.py
python -m pytest tests/core/process/detect/
python -m pytest tests/core/ingest/
python -m pytest tests/integration/
python -m pytest tests/
```

Required fault cases:

- Missing one required source while all others agree.
- Mid-stream S3/HTTPS disconnect after at least one chunk.
- Gzip failure after partial output.
- Permission denied and disk full during payload and index publication.
- Worker exception before and after readiness release.
- Process death during JSON, RAP binary, and NEXRAD gzip publication.
- File arrival/touch after manifest readiness but before worker consumption.
- Concurrent NEXRAD pool creation, timeout, recycle threshold, and shutdown.
- Historical target older than all retained alert snapshots.
- Custom runtime base directory with WPC enabled.

### Node

```bash
npm test -- --runInBand
```

Add:

- Repeated API reads while Python atomically replaces indexes and mutable JSON.
- No partial RAP/NEXRAD downloads while publication is in progress.
- Clustered rate-limit behavior at the documented effective limit.
- Rejection of `0junk`, whitespace, exponent, decimal, and repeated tile
  coordinate values.

### Operational smoke

1. Run at least five warm real-time cycles with all components enabled.
2. Inject one retryable failure in each staged source.
3. Confirm the failed scan is retried and later committed once.
4. Confirm one coherent input manifest per committed scan.
5. Kill and restart a writer during each artifact family.
6. Verify API reads remain valid throughout.
7. Confirm cleanup never removes a path leased by an active cycle.
8. Confirm accessory-process restart and crash-loop telemetry.
9. Compare generated schemas and public API envelopes with the baseline.

## Rollout order

1. Phase 0 tests.
2. C1/C2 cycle truth and retry changes.
3. H3 pinned input manifest.
4. H4/H5/H6 transactional ingest/publication.
5. H1/H2/H8/H10 detection and historical corrections.
6. H9 NEXRAD lifecycle and H7/M1/M3/L4 WPC corrections.
7. Remaining medium/low operational hardening.

Do not implement early scheduler retry without transactional publication and
idempotent output handling; otherwise retries can amplify partial-write and
duplicate-publication hazards. It is acceptable to land the work in multiple
PRs, but each PR must preserve a truthful terminal cycle result and add its
failure-path tests before changing production behavior.

## Completion criteria

- [ ] No failed cycle advances the last successful timestamp.
- [ ] All required scheduler sources participate in timestamp intersection.
- [ ] Every cycle consumes one pinned, timestamp-validated input manifest.
- [ ] No final artifact path is visible before validation and atomic commit.
- [ ] Single-frame and historical outputs represent their requested times.
- [ ] WPC honors the configured base directory and real fallback timestamp.
- [ ] NEXRAD pool and GUI publication are concurrency-safe.
- [ ] Accessory process failures are detected and recovered or surfaced.
- [ ] Full Python and Node suites pass.
- [ ] Fault-injection and five-cycle operational smoke criteria pass.
