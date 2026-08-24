# Real-Time Runner Decomposition Plan

**Audit baseline:** commit `8a6206d` ("MRG: Merge pull request #93
from ewsofficial/yuchen-wei3667/configuration-extraction-yaml") on
`version-test/3.0.0`. This is a re-audit; the original audit was taken at
commit `28beff7`, before configuration extraction and the truthful-cycle-state
work landed.  
**Package version:** `2.7.0` on branch `version-test/3.0.0`  
**Status:** planning only; this document does not implement the split

## Objective

Replace the current all-in-one `src/run.py` runtime with three independently
operable services:

1. A primary EdgeWARN service that owns MRMS selection/ingest and the
   detection, integration, tracking, CTAM, alert, and API-index cycle.
2. An EWMRS/accessory service that owns MRMS/GOES/RAP rendering and the
   continuous GOES ABI, METAR, NWS, and WPC ingests.
3. A NEXRAD service that owns both Level-II ingest and NEXRAD rendering.

Also provide a thin all-services launcher only if measured performance shows
that launching the three services beneath one supervisor does not materially
degrade primary-cycle latency, EWMRS/NEXRAD freshness, CPU, memory, or I/O.

The split must reduce lifecycle coupling and import/process complexity without
duplicating source downloads, changing scientific outputs, or replacing one
in-process race with an unreliable cross-process race.

## Current-state evidence (re-audited at `8a6206d`)

`src/run.py` currently owns all of the following:

- MRMS timestamp discovery (`latest_common_minute_1h` plus the HTTPS fallback)
  and the forever polling loop.
- A lifetime `multiprocessing.Manager`.
- Per-cycle EdgeWARN and EWMRS workers and their readiness events/state via
  `run_tandem_cycle_once()`.
- Per-cycle MRMS, RAP, and scan-time GLM ingestion through
  `common.pipeline.coordinator.run_tandem_ingest_cycle()`.
- Persistent GOES ABI ingest and render processes, queues, and activity events,
  registered on an in-process `AccessorySupervisor`.
- Persistent METAR, NWS, and WPC processes, also on `AccessorySupervisor`.
- Persistent NEXRAD ingest and render processes plus a NEXRAD log queue and an
  atomic JSON heartbeat with staleness-based restarts.
- Shutdown of every child and queue via `StartedProcessRegistry`.

Since the original audit, significant prerequisite work has landed inside this
monolith. The plan must preserve it, not redo it:

- **Truthful durable cycle state exists.** `src/util/runtime/cycle.py`
  defines `CycleOutcome`, `CycleStageResult`, `CycleStatus`,
  `CycleRetryPolicy` (bounded exponential backoff), `PersistedCycleState`, and
  an atomically written `CycleStateStore`. `run.py` advances
  `last_successful`/`selection_cursor` only after `run_tandem_cycle_once()`
  returns a validated outcome, distinguishes attempted/successful/abandoned
  scans across restarts, seeds from the stormcell watermark only when no
  authoritative state exists, and persists pending retries with backoff.
- **Exact-path input manifests exist.** `common/ingest/manifest.py`
  (`CycleInputManifest`, `StagedInput`) is built by the shared coordinator,
  alignment-validated, passed to both workers, consumed by detection input
  preparation in `src/EdgeWARN/pipeline.py`, and honored by the EWMRS MRMS and
  GOES render pipelines, which pin layers via
  `CycleInputManifest.latest_for_directory()` (`input_manifest_bound`). The
  remaining gap is that `_render_layer()` still falls back to
  latest-by-mtime when no manifest is supplied, so exact-path selection is not
  yet mandatory.
- **Bounded child supervision exists in-process.** `AccessorySupervisor` in
  `src/util/runtime/processes.py` implements bounded exponential restart
  backoff, crash-loop disabling after `max_restarts` within a window, atomic
  health-file publication, cleanup-event clearing on death, and heartbeat
  staleness checks for the non-daemonic NEXRAD ingest child.
- **Configuration is YAML-first.** `src/common/config/` (loader, overlay,
  validator) reads catalogs from a configurable `config/` root
  (`--config-dir` / `EDGEWARN_CONFIG_DIR`); `util/runtime/config.py` exposes
  `section()`/`resolve_file()`; disable flags are
  `argparse.BooleanOptionalAction` flags whose defaults resolve from
  `runtime.yaml` with environment overrides via `overlay.resolve`.

The process boundary is still not confined to `run.py`.
`src/util/runtime/cycle.py` imports both `EdgeWARN.pipeline.edgewarn_tandem_worker`
and `EWMRS.pipeline.ewmrs_tandem_worker` at module load, while
`src/util/runtime/background.py` imports GOES, METAR/NWS/WPC, NEXRAD, and EWMRS
implementations, and `src/util/runtime/__init__.py` re-exports all of them.
Copying the entrypoint code into three files would therefore preserve much of
the current coupling.

The existing shared staged-ingest flow must also be handled deliberately:

- Detection MRMS is released before integration MRMS and RAP complete.
- EWMRS MRMS rendering waits for validated detection and integration MRMS
  groups (`ewmrs_mrms_inputs_ready`).
- Primary integration requires integration MRMS and raw RAP, plus scan-time
  GLM when GOES is enabled; GLM readiness is merged into the manifest before
  integration release.
- GOES ABI is an EWMRS render input ingested by the background
  `goes_loop`; it is not a substitute for scan-time GLM. A
  `pause_ingest_during_render` coordination option (YAML/env) pauses ABI
  ingest while a GOES render is active, using in-process events.
- RAP Uint16 conversion remains an EWMRS-derived artifact executed inside the
  shared ingest coordinator (`_run_rap_uint16_conversion` in
  `src/common/pipeline/coordinator.py`).
- EWMRS GUI cleanup still reaches NEXRAD outputs:
  `cleanup_old_gui_files()` calls `_cleanup_old_nexrad_gui_files()` in
  `src/EWMRS/pipeline.py`. After the split, only the NEXRAD service may clean
  NEXRAD artifacts.

What has *not* changed since the original audit: `run.py` still parses
arguments, calls `initialize_runtime()`, resolves coordination settings, and
reassigns `sys.stdout`/`sys.stderr` as import side effects (module scope runs
before `main()`).

## Requirements and non-negotiable behavior

- Each of the three services must be directly runnable and useful without a
  common Python parent.
- Starting or restarting EWMRS or NEXRAD must not restart or interrupt an
  active primary EdgeWARN cycle.
- The primary service must be able to run with EWMRS and NEXRAD stopped.
- EWMRS must consume MRMS/RAP files staged by the primary service; it must not
  download a second copy of MRMS or RAP.
- Scan-time GLM stays with the primary integration service. GOES ABI stays with
  EWMRS.
- NEXRAD ingest and rendering move together to the NEXRAD service. EWMRS must
  neither launch nor clean NEXRAD work.
- Cross-service readiness must be durable, atomic, timestamp-specific,
  restart-safe, and located beneath the configured runtime base directory.
- A failed phase must never publish a successful ready record.
- Consumers must use the exact paths in a committed ready record, not
  latest-by-mtime discovery.
- Existing scientific schemas, API contracts, source selection, fallbacks,
  retention policies, and base-directory overrides remain unchanged.
- Preserve the truthful cycle-state semantics already implemented:
  `last_successful` advances only after a validated `CycleOutcome`, and
  attempted/successful/abandoned state survives restarts via
  `CycleStateStore`.
- Preserve the exact-path input-manifest plumbing already implemented; make
  manifest-bound selection mandatory rather than falling back to
  latest-by-mtime when no manifest is supplied.
- Existing disable flags retain their meaning when passed to the optional
  all-services launcher. Flags now default from `runtime.yaml` through the
  config overlay (`argparse.BooleanOptionalAction` plus `overlay.resolve`);
  CLI values must continue to win over YAML/env layers.
- No entrypoint may parse arguments, initialize runtime paths, or spawn
  processes as an import side effect. (`run.py` currently violates this at
  module scope; the split must not carry it forward.)
- Every service must handle `SIGINT` and `SIGTERM`, close its own children, and
  leave no orphaned worker or stale permanent pause.
- Each service is published under one canonical name from a single registry,
  and the unified Node API must expose whether that named service is active;
  requests that depend on an inactive service fail with a structured
  `SERVICE_NOT_ENABLED` error rather than serving stale artifacts silently.

## Target topology

```text
                         configured <BASE_DIR>
                                  |
          +-----------------------+-----------------------+
          |                       |                       |
          v                       v                       v
  run_edgewarn.py          run_ewmrs.py           run_nexrad.py
  ----------------         ------------           --------------
  MRMS timestamp poll      cycle-record poll       Level-II ingest
  MRMS ingest (one owner)  MRMS render             NEXRAD render
  raw RAP ingest           RAP Uint16
  scan-time GLM            GOES ABI ingest/render
  detect/integrate/CTAM    METAR/NWS/WPC
          |                       ^
          | atomic phase records |
          +-----------------------+

  run_all.py / eventual run.py
  -----------------------------------------------
  optional thin subprocess supervisor only;
  performs no ingest, scientific work, or rendering
```

The runtime filesystem is the data plane and the coordination plane. The
launcher, when enabled, is not part of the readiness protocol.

## Entrypoints and ownership

### `src/run_edgewarn.py`

This is the latency-sensitive primary service.

It owns:

- `MRMSUpdateChecker` and the S3/HTTPS selection policy.
- Detection and integration MRMS downloads, exactly once per cycle.
- Raw RAP download required by integration.
- Scan-time GLM download and validation when GOES is enabled.
- Detection, tracking/lineage, integration, CTAM, alert generation, and
  EdgeWARN API index/snapshot updates.
- Primary cycle outcome, retry, and last-successful-cycle state.
- Publication of immutable MRMS-ready and RAP-ready records for EWMRS.
- A short-lived primary-activity lease if cooperative NEXRAD throttling is
  enabled.

It does not import or start EWMRS, NEXRAD, METAR, NWS, WPC, or GOES ABI loops.
It may retain an EdgeWARN child worker if staged detection/integration overlap
still provides a verified latency benefit, but that worker is private to this
service.

Advancing `last_successful` already occurs only after a truthful
`CycleOutcome` via `CycleStateStore` (re-audit finding); the split must carry
`CycleStateStore`, `CycleRetryPolicy`, `PersistedCycleState`, and the
selection-cursor logic into this service without regressing those semantics.
The in-process `AccessorySupervisor` restart/health machinery should be
generalized (see refactoring boundaries) so the primary can supervise its own
private worker with the same bounded-backoff behavior.

### `src/run_ewmrs.py`

This service owns EWMRS and non-NEXRAD accessory work.

It owns:

- Consumption of committed MRMS-ready records.
- MRMS layer rendering from the exact source paths in each record.
- GOES ABI polling, local readiness checks, and GOES rendering.
- Consumption of RAP-ready records and RAP Uint16 conversion.
- METAR, NWS, and WPC continuous ingest loops.
- EWMRS MRMS, GOES, RAP, and general GUI retention/index maintenance.
- Its own durable consumer checkpoints and retry/dead-letter state.

It does not perform MRMS timestamp discovery or MRMS/RAP downloads. It does not
perform scan-time GLM integration work. It does not import, start, render, or
clean NEXRAD.

METAR and NWS are accessory inputs that the primary integration code may read.
Their writers must publish atomically, and primary monitoring must report their
age. Stopping EWMRS therefore degrades those optional inputs visibly without
blocking MRMS detection.

### `src/run_nexrad.py`

This service owns the complete NEXRAD lifecycle:

- `run_realtime_ingestion_pipeline`.
- The NEXRAD parser process pool and its shutdown.
- NEXRAD GUI rendering.
- NEXRAD manifests, GUI indexes, retention, and cleanup.
- NEXRAD-specific concurrency, freshness, restart, and backlog telemetry.

The NEXRAD ingest child must remain non-daemonic because it creates parser
workers. The NEXRAD parent supervises ingest and render independently with
bounded restart backoff. If one component fails, the other may continue when
safe, and health must show the degraded component.

### Optional `src/run_all.py` and compatibility `src/run.py`

Build `run_all.py` only as a thin candidate supervisor. It starts the three
entrypoints with `subprocess.Popen` using explicit argument lists and the
current Python executable. It must:

- Inherit stdout/stderr or use direct per-service log destinations. It must not
  pipe all output through the parent and create a logging bottleneck.
- Forward `SIGINT`/`SIGTERM`, wait a bounded interval, then terminate only its
  known children.
- Exit nonzero if a required child exits unexpectedly.
- Avoid importing any pipeline module.
- Avoid creating a `multiprocessing.Manager`, queues, readiness events, worker
  pools, or runtime artifacts.
- Pass the same base directory to every child.
- Support selecting a subset, for example
  `--services edgewarn,ewmrs,nexrad`.

After the performance gate passes, make `src/run.py` a compatibility wrapper
for `run_all.py` for one deprecation window. If the gate fails, the supported
production deployment remains the three direct commands and `run_all.py` stays
experimental until its measured regression is removed. Do not preserve the
old monolithic implementation as a hidden fallback.

## Source and artifact ownership matrix

| Source/artifact family | Single writer/owner | Consumers |
| --- | --- | --- |
| MRMS staged source files | Primary | Primary, EWMRS |
| Raw RAP | Primary | Primary, EWMRS |
| Scan-time GOES GLM | Primary | Primary integration |
| GOES ABI channels | EWMRS | EWMRS rendering |
| Stormcells, cells, CTAM, alerts | Primary | APIs and downstream clients |
| EWMRS MRMS/GOES/RAP GUI artifacts | EWMRS | EWMRS API |
| METAR and NWS snapshots | EWMRS | Primary integration and APIs |
| WPC artifacts | EWMRS | EWMRS API |
| NEXRAD source, manifests, and GUI | NEXRAD | EWMRS API routes only |
| Cross-service phase records | Primary | EWMRS |
| EWMRS consumer checkpoints | EWMRS | EWMRS |
| NEXRAD consumer/checkpoint state | NEXRAD | NEXRAD |
| Service heartbeats (`services/<name>.json`) | each service, own name only | Unified API, operators |

This matrix is an enforcement target. Tests should fail if EWMRS invokes an
MRMS/RAP downloader or if non-NEXRAD cleanup removes NEXRAD files.

## Durable handoff protocol

Use phase-specific immutable records rather than attempting to share
`multiprocessing.Event`, `Manager.dict`, or queues across independent
entrypoints.

Suggested layout:

```text
<BASE_DIR>/
└── state/
    └── realtime/
        ├── cycles/
        │   └── <cycle-id>/
        │       ├── mrms-ready.json
        │       ├── rap-ready.json
        │       └── primary-outcome.json
        ├── consumers/
        │   └── ewmrs.json
        ├── services/
        │   ├── edgewarn.json
        │   ├── ewmrs.json
        │   └── nexrad.json
        └── leases/
            └── primary-active.json
```

The filenames under `services/` are the canonical service-name registry:
`edgewarn`, `ewmrs`, `nexrad`. The same names are used for single-instance
locks, heartbeats, lease ownership, and API discovery. Accessory loops
(METAR, NWS, WPC, GOES ABI) are not top-level services; their status appears
as child entries inside the EWMRS heartbeat rather than as separate files.
Each service publishes an atomic heartbeat containing PID, run ID, version,
last successful activity, current phase, and degraded children (see the
lifecycle section).

`<cycle-id>` must be a canonical UTC analysis timestamp, not process start
time. Each phase record should contain:

- Schema version and unique cycle ID.
- Requested analysis timestamp and publication timestamp.
- Phase name and explicit successful status.
- Exact committed paths for every required file.
- Parsed timestamp, product/modifier identity, size, and optional checksum for
  each path.
- Required-product list and validation result.
- Producing service version and run ID.
- Any non-fatal warnings that consumers need to surface.

Publication rules:

1. Write to a sibling temporary file.
2. Validate the complete payload.
3. Flush and atomically replace the final phase filename.
4. Treat the final filename as the only commit point.
5. Never overwrite an immutable successful phase record with failure state.
6. Put retries/failures in `primary-outcome.json` or a separate attempt log.

`mrms-ready.json` is published as soon as all EWMRS-required detection and
integration MRMS products are validated. It must not wait for detection,
integration, CTAM, ABI, or RAP Uint16 completion. `rap-ready.json` is published
independently after the raw RAP file is validated. This preserves staged
parallelism while removing in-memory barriers.

The EWMRS service records success only after all configured required render
artifacts for the phase are committed. Its checkpoint update is atomic and
occurs after artifact/index publication. A restart resumes unacknowledged
records idempotently.

Process ready records in timestamp order while their exact source files remain
retained. If EWMRS falls outside the source-retention window, record each
unrecoverable cycle explicitly and resume at the oldest still-valid record;
do not silently render a newer file under an older cycle timestamp. Backlog
policy and retention must be sized together from measured render throughput.

## Refactoring boundaries

Suggested modules (new unless noted):

- `src/util/runtime/primary_service.py` for timestamp polling and primary-cycle
  orchestration, absorbing the polling/retry loop currently inline in
  `run.py`'s `main()`.
- `src/util/runtime/ewmrs_service.py` for phase consumption and accessory
  supervision.
- `src/util/runtime/nexrad_service.py` for NEXRAD supervision.
- `src/util/runtime/handoff.py` for schemas, atomic publication, discovery,
  validation, leases, and checkpoints. Reuse the atomic-write pattern already
  proven in `CycleStateStore`, `_record_health()`, and the NEXRAD heartbeat
  writer.
- `src/util/runtime/cli.py` for reusable common flags and service-specific
  parsers. The existing monolithic parser lives in `util/io.py`
  (`IOManager.get_args`) and resolves YAML defaults via the config overlay;
  parser builders must preserve that overlay behavior per service.
- Rename/generalize the existing `src/util/runtime/processes.py`
  (`AccessorySupervisor`, `StartedProcessRegistry`) into a shared supervisor
  module so every service parent gets the bounded-backoff, crash-loop, health,
  and heartbeat-staleness behavior already implemented there.

Split `src/util/runtime/cycle.py` so the primary path no longer imports
`ewmrs_tandem_worker`. Split `src/util/runtime/background.py` by service so
importing primary runtime code does not import EWMRS or NEXRAD implementations;
today `src/util/runtime/__init__.py` re-exports every background loop, which
forces the full import graph on every consumer. Keep narrow compatibility
re-exports only during migration and remove them after all callers and tests
use the new modules.

Exact-input render variants now exist (`run_render_pipeline`,
`run_mrms_render_pipeline`, and `run_goes_render_pipeline` all accept an
`input_manifest` and pin layers via `latest_for_directory()`), and detection
input preparation consumes the same manifest. Remaining work: make
manifest-bound selection mandatory — `_render_layer()` still selects
latest-by-mtime when no manifest is supplied (`input_manifest_bound` false) —
and thread manifests through the durable handoff records instead of in-memory
shared state.

## CLI contract

### Direct commands

```bash
python src/run_edgewarn.py --lat_limits 20 55 --lon_limits 230 300
python src/run_ewmrs.py
python src/run_nexrad.py
```

All three accept `--base_dir` / `--base-dir` and `--config-dir`. Only the
primary command needs latitude/longitude, detection, tracking, CTAM, and
MRMS-core flags.

Disable flags are `argparse.BooleanOptionalAction` switches whose defaults
resolve from `runtime.yaml` through `overlay.resolve`, with environment
overrides; each service must keep that layering for the flags it owns.

Recommended service ownership:

| Flag | Primary | EWMRS | NEXRAD | Launcher routing |
| --- | --- | --- | --- | --- |
| `--base-dir` | yes | yes | yes | all enabled services |
| `--config-dir` | yes | yes | yes | all enabled services |
| `--lat_limits`, `--lon_limits` | yes | no | no | primary |
| `--profile` | yes | optional render profile | optional NEXRAD profile | relevant services |
| `--disable-ctam` | yes | no | no | primary |
| `--disable-tracking` | yes | no | no | primary |
| `--disable-polygon-expansion` | yes | no | no | primary |
| detection tuning: `--refl-threshold`, `--min-seed-percentage`, `--drop-offset` | yes | no | no | primary |
| `--disable-goes` | disables scan GLM | disables ABI ingest/render | no | primary and EWMRS |
| `--disable-metar` | no | yes | no | EWMRS |
| `--disable-nws` | no | yes | no | EWMRS |
| new `--disable-wpc` | no | yes | no | EWMRS (WPC today runs unless `--mrms-core-only`; it has no dedicated flag) |
| `--disable-ewmrs` | no direct meaning | no direct meaning | no | launcher omits EWMRS |
| `--disable-nexrad` | no direct meaning | no direct meaning | no | launcher omits NEXRAD |
| `--mrms-core-only` | MRMS-only primary behavior; implies disabling EWMRS, GOES/GLM, RAP, NEXRAD, NWS, METAR, WPC | no | no | starts only primary |

Keep `IOManager` logging behavior, but replace its monolithic parser with
parser builders so each service exposes only flags it honors. Add parser tests
for both spellings of `--base-dir`, longitude normalization in the primary
service, YAML-default resolution through the overlay for each owned flag, and
exact legacy-flag routing in the launcher.

## Lifecycle, health, and restart semantics

- Each direct service takes a single-instance lock beneath
  `<BASE_DIR>/state/realtime/services/`. A second instance fails clearly before
  starting work.
- Each service publishes an atomic heartbeat containing PID, run ID, version,
  last successful activity, current phase, and degraded children.
- Heartbeats are diagnostic only; correctness uses committed phase records and
  checkpoints.
- Child supervision uses bounded exponential backoff. `AccessorySupervisor`
  already implements this within the monolith (exponential backoff,
  crash-loop disabling after `max_restarts` within a window, atomic health
  file, cleanup-event clearing, NEXRAD heartbeat-staleness restarts); extract
  and reuse it rather than reimplementing. Add backoff reset after a stable
  interval, which the current implementation does not do.
- Heartbeat infrastructure partially exists (NEXRAD ingest heartbeat with
  PID/staleness checks); extend it to all services.
- Primary failure does not delete EWMRS/NEXRAD state. EWMRS drains already
  committed records. NEXRAD continues independently.
- EWMRS failure cannot mark primary cycles failed. Its backlog and oldest
  unprocessed record become health metrics.
- Graceful shutdown stops admission, lets an atomic artifact write finish,
  commits or rejects the current outcome, and then joins owned children.
- Forced shutdown never removes another service's lock, checkpoint, lease, or
  output.

## API visibility of service state

The unified Node API must make intentional or accidental service absence
visible instead of silently serving stale artifacts. Discovery is by canonical
service name, scanning `<BASE_DIR>/state/realtime/services/<name>.json`.

### Service-name registry and heartbeat states

| Name | Producer | Heartbeat file |
| --- | --- | --- |
| `edgewarn` | primary service | `services/edgewarn.json` |
| `ewmrs` | EWMRS/accessory service | `services/ewmrs.json` |
| `nexrad` | NEXRAD service | `services/nexrad.json` |

A named service is in exactly one of these states, derived from its heartbeat:

- `active`: file exists, parses against the supported schema version, belongs
  to the current run (PID/run ID check where applicable), and `updated_at` is
  within the staleness threshold.
- `stale`: file exists but `updated_at` exceeds the threshold — the service
  crashed, hung, or was killed without cleanup.
- `disabled`: no heartbeat file exists — the service was never started or was
  intentionally omitted (`--disable-*` flags, launcher `--services` subset,
  service unit not enabled).
- `degraded`: the service is active but reports degraded children (for
  example a crash-looped accessory loop inside the EWMRS heartbeat). Degraded
  services still serve requests; degradation is surfaced, never fabricated as
  health.

The staleness threshold reuses the existing supervisor settings from
`config/runtime.yaml` rather than introducing a second tuning surface.

### Route-family dependencies

Each route family declares exactly one required service:

| Route family | Required service |
| --- | --- |
| `/api/v3/cells*`, `/api/v3/storm-snapshots*`, `/api/v3/alert-snapshots*`, `/api/v3/alerts*` | `edgewarn` |
| `/api/v3/render-products*`, `/api/v3/models/rap/*`, `/api/v3/analyses/wpc/*`, `/api/v3/styles/colormaps` | `ewmrs` |
| `/api/v3/radar-sites*` | `nexrad` |
| legacy adapters (`/renders/*`, `/wpc/*`, `/colormaps`, `/rap/*`, `/nexrad/*`) | same service as the v3 family they adapt |

### Required behavior

- A small shared scanner module resolves each service state with a short-lived
  cache (mtime-based or 1–2 s TTL) so per-request scans do not amplify I/O.
- When a request hits a route whose required service is not `active`, the API
  returns HTTP 503 with the established error envelope:

```json
{
  "success": false,
  "error": {
    "code": "SERVICE_NOT_ENABLED",
    "message": "Required service is not active",
    "service": "nexrad",
    "state": "disabled",
    "last_seen": null
  }
}
```

  `state` distinguishes `disabled`, `stale`, and (where detectable)
  `unsupported-schema`; `last_seen` carries the heartbeat's `updated_at` when
  present so operators can tell "turned off on purpose" apart from "crashed."
- `/health/ready` keeps its existing directory-based status contract but gains
  a diagnostic `services` block summarizing each registry name's state; it
  does not flip to 503 solely because an optional service is disabled.
- This is a documented, deliberate exception to "existing API contracts remain
  unchanged": new error responses must be added to `docs/api/api_endpoints.md`
  and the OpenAPI document when implemented.
- The Python services only publish heartbeats; they never read them for
  correctness. The API is a consumer, not a participant in the handoff
  protocol.

## NEXRAD contention after decomposition

Separate parents improve fault isolation but do not reserve CPU, RAM, or disk
bandwidth. The former NEXRAD / MRMS resource-contention remediation plan (no
longer present in `plans/`) proposed parent-owned multiprocessing events; the
in-process variant that landed instead is `goes_coordination.pause_ingest_during_render`
in `config/runtime.yaml`, which pauses GOES ABI ingest while a GOES render is
active using events shared inside one process. That mechanism works only
because ingest and render share a parent; it does not translate across
independent services.

Replace that portion with an optional cross-service primary-activity lease:

- Primary atomically creates/refreshes a lease while a latency-sensitive cycle
  is active and clears it in `finally`.
- The lease includes run ID, cycle ID, heartbeat time, and expiry so a primary
  crash cannot pause NEXRAD forever.
- NEXRAD checks the lease before admitting a new site, parser job, or render
  batch, but never interrupts an atomic unit already in progress.
- NEXRAD publishes ingest/render quiescence in its service heartbeat.
- Primary waits only a small configured interval for quiescence; timeout is
  observable and does not block the weather cycle indefinitely.

Keep this policy feature-gated until its own output-parity, primary-latency,
and NEXRAD-freshness criteria pass. The three-service split does not depend on
turning the pause policy on.

## Phased implementation

### Phase 0 — Characterize and lock down contracts

- [ ] Define the canonical service-name registry (`edgewarn`, `ewmrs`,
  `nexrad`) and the API-consumable heartbeat schema, and record the
  route-family-to-service dependency map.
- [ ] Add deterministic tests for current flag behavior, service ownership,
  phase readiness, cycle success/retry, and shutdown. (Cycle-state,
  retry-policy, and supervisor tests exist; extend them to handoff and
  ownership contracts.)
- [ ] Record an independent-process baseline for primary phase latency,
  EWMRS/NEXRAD freshness, CPU, RSS, disk I/O, and output hashes.
  (`[PhaseTelemetry]` producer-time phase lines in `util/runtime/cycle.py`
  provide the latency instrument.)
- [ ] Define required versus optional render layers and the maximum retained
  cycle backlog.
- [x] Implement truthful structured cycle outcomes: `CycleOutcome`,
  `CycleStageResult`, `CycleRetryPolicy`, and the atomic restart-safe
  `CycleStateStore` landed since the original audit (re-audit at `8a6206d`).
- [x] Implement exact-path input manifests: `CycleInputManifest`/
  `StagedInput` flow from the shared coordinator through detection input
  preparation and the MRMS/GOES render pipelines with alignment validation
  (re-audit at `8a6206d`). Follow-up: make manifest binding mandatory instead
  of falling back to latest-by-mtime when no manifest is supplied.
- [ ] Fix atomic publication prerequisites for shared source files and indexes.
  (Atomic publication exists for cycle state, health, and NEXRAD heartbeats;
  source-file and index publication still needs the same guarantee.)

### Phase 1 — Extract service modules without changing deployment

- [ ] Move module-level parsing and runtime initialization into `main()`.
  (`run.py` currently parses args, calls `initialize_runtime()`, resolves
  coordination settings, and wraps `sys.stdout`/`sys.stderr` at import time.)
- [ ] Split CLI builders and runtime imports by ownership.
- [ ] Extract primary, EWMRS/accessory, and NEXRAD service functions.
- [ ] Keep the existing runner as a temporary adapter calling those functions.
- [ ] Verify imports of each service do not load the other scientific stacks.

### Phase 2 — Introduce and shadow the durable handoff

- [ ] Add versioned phase-record and checkpoint schemas.
- [ ] Publish MRMS-ready and RAP-ready records in parallel with existing
  in-memory callbacks.
- [ ] Run a shadow EWMRS consumer that validates records and exact paths but
  does not publish GUI output.
- [ ] Test crash-between-temp-and-rename, malformed record, duplicate record,
  missing exact input, cleanup overlap, restart, and backlog behavior.
- [ ] Compare shadow selections with the files used by the current tandem
  worker for at least ten warm cycles.

### Phase 3 — Cut NEXRAD over to its service

- [ ] Add `run_nexrad.py` and move ingest/render supervision to it.
- [ ] Remove NEXRAD launch and cleanup from primary/EWMRS paths.
- [ ] Verify non-daemonic parser-pool creation, independent component restart,
  shutdown, freshness, retention, and output parity.
- [ ] Publish the `nexrad` service heartbeat under the canonical name and gate
  the radar route families (`/api/v3/radar-sites*`, `/nexrad/*`) behind it
  with `SERVICE_NOT_ENABLED` responses.
- [ ] Adapt cooperative pause coordination to the cross-process lease, still
  default off. (The in-process `pause_ingest_during_render` option only covers
  GOES ingest/render inside one parent; the lease replaces it for NEXRAD
  across services.)

### Phase 4 — Cut EWMRS and accessories over

- [ ] Add `run_ewmrs.py`.
- [ ] Consume committed MRMS/RAP records using exact paths.
- [ ] Move RAP Uint16 conversion out of the shared primary coordinator.
- [ ] Move GOES ABI ingest/render and METAR/NWS/WPC supervision.
- [ ] Publish the `ewmrs` service heartbeat (with accessory child states) and
  gate the render/RAP/WPC/colormap route families behind it with
  `SERVICE_NOT_ENABLED` responses.
- [ ] Constrain GUI cleanup by owner; EWMRS must not touch NEXRAD.
- [ ] Remove the EWMRS worker, queues, events, and activity state from the
  primary process.
- [ ] Verify EWMRS can start before primary, after primary, and after a backlog
  has accumulated.

### Phase 5 — Finalize the primary service

- [ ] Add `run_edgewarn.py` as the supported primary command.
- [ ] Publish the `edgewarn` service heartbeat and gate the analysis route
  families (`/cells`, `/storm-snapshots`, `/alert-snapshots`, `/alerts`)
  behind it with `SERVICE_NOT_ENABLED` responses.
- [ ] Rename/refactor tandem-specific config and telemetry to primary-cycle
  terminology.
- [ ] Remove EWMRS and NEXRAD imports from the primary import graph.
- [x] Advance last-success state only after validated primary completion
  (already implemented via `CycleStateStore.record_outcome`; verify the split
  preserves it).
- [ ] Verify primary operation with both other services stopped and during
  their independent restart.

### Phase 6 — Add and qualify the optional launcher

- [ ] Add `run_all.py` with direct inherited logging and explicit flag routing.
- [ ] Add subprocess/signal/exit-code tests without starting scientific work.
- [ ] Run the launcher performance experiment below.
- [ ] If it passes, convert `run.py` to a compatibility wrapper and document
  both direct and all-services commands.
- [ ] If it fails, keep direct commands as production and record the failed
  metric before optimizing or promoting the launcher.
- [ ] Delete the old monolithic orchestration after the migration window; do
  not maintain two production schedulers.

### Phase 7 — Documentation and deployment

- [ ] Update `README.md`, `INSTALLATION.md`, `docs/core/README.md`,
  `docs/core/ingestion.md`, and `docs/core/goes_pipeline.md`.
- [ ] Document the service-name registry, heartbeat states, route-family
  dependencies, and `SERVICE_NOT_ENABLED` responses in
  `docs/api/api_endpoints.md` and the OpenAPI document; add Jest/Supertest
  coverage for the scanner (active/stale/disabled/degraded) and gated routes
  under `tests/api/`.
- [ ] Document systemd/container examples with one unit/container per direct
  service and a shared configured base directory.
- [ ] Document single-writer requirements, service dependencies, health files,
  backlog recovery, flags, stop order, and rollback.
- [ ] Update the realtime memory benchmark so it can measure one PID tree or
  aggregate three independent service trees.

## Test plan

### Unit and contract tests

- Importing any entrypoint has no side effects.
- Each parser accepts only owned flags; launcher routing is exact.
- Phase records and checkpoints are schema-validated and atomically committed.
- A temp file, malformed JSON, failed phase, wrong timestamp, missing file, or
  path outside the configured base directory is never consumed.
- Duplicate records and restarts are idempotent.
- Consumer checkpoints advance only after validated artifact publication.
- Cleanup preserves leased/unprocessed exact inputs and respects ownership.
- Primary success, failure, interruption, and retry publish truthful outcomes.
- Service signal handlers stop only owned processes.
- NEXRAD ingest remains non-daemonic and always shuts down its parser pool.

### Integration tests

- Run primary alone through a deterministic cached cycle.
- Start EWMRS before primary and confirm it consumes the next committed cycle.
- Start EWMRS after several committed cycles and confirm ordered recovery.
- Kill EWMRS during MRMS render, restart it, and confirm one coherent
  idempotent result.
- Kill primary after MRMS-ready but before primary outcome; EWMRS may render
  the committed phase while primary retries its own failed work.
- Restart NEXRAD ingest without interrupting render, and vice versa.
- Stop/restart EWMRS and NEXRAD during a primary cycle; primary latency and
  outcome remain independent.
- Run all three against one base directory and assert the ownership matrix,
  exact timestamps, indexes, and artifact hashes.
- Verify `--disable-goes`, `--mrms-core-only`, `--disable-ewmrs`, and
  `--disable-nexrad` behavior through direct and launcher commands.

### API service-visibility tests

- Heartbeat states classify correctly: missing file (`disabled`), fresh file
  (`active`), expired `updated_at` (`stale`), degraded children (`degraded`).
- Gated routes return 503 `SERVICE_NOT_ENABLED` with the correct
  `service`/`state`/`last_seen` fields when their service is disabled or
  stale, and serve normally when it is active.
- Route-family mapping is exhaustive: every public route declares exactly one
  required service.
- `/health/ready` keeps its directory-based status while reporting the
  services block, including with all three services disabled.
- The scanner cache does not serve state older than its TTL and does not
  perform a heartbeat read per request under load.

Run targeted tests first in `EdgeWARN-dev`, then the complete Python suite and
the existing Node API suite. No test may depend on live NOAA/AWS availability
for lifecycle or handoff correctness.

## All-services launcher performance gate

Compare two modes using the same commit, Conda environment, host, configuration,
input window, warm caches, and base-directory storage class:

1. **Independent mode:** start `run_edgewarn.py`, `run_ewmrs.py`, and
   `run_nexrad.py` directly from three shells/service units.
2. **Launcher mode:** start the same three commands through `run_all.py`.

Use at least ten comparable warm MRMS cycles in each mode, plus a long enough
soak to cover multiple NEXRAD updates. Randomize or alternate mode order to
reduce host-temperature and network-time bias. Use producer-time phase
telemetry, not parent log-drain timestamps.

Record:

- MRMS discovery-to-detection-release p50/p95.
- Detection, integration, CTAM, and total primary-cycle p50/p95.
- EWMRS record-to-render completion lag and backlog.
- NEXRAD source-to-render freshness and backlog.
- Aggregate and per-service CPU, peak/steady RSS, disk throughput/latency, and
  swap activity.
- Failed/retried cycles, child restarts, shutdown time, and orphan count.
- Exact output/artifact parity.

The launcher qualifies only when:

- Primary p95 phase and total-cycle latency are no more than 5% worse.
- EWMRS and NEXRAD p95 freshness/backlog are no more than 5% worse.
- Aggregate CPU differs by no more than 3 percentage points under comparable
  load.
- Peak aggregate RSS overhead is no more than the greater of 5% or 75 MiB,
  accounting for the supervisor process.
- There is no new swap activity or material disk-latency regression.
- Outputs are identical and there are no additional failures, restarts,
  shutdown timeouts, or orphaned processes.

If a threshold fails, direct startup remains the production recommendation.
Investigate the launcher itself first, especially captured logging, polling,
restart loops, and duplicated imports. Do not weaken the gate merely to retain
the convenience command.

## Acceptance criteria

- Three documented direct commands independently provide the requested
  primary, EWMRS/accessory, and NEXRAD responsibilities.
- Primary owns the only MRMS/RAP downloads and retains scan-time GLM,
  detection, integration, and CTAM behavior.
- EWMRS owns ABI/accessory ingest and MRMS/GOES/RAP rendering without owning
  NEXRAD.
- NEXRAD owns its complete ingest/render/cleanup lifecycle.
- No cross-service `multiprocessing` object is required.
- Cross-service records are atomic, exact-path, timestamp-pinned, durable, and
  restart-safe.
- Starting, stopping, crashing, or restarting EWMRS/NEXRAD does not interrupt
  an active primary cycle.
- No duplicate download, mixed-timestamp input, output-schema change, cleanup
  ownership violation, unbounded backlog, or orphaned process is observed.
- Existing disable/base-directory behavior is preserved through the applicable
  direct command and optional launcher.
- Each service publishes a heartbeat under its canonical name, and the unified
  API reports `SERVICE_NOT_ENABLED` with `disabled`/`stale`/`degraded` state
  for any route whose required service is not active.
- The full relevant test suites and operational parity checks pass.
- A single all-services command is promoted only after the stated performance
  gate passes.

## Rollout and rollback

1. Land schemas, exact-path plumbing, tests, and shadow publication first.
2. Deploy NEXRAD as a separate service while the old runner has NEXRAD
   explicitly disabled.
3. Deploy EWMRS/accessories separately while the old runner has those
   components explicitly disabled.
4. Deploy `run_edgewarn.py` and remove the old tandem parent after parity.
5. Qualify and optionally promote `run_all.py`.

At every cutover there must be exactly one writer for each row in the ownership
matrix. Roll back by stopping the new service and re-enabling the corresponding
old owner, never by running both writers concurrently. Retain cycle records and
checkpoints during rollback so diagnosis does not destroy recovery evidence.

## Non-goals

- Combining the Node API servers with these Python processing services.
- Changing detection, tracking, integration, CTAM, NEXRAD parsing, or render
  science.
- Making EWMRS or NEXRAD success part of the primary cycle success condition.
- Using a message broker or database when atomic runtime-filesystem records are
  sufficient.
- Claiming that separate parent processes alone solve host resource
  contention.
