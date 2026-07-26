# Real-Time Runner Decomposition Plan

**Audit baseline:** commit `28beff7242495170ad4cc34d22d74f0b3316e931`
on `version-test/3.0.0`  
**Package version:** `2.7.0`  
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

## Current-state evidence

`src/run.py` currently owns all of the following:

- MRMS timestamp discovery and the forever polling loop.
- A lifetime `multiprocessing.Manager`.
- Per-cycle EdgeWARN and EWMRS workers and their readiness events/state.
- Per-cycle MRMS, RAP, and scan-time GLM ingestion.
- Persistent GOES ABI ingest and render processes, queues, and activity events.
- Persistent METAR, NWS, and WPC processes.
- Persistent NEXRAD ingest and render processes and a NEXRAD log queue.
- Shutdown of every child and queue.

The process boundary is not merely located in `run.py`.
`src/util/runtime/cycle.py` imports both EdgeWARN and EWMRS workers, while
`src/util/runtime/background.py` imports GOES, accessory, NEXRAD, and EWMRS
implementations at module load. Copying the entrypoint code into three files
would therefore preserve much of the current coupling.

The existing shared staged-ingest flow must also be handled deliberately:

- Detection MRMS is released before integration MRMS and RAP complete.
- EWMRS MRMS rendering currently waits for validated detection and integration
  MRMS groups.
- Primary integration requires integration MRMS and raw RAP, plus scan-time
  GLM when GOES is enabled.
- GOES ABI is an EWMRS render input and is already ingested by a background
  loop; it is not a substitute for scan-time GLM.
- RAP Uint16 conversion is an EWMRS-derived artifact, but it currently runs
  inside the shared ingest coordinator.
- EWMRS render helpers currently select latest source files independently.
  Separate services make that unsafe unless the handoff pins exact paths.
- EWMRS GUI cleanup currently reaches NEXRAD outputs. After the split, only the
  NEXRAD service may clean NEXRAD artifacts.

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
- Existing disable flags retain their meaning when passed to the optional
  all-services launcher.
- No entrypoint may parse arguments, initialize runtime paths, or spawn
  processes as an import side effect.
- Every service must handle `SIGINT` and `SIGTERM`, close its own children, and
  leave no orphaned worker or stale permanent pause.

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

Advancing `last_processed` must occur only after a truthful primary
`CycleOutcome`. This split must not preserve the current behavior that marks a
timestamp processed before `run_tandem_cycle_once()` reports its result.

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

Suggested modules:

- `src/util/runtime/primary_service.py` for timestamp polling and primary-cycle
  orchestration.
- `src/util/runtime/ewmrs_service.py` for phase consumption and accessory
  supervision.
- `src/util/runtime/nexrad_service.py` for NEXRAD supervision.
- `src/util/runtime/handoff.py` for schemas, atomic publication, discovery,
  validation, leases, and checkpoints.
- `src/util/runtime/cli.py` for reusable common flags and service-specific
  parsers.
- `src/util/runtime/supervisor.py` for lifecycle helpers shared by service
  parents and the optional launcher.

Split `src/util/runtime/cycle.py` so the primary path no longer imports
`ewmrs_tandem_worker`. Split `src/util/runtime/background.py` by service so
importing primary runtime code does not import EWMRS or NEXRAD implementations.
Keep narrow compatibility re-exports only during migration and remove them
after all callers and tests use the new modules.

The EWMRS rendering APIs must gain exact-input variants. Passing only `dt` is
insufficient while `_render_layer()` and other helpers select latest local
files. The handoff consumer should pass an immutable input manifest through
the render stack. Apply the same exact-path rule to primary detection and
integration as part of the prerequisite cycle-correctness work.

## CLI contract

### Direct commands

```bash
python src/run_edgewarn.py --lat_limits 20 55 --lon_limits 230 300
python src/run_ewmrs.py
python src/run_nexrad.py
```

All three accept `--base_dir` / `--base-dir`. Only the primary command needs
latitude/longitude, detection, tracking, CTAM, and MRMS-core flags.

Recommended service ownership:

| Flag | Primary | EWMRS | NEXRAD | Launcher routing |
| --- | --- | --- | --- | --- |
| `--base-dir` | yes | yes | yes | all enabled services |
| `--lat_limits`, `--lon_limits` | yes | no | no | primary |
| `--profile` | yes | optional render profile | optional NEXRAD profile | relevant services |
| `--disable-ctam` | yes | no | no | primary |
| `--disable-tracking` | yes | no | no | primary |
| `--disable-polygon-expansion` | yes | no | no | primary |
| `--disable-goes` | disables scan GLM | disables ABI ingest/render | no | primary and EWMRS |
| `--disable-metar` | no | yes | no | EWMRS |
| `--disable-nws` | no | yes | no | EWMRS |
| new `--disable-wpc` | no | yes | no | EWMRS |
| `--disable-ewmrs` | no direct meaning | no direct meaning | no | launcher omits EWMRS |
| `--disable-nexrad` | no direct meaning | no direct meaning | no | launcher omits NEXRAD |
| `--mrms-core-only` | MRMS-only primary behavior | no | no | starts only primary |

Keep `IOManager` logging behavior, but replace its monolithic parser with
parser builders so each service exposes only flags it honors. Add parser tests
for both spellings of `--base-dir`, longitude normalization in the primary
service, and exact legacy-flag routing in the launcher.

## Lifecycle, health, and restart semantics

- Each direct service takes a single-instance lock beneath
  `<BASE_DIR>/state/realtime/services/`. A second instance fails clearly before
  starting work.
- Each service publishes an atomic heartbeat containing PID, run ID, version,
  last successful activity, current phase, and degraded children.
- Heartbeats are diagnostic only; correctness uses committed phase records and
  checkpoints.
- Child supervision uses bounded exponential backoff and resets the backoff
  after a stable interval. Repeated crash loops become degraded health rather
  than unbounded rapid respawn.
- Primary failure does not delete EWMRS/NEXRAD state. EWMRS drains already
  committed records. NEXRAD continues independently.
- EWMRS failure cannot mark primary cycles failed. Its backlog and oldest
  unprocessed record become health metrics.
- Graceful shutdown stops admission, lets an atomic artifact write finish,
  commits or rejects the current outcome, and then joins owned children.
- Forced shutdown never removes another service's lock, checkpoint, lease, or
  output.

## NEXRAD contention after decomposition

Separate parents improve fault isolation but do not reserve CPU, RAM, or disk
bandwidth. The existing
[NEXRAD / MRMS Resource-Contention Remediation Plan](nexrad-mrms-resource-contention-remediation-plan.md)
currently proposes parent-owned multiprocessing events; those specific wiring
steps are incompatible with independent services.

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

- [ ] Add deterministic tests for current flag behavior, service ownership,
  phase readiness, cycle success/retry, and shutdown.
- [ ] Record an independent-process baseline for primary phase latency,
  EWMRS/NEXRAD freshness, CPU, RSS, disk I/O, and output hashes.
- [ ] Define required versus optional render layers and the maximum retained
  cycle backlog.
- [ ] Implement truthful structured cycle outcomes and exact-path input
  manifests required by this plan.
- [ ] Fix atomic publication prerequisites for shared source files and indexes.

### Phase 1 — Extract service modules without changing deployment

- [ ] Move module-level parsing and runtime initialization into `main()`.
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
- [ ] Adapt cooperative pause coordination to the cross-process lease, still
  default off.

### Phase 4 — Cut EWMRS and accessories over

- [ ] Add `run_ewmrs.py`.
- [ ] Consume committed MRMS/RAP records using exact paths.
- [ ] Move RAP Uint16 conversion out of the shared primary coordinator.
- [ ] Move GOES ABI ingest/render and METAR/NWS/WPC supervision.
- [ ] Constrain GUI cleanup by owner; EWMRS must not touch NEXRAD.
- [ ] Remove the EWMRS worker, queues, events, and activity state from the
  primary process.
- [ ] Verify EWMRS can start before primary, after primary, and after a backlog
  has accumulated.

### Phase 5 — Finalize the primary service

- [ ] Add `run_edgewarn.py` as the supported primary command.
- [ ] Rename/refactor tandem-specific config and telemetry to primary-cycle
  terminology.
- [ ] Remove EWMRS and NEXRAD imports from the primary import graph.
- [ ] Advance last-success state only after validated primary completion.
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
