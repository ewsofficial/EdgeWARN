# RAP Integration Staging Remediation Plan

## Objective

Keep EdgeWARN integration available during short RAP publication gaps by selecting
the newest acceptable RAP analysis from the local cache or NOAA S3, while making
the staleness decision, attempted sources, and terminal failure reason observable.

## Current Status

- **Affected version/branch:** The report describes v2.7.0 behavior on
  `version-test/3.0.0`; the current checkout is also on `version-test/3.0.0`.
- **Confirmed failure path:**
  1. `src/common/ingest/synoptic/downloader.py::download_synoptic()` considers
     only the requested hour and one previous hour.
  2. An async 404 skips the sync attempt for that same key, but after both hours
     return 404 the function returns `None`.
  3. `src/common/ingest/synoptic/main.py::download_rap_async()` passes that
     false result to the coordinator.
  4. `src/common/pipeline/coordinator.py::_safe_ingest()` treats the missing
     required path as an async failure and invokes `download_rap()` as a sync
     fallback. That wrapper repeats the same complete source-level search.
  5. The coordinator sets `rap_inputs_ready=False`; `src/util/runtime/cycle.py`
     consequently releases integration as unavailable, and the EdgeWARN worker
     skips integration.
- **Current staleness/retention behavior:** `RAP_MAX_AGE_MINUTES = 90` is used
  only by generic cache cleanup. Cleanup measures filesystem modification time,
  not the RAP analysis time encoded in the filename. The downloader does not
  use this constant to bound fallback selection.
- **Current local-cache behavior:** The S3 downloader returns an exact candidate
  path immediately if it exists, but the fallback loop does not independently
  discover the newest acceptable cached analysis, report its age, or reject it
  based on analysis time.
- **Current diagnostics:** Individual 404 keys are logged, but the terminal
  coordinator state only says `RAP inputs unavailable`. It does not include the
  checked window, selected/required analysis age, or failure category.
- **Current test coverage:** Tests cover current/previous-hour attempts, 404
  logging, cleanup call counts, and coordinator readiness on a generic RAP
  success/failure. They do not cover a bounded multi-hour search, analysis-age
  enforcement, local fallback, rollover, de-duplicated retries, or detailed RAP
  readiness errors.
- **Runtime-path discrepancy:** No `opdata` or `op-data` path is hard-coded in
  the Python runtime. `util.file.initialize_filesystem()` derives `data/RAP`
  from the supplied base directory. The reported path change is therefore
  likely launch/deployment configuration and should be audited separately.
- **Implementation status:** No remediation has been implemented yet.

## Proposed Policy

1. Define one RAP availability policy in
   `src/common/ingest/synoptic/config.py`:
   - `RAP_MAX_AGE_MINUTES`, default **180 minutes**.
   - Optional `EDGEWARN_RAP_MAX_AGE_MINUTES` override, validated as a
     non-negative integer with a clear startup/configuration error for invalid
     values.
   - Continue to keep at most `RAP_MAX_FILES = 3` cached analyses unless
     operational evidence requires a different count.
2. Measure staleness as:

   `requested scan timestamp - candidate RAP analysis timestamp`

   Do not use download time or filesystem modification time for eligibility.
3. Normalize the requested time to UTC and floor candidates to whole analysis
   hours. Enumerate newest-to-oldest candidates whose analysis age is less than
   or equal to the configured limit.
4. The 180-minute default is intentional: for scans a few minutes after an
   hour, it permits the second-prior analysis when the current and previous
   analyses are missing. A 90-minute default would reproduce the reported
   outage because the second-prior RAP analysis is already about 120–179
   minutes old.
5. A local candidate is acceptable when it has the expected filename, is a
   regular non-empty file, and its encoded analysis timestamp is within policy.
   Full GRIB decoding remains the responsibility of the consuming pipeline;
   adding an expensive decode to readiness is outside this fix.

## Proposed Changes

### 1. Centralize RAP policy and timestamp helpers

Files:

- `src/common/ingest/synoptic/config.py`
- optionally a small RAP-specific helper module under
  `src/common/ingest/synoptic/`

Changes:

- Move the maximum-age definition out of `main.py` so downloader selection and
  retention import the same value.
- Add helpers to:
  - normalize aware/naive input consistently to UTC;
  - enumerate eligible hourly analysis timestamps;
  - parse the analysis timestamp from
    `RAP.YYYYMMDD-HHz.awp130pgrbf00.grib2`;
  - calculate and format candidate age.
- Keep the generic synoptic downloader reusable by accepting `max_age_minutes`
  (and, if needed, a local-candidate validator) rather than embedding RAP-only
  constants in generic code.

### 2. Replace the fixed two-hour fallback with one bounded search

File:

- `src/common/ingest/synoptic/downloader.py`

Changes:

- Replace `[dt, dt - timedelta(hours=1)]` with the policy-generated candidate
  sequence.
- For each candidate, in newest-first order:
  1. Build the rollover-safe S3 key and expected local path.
  2. Check and validate the exact local file before opening a network client.
  3. If local data is valid, log the selected analysis timestamp, age, path,
     and source=`local`, then return it.
  4. Otherwise attempt the async S3 download.
  5. On a definitive 404/`NoSuchKey`, record that key once and continue to the
     next hour; do not retry the same missing object synchronously.
  6. On an async transport/client failure where the sync implementation is a
     materially different fallback, try sync once for that candidate.
  7. If downloaded, validate the resulting non-empty file, log analysis time,
     age, and source=`s3`, then return it.
- Track attempted keys and failure categories. On exhaustion, raise a
  RAP/synoptic-specific availability exception containing:
  - requested timestamp;
  - maximum permitted age;
  - every candidate/key checked;
  - whether each failure was `not_found`, `transport`, `authentication`,
    `local_invalid`, or another client error.
- Do not silently collapse a fully exhausted search to an unqualified `None`.

### 3. Make RAP cache retention use analysis time

File:

- `src/common/ingest/synoptic/main.py`
- RAP-specific helper/test file as needed

Changes:

- Replace RAP’s use of generic mtime-based `clean_old_files()` with a
  RAP-specific cleanup routine that parses analysis timestamps from filenames.
- Preserve files eligible under the same `RAP_MAX_AGE_MINUTES` policy during
  pre-download cleanup; then enforce the three-file cap newest-analysis-first
  after a successful selection/download.
- Ignore `.idx` and unrelated files safely, and log malformed RAP filenames
  rather than treating their modification time as analysis freshness.
- Keep all cleanup constrained to `fs.RAP_DIR` under the configured runtime
  base directory.

### 4. Remove the duplicate coordinator-level RAP search

File:

- `src/common/pipeline/coordinator.py`

Changes:

- Treat `download_rap_async()` as the owner of local lookup, bounded S3 search,
  and meaningful async-to-sync source fallback.
- Add a localized way for `_safe_ingest()` to disable its outer sync retry, or
  use a dedicated RAP ingest wrapper. Do not call `download_rap()` immediately
  after an already exhaustive `download_rap_async()` result.
- Preserve `require_result=True` semantics: readiness is true only when a staged
  path is returned.
- Carry the structured terminal RAP reason into
  `state.errors["rap_ingest"]`. Keep
  `edgewarn_integration_ingest` as the aggregate error, but make the RAP entry
  identify the exact missing prerequisite and staleness window.
- Keep `mrms_core_only` unchanged: RAP remains optional only in that mode.
- Do not gate EdgeWARN readiness on EWMRS RAP Uint16 conversion; the current
  separation of raw RAP readiness from derived render conversion is correct.

### 5. Improve operational logging and documentation

Files:

- `src/common/ingest/synoptic/downloader.py`
- `docs/core/ingestion.md`
- environment/configuration documentation (`README.md` and/or
  `INSTALLATION.md`)

Changes:

- Emit one terminal selection record, for example:

  `RAP selected analysis=2026-07-26T11:00:00Z age_minutes=126 source=s3 path=...`

- Emit one terminal exhaustion record with the age limit and checked keys,
  while retaining per-key 404 warnings.
- Document `EDGEWARN_RAP_MAX_AGE_MINUTES`, its 180-minute default, analysis-time
  semantics, and the effect of setting it lower.
- Add the RAP selection metadata to cycle telemetry if the existing log format
  can carry it without changing public API contracts.

### 6. Audit the runtime base-directory deployment separately

Operational action (not a downloader alias):

- Compare the launch arguments/environment for the current `EdgeWARN` process
  with the prior deployment and confirm whether `<base-dir>/opdata` versus
  `<base-dir>/op-data` was intentional.
- Log the resolved `fs.BASE_DIR` and `fs.RAP_DIR` once at startup.
- If the path change was accidental, fix the service/screen launch
  configuration or migrate the cache deliberately. Do not make the ingest code
  search multiple undeclared base directories, because that would weaken the
  configured-base-directory source-of-truth and cleanup safety rules.

## Regression Test Plan

### Downloader unit tests

Extend `tests/core/ingest/test_synoptic_downloader.py`:

1. **Current available:** selects the requested hour and performs no older
   request.
2. **Previous available:** current returns 404, previous is staged, and each key
   is attempted once.
3. **Second previous available:** current and previous return 404, second
   previous within the configured window is staged and reported with the
   correct age.
4. **Out of policy:** an object/local file exists just beyond the maximum age;
   it is neither selected nor requested, and exhaustion reports the configured
   limit.
5. **Boundary inclusive:** a candidate exactly at the maximum age is accepted;
   one minute beyond it is rejected.
6. **Local hit:** a valid local fallback returns without any S3 call.
7. **Invalid local file:** a zero-byte exact candidate is rejected/logged and
   the remote lookup proceeds.
8. **404 de-duplication:** each permitted S3 key is checked exactly once and
   sync is not invoked for a definitive async 404.
9. **Transport fallback:** a non-404 async transport failure invokes sync once
   for that candidate; a sync success is selected.
10. **Failure classification:** authentication/transport failures remain
    distinguishable from all-candidates-404 in the terminal exception/log.
11. **Day/month/year rollover:** e.g. a `2026-03-01T00:05Z` request builds
    `rap.20260228/rap.t23z...` and older keys correctly; add a year rollover
    parameter case.
12. **Timezone normalization:** an aware non-UTC timestamp maps to the correct
    UTC analysis keys; define and test the chosen behavior for naive inputs.

### RAP wrapper and retention tests

Update `tests/core/ingest/synoptic/test_main.py` and add focused cleanup tests:

1. Downloader and cleanup receive the same configured maximum-age value.
2. Cleanup orders/prunes by the RAP timestamp in the filename, not mtime.
3. A recently downloaded but analysis-stale file is removed.
4. An analysis-valid file with an artificially old mtime remains eligible.
5. Pre-download cleanup does not delete an eligible local fallback before the
   downloader can select it.
6. The three-file cap retains the newest three RAP analyses.
7. Malformed names and `.idx` files are handled safely.
8. Invalid `EDGEWARN_RAP_MAX_AGE_MINUTES` values fail clearly and valid
   overrides change both search and retention behavior.

### Coordinator/readiness tests

Update `tests/core/test_tandem_coordinator.py` and
`tests/integration/test_tandem_coordinator.py`:

1. MRMS detection, MRMS integration, and GLM success plus second-prior RAP
   success releases EdgeWARN integration as ready.
2. MRMS and GLM success plus exhausted RAP search leaves integration
   unavailable, with `rap_ingest` as the sole source-specific missing
   prerequisite and an aggregate `edgewarn_integration_ingest` error.
3. Exhausted RAP search is invoked once; the coordinator does not repeat the
   entire async-plus-sync candidate sequence.
4. A selected local RAP path sets `rap_inputs_ready=True`.
5. `mrms_core_only=True` still releases base integration without RAP.
6. EWMRS RAP Uint16 conversion failure does not revoke EdgeWARN raw-RAP
   readiness.

### Acceptance scenario

Add a mocked end-to-end staged-ingest regression reproducing the report:

- scan time `2026-07-26T13:06:00Z`;
- empty RAP cache;
- 13z and 12z objects return 404;
- 11z exists;
- MRMS and GLM are ready.

Expected result:

- 11z is staged under the configured `<BASE_DIR>/data/RAP`;
- selected age is logged as 126 minutes;
- no duplicate coordinator search occurs;
- `rap_inputs_ready=True`;
- `integration_released status=ready`;
- EdgeWARN integration is not skipped.

Run focused tests first:

```bash
conda run -n EdgeWARN-dev python -m pytest \
  tests/core/ingest/test_synoptic_downloader.py \
  tests/core/ingest/synoptic/test_main.py \
  tests/core/test_tandem_coordinator.py \
  tests/integration/test_tandem_coordinator.py
```

Then run the full Python suite:

```bash
conda run -n EdgeWARN-dev python -m pytest tests/
```

## Implementation Order

1. Add policy/timestamp helpers and their boundary/rollover tests.
2. Implement bounded local-first selection and structured exhaustion reporting.
3. Replace RAP mtime cleanup with analysis-time retention.
4. Remove the duplicate coordinator retry and propagate detailed readiness
   errors.
5. Add coordinator and acceptance regressions.
6. Update configuration/ingestion documentation.
7. Run focused tests, then the full Python suite.
8. Verify the production launch base directory before deployment and monitor
   RAP selection age plus `integration_released` telemetry for at least several
   cycles.

## Completion Criteria

- The reported 13:06 scenario selects 11z under the default policy and releases
  integration.
- No RAP candidate outside the configured analysis-age window can satisfy
  readiness, regardless of file mtime.
- Each S3 key is attempted at most once per RAP ingest operation after a
  definitive 404.
- A local eligible RAP analysis avoids network access.
- Terminal failure identifies RAP, the configured age limit, all candidates
  checked, and the relevant failure categories.
- Cache cleanup and downloader selection enforce the same analysis-time policy.
- Focused and full Python test suites pass in `EdgeWARN-dev`.
