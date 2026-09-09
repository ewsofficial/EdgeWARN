# EdgeWARN-Core Test-Suite Remediation Plan

Source audit: [test-suite-audit.md](test-suite-audit.md), dated 2026-09-07.

## Objective

Restore confidence in the test suite by making default runs reproducible,
removing tests that overstate their coverage, adding deterministic connected
weather-data tests, and reorganizing the suite around stable component
ownership.

The audit identifies a credibility problem more than a raw test-count problem:
valuable failure and recovery coverage exists, but misleading integration
tests, environment-sensitive discovery, and duplicated characterization obscure
what the suite actually proves.

This work should be delivered incrementally. Do not begin with a directory-wide
rewrite, and do not remove a test merely to reduce the test count.

## Guiding Principles

- Preserve failure-state, recovery, data-integrity, and security tests.
- Prefer observable behavior over implementation shape, source substrings, or
  configured mock return values.
- Keep correctness tests deterministic and offline by default.
- Use small sanitized weather fixtures instead of live NOAA or AWS calls.
- Prefer several strong boundary slices over one large end-to-end smoke test.
- For every deletion or merge, identify the realistic failure previously caught
  and the retained test that continues to catch it.
- For numerical and spatial behavior, use an independent expected result or
  invariant.
- For orchestration, validate state transitions and output visibility rather
  than mock call order alone.

## Implementation Sequence

### Phase 1: Restore a Trustworthy Baseline (P0)

Make the suite reliably collect and execute in a clean declared environment.

#### Tasks

- Reproduce the rasterio, netCDF4, and pyproj Brotli linkage failure from a clean
  Conda environment.
- Treat `EdgeWARN` as the canonical Conda environment name and align dependency
  resolution and project documentation with that workflow.
- Record exact native package versions or introduce a lock/export strategy if
  unconstrained dependency solving caused the ABI mismatch.
- Add a clean-environment `pytest --collect-only` CI check.
- Separate correctness tests from benchmarks and runtime-data-dependent tests.
- Prevent default pytest from discovering data under an operator's runtime base
  directory.
- Constrain test runtime roots to temporary directories and restore affected
  configuration loaders, filesystem globals, caches, and environment variables.
- Run representative tests alone and in shuffled order to expose state leaks.
- Investigate lazy `EdgeWARN` package exports so narrow unit tests do not import
  the full rasterio/netCDF4 scientific stack unnecessarily.

#### Acceptance Criteria

- Full Python collection completes with zero collection errors.
- `npm test` and the default Python correctness command are deterministic and
  offline.
- Representative tests produce the same results alone and in shuffled order.
- Missing benchmark data cannot silently alter default test selection.
- Native-library problems are reported as environment failures, not hidden by
  skipping affected test modules.

### Phase 2: Remove False Confidence (P0)

Correct tests whose names or assertions currently overstate the production
behavior they execute.

#### Tasks

- Delete `test_full_detect_to_integrate_pipeline`, or replace it after a real
  connected pipeline slice exists. Its current dictionary mutation executes no
  production pipeline.
- Replace the fully mocked ingest-to-detect integration tests with:
  - a small component test for any distinct orchestration or option wiring;
  - a real synthetic decode-to-detection integration test.
- Rewrite `test_load_probsevere_normalizes_longitude` to assert the transformed
  geometry and retained feature rather than dictionary length.
- Resolve the EWMRS invalid-bounds contract:
  - if fallback is intended, fix production code and assert default bounds;
  - if rejection is intended, change the message and assert the rejection.
- Replace the configuration-arithmetic pruning assertion with just-before,
  exactly-at, and just-after expiry cases against the pruning implementation.
- Delete the private-method-absence assertion while preserving current
  reacquisition behavior tests.

#### Acceptance Criteria

- Every test name describes production behavior that the test actually runs.
- Assertions validate meaningful output values, identity, geometry, files, or
  state transitions.
- No integration test passes solely by asserting values inserted by that test.
- Invalid EWMRS bounds have one documented, unambiguous contract.

### Phase 3: Remove Dormant and Misleading Artifacts (P0/P1)

Perform an evidence-backed cleanup after the baseline is green.

#### Tasks

- Remove the three permanently skipped retired NEXRAD serializer tests and
  helpers used only by the retired serializer.
- If orientation remains an output requirement, first add an active numerical
  orientation test for the current serializer.
- Remove the unused RAP `mock_datasets` fixture, or use it in a genuine
  extractor-boundary test.
- Confirm that the following baselines have no internal or external consumer,
  then remove them or restore a purposeful consumer:
  - `tests/config_baseline/azshear_constants.json`
  - `tests/ctam_baseline/stormcell_entry_field_inventory.json`
  - `tests/ctam_baseline/stormcell_grid_only_synthetic_entry.json`
  - `tests/ctam_baseline/stormcell_snapshot_envelope.json`
- Correct the stale case-sensitive StormCast benchmark import.
- Remove empty Jest/Python scaffolding only after verifying pytest collection and
  helper imports.

#### Acceptance Criteria

- No permanently skipped test remains solely for a retired implementation.
- No golden fixture remains without a known consumer and documented contract.
- Each deletion records either its replacement or why it provided no production
  regression protection.

### Phase 4: Add a Connected Weather-Data Spine (P1)

Create a few deterministic, connected boundary tests instead of one oversized
end-to-end test.

#### Fixtures

Add small sanitized inputs under `tests/fixtures/weather/`:

- an MRMS-like reflectivity raster and precipitation product;
- realistic ProbSevere JSON;
- a minimal GRIB sample with known coordinates, units, fill values, scanning
  direction, aliases, and multiple fields;
- existing constructed AR2V bytes as the basis for radar cases.

#### Connected Slices

1. Decode MRMS-like inputs through the real detection loader and geometry code.
2. Integrate real ProbSevere fields, GLM/statistical inputs, and RAP values.
3. Publish storm-cell output and reopen the snapshot/index from disk.
4. Run a second cycle with tracking/history and CTAM enabled.
5. Verify pinned scan inputs so newer files cannot contaminate an active cycle.
6. Exercise missing optional data and malformed input with explicit expected
   outcomes.

Remote boundaries should use protocol-faithful fake transports. Live NOAA and
AWS access belongs only in an explicit optional compatibility probe.

#### Acceptance Criteria

- At least one test runs real decode, detection, enrichment, publication, and
  reopen behavior without replacing the detector, mapper, integrator, or saver.
- Published output is independently read and validated.
- A second cycle proves history and identity continuity.
- Input timestamps remain consistent when newer data arrives mid-cycle.
- The test uses a disposable runtime tree and no external network service.

### Phase 5: Close High-Risk Semantic Gaps (P1)

Add focused behavioral matrices around scientific calculations and storm
identity.

#### ProbSevere Integration

- Feature `id` versus `properties.ID`.
- Numeric and string identifiers.
- Missing and unmatched identifiers.
- Missing fields and malformed numeric values.
- Alternate configuration field mappings.
- Preservation of existing storm-cell properties.
- Correct `MATCH_ERROR` behavior.

#### Morphology

- Straight line.
- Branching structure.
- Compact blob.
- Concave shape.
- Tiny mask.
- Empty mask.

Use exact expected values where stable and independent invariants elsewhere.
Keep performance measurement separate from correctness.

#### Tracking and Lineage

- Exact track-to-detection mapping rather than active-result counts.
- Split and merge ancestry.
- Crossing paths.
- Drop and reacquisition.
- Continuity under changed detection IDs.
- Reset behavior after long time gaps.

#### Configuration Schema

- Add language-neutral vectors shared between Python and JavaScript validators.
- Exercise one invalid constraint at a time.
- Cover inclusive boundaries, oversized arrays, uniqueness, patterns, enums,
  unsupported keywords, and nonfinite values.

#### API Contracts

- Test rejected-origin and preflight CORS behavior independently of throttling.
- Keep rate-limit behavior as a separate test.
- Validate representative success and problem responses against OpenAPI schemas.
- Cover method, query, bounds, streaming disconnect, and handle-cleanup behavior.

### Phase 6: Harden Recovery and Process Boundaries (P1)

Preserve the audit's strongest tests while adding real execution at boundaries
that currently rely only on fakes.

#### Tasks

- Retain durable handoff, atomic publication, retry, checkpoint, and restart
  fault-injection cases.
- Replace correctness sleeps with events, readiness probes, controllable
  futures, or explicit heartbeats.
- Add a bounded real worker-pool death and replacement test that verifies:
  - the replacement generation succeeds;
  - the old PID is gone;
  - file handles and temporary artifacts are released;
  - the production multiprocessing start method is covered.
- Add one real renderer-executor test using a tiny source artifact.
- Verify process initialization, non-daemon topology, chunk/index publication,
  and failure without a success checkpoint.
- Add a restart/replay test that interrupts publication before checkpointing,
  restarts against the same temporary tree, and proves that no successful cycle
  is skipped or published twice.

#### Acceptance Criteria

- Correctness no longer depends on fixed startup sleeps or sub-second timing
  thresholds.
- At least one real worker generation is killed and replaced successfully.
- Restart tests validate durable externally visible state, not only mock calls.
- Precise unit-level fault injection remains in place for diagnostic value.

### Phase 7: Make CI Lanes Express Actual Scope (P1/P2)

Create explicit execution lanes for different guarantees.

#### Proposed Lanes

- Fast offline correctness.
- Connected integration.
- Process, restart, and signal tests.
- Node API contracts.
- Packaging and installed-command checks.
- Explicit opt-in benchmarks and live compatibility probes.

Register and consistently apply markers such as `unit`, `integration`,
`process`, `slow`, `benchmark`, and `network`. Because pytest uses strict
markers, marker registration and application must land together.

#### Coverage Rollout

1. Add `pytest-cov` and collect Python coverage without initially enforcing a
   threshold.
2. Run Node CI with coverage collection enabled.
3. Publish coverage and machine-readable test-result artifacts.
4. Confirm whether Python child-process execution is captured.
5. Review the baseline before setting thresholds.
6. Introduce conservative thresholds and ratchet them upward through reviewed
   changes.

#### Packaging

- Build the wheel once.
- Reuse that exact artifact for the outside-checkout smoke test and packaging
  tests.
- Avoid rebuilding the wheel in multiple jobs or test fixtures.

#### Acceptance Criteria

- Each CI lane has a stated boundary and deterministic selection rule.
- Default CI excludes benchmarks, live network access, and operator data.
- Coverage reports are available for both languages.
- Coverage thresholds are based on a reviewed baseline rather than aspiration.
- Packaging tests consume one produced wheel outside the checkout.

### Phase 8: Reorganize by Stable Ownership (P2)

Reorganize only after behavioral gaps and execution lanes are stable.

#### Target Areas

- Configuration behavior: `tests/unit/config/`.
- Declaration and source-ownership checks: `tests/architecture/`.
- Detection, tracking, Kalman, and lineage: `tests/unit/detection/`.
- RAP, GLM, ProbSevere, statistics, and AzShear:
  `tests/unit/enrichment/`.
- EWMRS, GOES, RAP, and NEXRAD rendering: `tests/unit/rendering/`.
- Durable handoff and replay: `tests/integration/handoff/`.
- Real process lifecycle: `tests/integration/processes/`.
- Binary producer/consumer contracts: `tests/integration/serialization/`.
- Installed-wheel and container execution: `tests/packaging/`.
- Performance work: top-level opt-in `benchmarks/`.

#### Migration Rules

- Replace severity and migration-phase filenames with behavioral ownership.
- Preserve issue provenance in descriptive names or docstrings where useful.
- Move shared builders and contract vectors out of other test modules.
- Keep the root `conftest.py` small and free of eager scientific imports.
- Consolidate configuration snapshots, literal counts, AST checks, and source
  substring checks only after mapping their distinct failure sensitivity.
- Preserve justified cross-language or schema-versus-runtime overlap.
- Move component-sized groups and keep each migration independently reviewable.

### Phase 9: Complete Secondary Boundary Coverage (P2/P3)

After the core remediation is stable:

- Add WPC parser/converter matrices and ancillary-service consumption.
- Add multi-timestamp historical replay with gaps, duplicates, timezone/day
  rollover, tracking continuity/reset, CTAM flags, and missing optional sources.
- Generate RAP, EWMRS, and NEXRAD artifacts with production Python encoders,
  serve them through Node, and independently validate bytes, shape, endian,
  nodata, and metadata.
- Add a container smoke test using disposable mounts and SIGTERM.
- Add supported-platform CI jobs or explicitly document Linux-only guarantees.
- Consolidate NEXRAD performance scripts around one sampler with explicit
  input/output paths and synthetic/live modes.
- Replace hard-coded local benchmark paths with required CLI arguments.
- Use targeted mutation experiments only for suspected weak assertions; do not
  choose deletion targets from assertion counts alone.

## Delivery Strategy

Use small pull requests with the following dependency order:

1. Environment and collection reliability.
2. Deterministic default selection and runtime-root isolation.
3. False-confidence test corrections.
4. Dormant artifact cleanup.
5. Deterministic weather fixtures and connected pipeline slices.
6. Scientific and identity assertion strengthening.
7. Real process/recovery boundaries.
8. CI lanes, reports, and initial coverage baselines.
9. Ownership-based file moves and consolidation.
10. Historical, serialization, container, and platform expansion.

Behavioral changes and large file moves should not share a pull request. This
keeps reviews attributable and makes regressions easier to bisect.

## Definition of Done

The remediation is complete when:

- Clean Python and Node runs are reproducible and offline by default.
- Python collection completes without native dependency errors.
- Benchmarks and live-data checks are explicit opt-ins.
- Tests cannot opportunistically read or modify an operator runtime tree.
- No test described as integration merely mutates local dictionaries or asserts
  configured mock returns.
- A deterministic weather fixture passes through real decoding, detection,
  enrichment, publication, and reopen.
- Multi-cycle tests validate storm identity and history continuity.
- Recovery tests cover a real process death and restart.
- API CORS, rate limiting, schema validation, and binary contracts are tested
  independently.
- Coverage is reported in CI with reviewed thresholds.
- Packaging uses one built artifact and validates it outside the checkout.
- Every deletion or merge has a documented retained regression contract.
- Suite documentation describes only boundaries the tests demonstrably execute.
