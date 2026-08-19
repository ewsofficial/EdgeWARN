# Source Configuration Extraction Completion Plan

## Purpose

Complete the outstanding acceptance criteria in
`plans/source-configuration-extraction-plan.md` identified by the post-migration
audit. This plan is limited to the remaining defects in base-directory ownership,
Python/Node loader parity, startup validation, catalog consistency coverage, and
operator documentation.

It does not expand scope into CTAM configuration, `colormaps.json` relocation,
`mappings.json` reconciliation, stable product IDs, or the code-owned RAP Uint16
display registry.

## Current Gaps

1. `filesystem.yaml` does not own the platform base-directory candidates; they
   remain in `src/util/file.py`, while Node duplicates them in `api.yaml`.
2. Python does not apply the `EDGEWARN_BASE_DIR` compatibility override, and Node
   treats conflicting CLI and environment base directories as an error instead of
   applying CLI > environment > YAML.
3. Startup paths do not validate the complete selected configuration tree before
   filesystem initialization, workers, or listeners are created.
4. The Node loader differs from Python for explicit config-root validation and
   symlink containment. Both validators accept non-finite numbers.
5. Required MRMS render-to-ingest, integration-source, full cross-language, and
   fixture-level behavioral checks are incomplete.
6. Configuration discovery, deployment, precedence, and authoritative-setting
   documentation is missing or stale.

## Phase 1: Unify Base-Directory Resolution

1. Extend `config/filesystem.yaml` and `config/schema/filesystem.schema.json`
   with the sole base-directory defaults:
   - POSIX default.
   - Windows default.
   - Any repository/install fallback only if it is an existing supported runtime
     behavior.
   - The `data`, `gui`, and `wpc` derived path templates, if they need to be
     exposed to Node rather than derived by `util.file`.
2. Remove `server.base_dir` from `config/api.yaml` and its schema. Do not retain
   it as a compatibility copy: Node must resolve the same `filesystem.yaml`
   values as Python.
3. Add a shared Python resolver in the configuration overlay layer that receives
   explicit CLI input, environment names, and the loaded filesystem settings.
   It returns the selected base directory and provenance using exactly:
   CLI > environment > YAML.
4. Change `IOManager.get_base_dir_arg()` and the early `util.file` binding path to
   use the resolver. Preserve `--base_dir` and `--base-dir` as aliases and add
   `EDGEWARN_BASE_DIR` as the Python compatibility environment override.
5. Change `src/api/config/index.js` to resolve the base directory through the
   shared Node loader/overlay using `filesystem.yaml`. An explicit API CLI value
   wins over `EDGEWARN_BASE_DIR` and legacy `BASE_DIR`; lower-priority values must
   not cause a conflict error merely because they differ.
6. Keep the existing derived artifact-directory names in `src/util/file.py`. The
   config value selects the base only; it must not create a second directory-name
   catalog.

### Phase 1 Acceptance

- `filesystem.yaml` is the only configuration file containing platform base
  directory defaults.
- Python and Node produce identical selected base directories and provenance for
  YAML-only, environment-only, CLI-only, and CLI-plus-environment inputs.
- Both POSIX and Windows path construction tests pass without depending on the
  current working directory.
- Import-time filesystem initialization still honors explicit base/config roots.

## Phase 2: Close Loader Parity and Startup Safety

1. In both schema walkers, reject non-finite `number` values before evaluating
   minimum, maximum, or other numeric constraints. Cover `NaN`, positive
   infinity, and negative infinity.
2. Make `src/config/loader.js` apply the same config-root validity rule as
   `src/common/config/loader.py`: an explicit root must contain `runtime.yaml`
   and each requested configuration file/schema must be present.
3. Harden Node `expandPath` containment by resolving real paths for existing
   token roots and candidate components, then rejecting a candidate that escapes
   its token root through a symlink. Preserve the current lexical rejection of
   traversal, backslashes, NULs, and bare relative paths. Define the behavior for
   a not-yet-existing leaf by resolving its nearest existing parent before joining
   the unresolved suffix.
4. Add an application-level `validateAllConfigs` API in each loader. It must load
   all 19 files and schemas with the selected root, cache the immutable results,
   and return a single actionable validation error before domain startup.
5. Invoke the Python all-config validation after command-line/config-root
   discovery but before filesystem setup, background loops, process pools, or
   child workers. Retain memoization so spawned children do not repeatedly parse
   configuration.
6. Invoke the Node all-config validation before constructing the API application
   or opening a listener. Continue to load individual immutable documents from
   the same cache after validation.
7. Ensure validation does not require runtime artifact directories to exist and
   does not create them as a side effect.

### Phase 2 Acceptance

- Python and Node reject the same malformed documents and report file plus dotted
  key/index.
- An invalid selected config root prevents directory creation, child processes,
  and HTTP listeners.
- Node rejects a symlink escape just as Python does.
- Explicit config roots missing `runtime.yaml` fail in both runtimes.

## Phase 3: Complete Configuration Acceptance Coverage

1. Extend `tests/core/config/test_catalog_invariants.py` so every MRMS render
   layer maps to a product in `mrms.products`, including the configured source
   field where a render name differs from the ingest product name.
2. Add the required integration-source invariant: every
   `integration.stats_datasets[*].source` resolves to an available configured
   MRMS, GOES, RAP, GLM, or other declared source. Make the source authority
   explicit in the test rather than inferring one parallel catalog from another.
3. Strengthen Node/Python product consistency tests to compare ordered product
   names and output-directory attribute names, not only catalog counts.
4. Add a cross-language default-document parity test. Load each repository YAML
   document through Python and Node, serialize canonical JSON, and compare the
   effective documents. Add malformed fixtures that compare acceptance/rejection
   and key-path errors for the supported schema keyword set.
5. Add base-directory precedence tests in both language suites for YAML only,
   each legacy environment alias, each CLI alias, and CLI over conflicting env.
6. Add path-validation tests for non-finite numbers, missing explicit roots,
   symlink escapes, and a non-existing final path beneath a valid root.
7. Add fixed-fixture behavioral regression tests for the newly configured seams:
   - MRMS/GOES/RAP source key generation and retention selection at boundaries.
   - Detection masks/cells and tracking/lineage assignments.
   - Integration statistics and configured rounding.
   - EWMRS float16 chunk metadata/payload selection and Node API product results.
   Use existing checked-in baseline fixtures where available; add small,
   deterministic fixtures only where none exist. Do not require live NOAA/AWS
   access.
8. Keep `test_boundary_audit.py` as the policy guard. Add narrowly targeted audit
   rules only if the fixes introduce a new configurable category; do not broaden
   it into a heuristic that flags intentional code constants.

### Phase 3 Acceptance

- All catalog invariants listed under the original plan's validation section are
  directly asserted.
- Python and Node have parity for repository defaults and supported invalid
  documents.
- Fixture tests demonstrate value-preserving behavior at catalog/config seams,
  not only accessor or count parity.

## Phase 4: Synchronize Documentation

1. Create `docs/core/configuration.md` as the operator reference. For each of the
   19 YAML files, identify ownership, key descriptions, units/ranges, restart
   requirement, and relevant CLI/environment override.
2. Update `INSTALLATION.md` with configuration-root discovery, `--config-dir`,
   `EDGEWARN_CONFIG_DIR`, base-directory aliases, the CLI > environment > YAML
   precedence order, `npm run validate-config`, Python validation, and deployment
   copying of the whole `config/` tree.
3. Update `README.md` and `docs/core/README.md` to describe the 19-file tree and
   float16 chunk artifacts rather than PNG tiles.
4. Update `CONFIGURATION_AUDIT.md` to remove obsolete statements and reflect the
   current NEXRAD, METAR, WPC, RAP, GOES, and artifact-format facts enumerated in
   the original plan.
5. Update the relevant ingest, detection, integration, and API documents to name
   their authoritative configuration file. Document legacy aliases and any
   deprecation window without changing their precedence.

### Phase 4 Acceptance

- Documentation identifies one authoritative file for every operator-facing
  setting in scope.
- Installation instructions permit launching from outside the repository with an
  explicit config root and base directory.
- No documentation claims float16 chunk output is PNG tile output.

## Implementation Order

1. Complete Phase 1 and its unit tests before changing startup behavior.
2. Complete Phase 2 before adding behavioral fixtures, so failures are reported
   before any runtime work begins.
3. Complete Phase 3 using the unified resolver and validators as the fixed
   contract.
4. Complete Phase 4 in the same change set as the final public behavior, then
   run the complete validation matrix.

## Verification Matrix

Run after each relevant phase:

```bash
npm run validate-config
PYTHONPATH=src python -m common.config.validate
python -m pytest tests/core/config
npm test -- --runTestsByPath tests/api/test_config_loader.js
```

Before declaring the extraction plan complete, also run the focused detection,
integration, EWMRS, and API fixture suites added in Phase 3, followed by the full
repository test suites where the required Conda environment and Node dependencies
are available:

```bash
python -m pytest tests/
npm test
```

## Completion Checklist

- [ ] `filesystem.yaml` is the only base-directory default authority.
- [ ] Python and Node enforce CLI > environment > YAML for base directories.
- [ ] Both loaders reject non-finite numbers, invalid explicit roots, and symlink
  path escapes consistently.
- [ ] Each root process validates all 19 documents before runtime side effects.
- [ ] Missing catalog and cross-language parity assertions are present.
- [ ] Fixture-level behavior equivalence covers configured production seams.
- [ ] Operator and architecture documentation is current and authoritative.
- [ ] All commands in the verification matrix pass.
