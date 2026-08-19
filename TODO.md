# Phase 1 CTAM — verification TODO

## 1. CTAM test suite — done

```
python -m pytest tests/core/ctam -q
```

**637 passed, 7 skipped.** (Was 558 passed/7 skipped before `test_readiness.py`
was filled in; that file went from 17 tests — vocabulary-agreement checks only
— to 79, adding coverage for every descriptor readiness branch (input/
stormcells/cell-history × present/missing/oversized/invalid/stale/wrong-role/
wrong-cycle), `evaluate_requirements` semantics (satisfied, missing,
role_mismatch, not_validated, analysis_time_mismatch, max_age, min_history,
optional-vs-required), `resolve()` fan-out, `cycle_status`/`write_cycle_status`,
the no-recursive-scan guarantee, and frozen-dataclass immutability. This was
the one gap an audit against the Phase 1 acceptance criteria found: "Readiness
accurately changes for present, missing, invalid, stale, wrong-role, and
wrong-cycle files" was correctly implemented in `readiness.py` but had zero
test coverage. It's closed now.)

Frozen Phase 0 contract tests — confirmed unaffected:

```
python -m pytest tests/core/ctam/contract -q
```

**229 passed, 4 skipped.**

## 2. Known pre-existing failures (do not chase these)

- `tests/util/test_io.py` has 5 pre-existing failures unrelated to this work
  (home-directory path assertions — confirmed via `git stash` that they fail
  identically before any Phase 1 change).
- `tests/util/test_runtime.py` has 3 pre-existing multiprocessing/pickle
  failures on Windows, also unrelated.
- 3 nexrad test modules are uncollectible on Windows (`resource` module import).
- Phase 0's recorded baseline (excluding the above): 53 failed, 1209 passed,
  16 skipped, ~41 of the 53 failures traced to the `atomic.py` EBADF defect
  that is now fixed on this branch. **Re-run the whole suite and diff by test
  identity against that baseline** (see `MEMORY.md`: "Environment-based test
  failures" — diff failure sets by test identity, don't just compare counts).

```
python -m pytest -q --ignore=tests/common/ingest/nexrad
```

## 3. Missing test coverage (never written)

- `tests/util/test_ctam_config.py` — resolver precedence tests for
  `src/util/ctam_config.py` (`resolve_ctam_module_dir`: CLI > env > YAML,
  relative-path resolution against repo root, absolute passthrough,
  `export_ctam_module_dir` round-trip via `monkeypatch.setenv`/`delenv`).
- CLI tests in `tests/util/test_io.py` for `--ctam-module-dir`,
  `--list-ctam-modules`, `--check-ctam-modules`: precedence, exit codes on an
  empty root / valid module / invalid manifest, and that both diagnostics
  exit before any pipeline setup runs.

## 4. Baseline regeneration (not done — confirmed via grep, see below)

`tests/config_baseline/environment_variables_python.json` still has **zero**
occurrences of `ctam` — `EDGEWARN_CTAM_MODULE_DIR` was never picked up by the
AST-based env-var scan. `cli_defaults.json` and `cli_shadowing_util_io.json`
each have exactly 1 occurrence (the pre-existing `disable_ctam`), meaning
`--ctam-module-dir`, `--list-ctam-modules`, and `--check-ctam-modules` are
**not yet reflected** in either baseline.

Read `tests/core/config/test_surface_baseline.py` for the exact regeneration
mechanism (env var or config flag — do not guess), then:

```
UPDATE_CTAM_BASELINE=1 python -m pytest tests/core/ctam tests/core/config -q   # or whatever flag test_surface_baseline.py actually documents
```

**Read the resulting diff line by line before trusting it.** In particular:
confirm `--ctam-module-dir` lands in `shadows_yaml` (it shadows
`run.ctam_module_dir`) rather than silently going missing, and confirm
`EDGEWARN_CTAM_MODULE_DIR` appears in `environment_variables_python.json`
with the count going from 17 to 18.

Also run `tests/core/config/test_boundary_audit.py` and
`tests/core/config/test_known_drift.py` — confirm `src/util/ctam_config.py` is
permitted wherever the config-loader import allowlist is enforced.

## 5. Documentation (not started)

- Write `docs/ctam/module-manifest.md` (the manifest reference). Once it
  exists, remove its entry from `FORWARD_REFERENCES` in
  `tests/core/ctam/contract/test_doc_citations.py` — that test fails the
  moment the file exists, by design (see the file's docstring).
- Update `docs/ctam/README.md` / `docs/core/configuration.md` /
  `docs/core/README.md` / top-level `README.md` / `INSTALLATION.md` for the
  new `--ctam-module-dir` / `--list-ctam-modules` / `--check-ctam-modules`
  flags and the `run.ctam_module_dir` config key.
- Re-run `tests/core/ctam/contract/test_doc_citations.py` after any doc edit —
  every citation must resolve to a real file and line.

## 6. Minor nit found during the syntax pass — fixed

`tests/core/ctam/test_readiness.py:368` triggered `SyntaxWarning: invalid
escape sequence '\Z'` — the docstring is now a raw string. Confirmed the
warning no longer appears in a full `tests/core/ctam` run.

## 7. Files touched/created this phase, for reference

Source: `src/EdgeWARN/ctam/{limits,manifest,discovery,readiness,run}.py`,
`src/EdgeWARN/process/integrate/pipeline.py`, `src/util/{ctam_config,io,atomic}.py`,
`.gitignore`, `config/runtime.yaml`, `config/schema/runtime.schema.json`.

Tests: `tests/core/ctam/test_{manifest,discovery,readiness}.py`,
`tests/core/ctam/contract/test_limits_constants.py`,
`tests/fixtures/ctam_modules/{cellstats,brokenmanifest}/`.

Not yet integrated into a commit — everything above is still working-tree
changes on `yuchen-wei3667/modular-ctam`.
