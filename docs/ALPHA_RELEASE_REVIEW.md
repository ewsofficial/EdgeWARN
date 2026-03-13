# EdgeWARN-Core Alpha Release Readiness Review

Date: 2026-03-13
Reviewer: Codex (automated + manual static review)

## Executive Verdict

**Not ready for alpha release yet**.

The repository shows strong progress in API test quality and security middleware defaults, but there are release-blocking issues around Python test reproducibility, runtime safety in one RAP integration path, and release/process consistency.

## Scope of Review

- Node.js API server and route layer
- Python processing pipeline hotspots (integration and ingest touchpoints)
- Test infrastructure and reproducibility
- Release hygiene (versioning/docs/process indicators)

## Checks Performed

1. `npm test`
2. `pytest tests -q`
3. `pytest tests/util/test_file.py tests/util/test_io.py -q`
4. `npm audit --omit=dev`
5. Manual source review of API/server config, RAP integration, and project metadata

## What Looks Good

- **API test suite is green**: 7/7 suites and 94/94 tests passed locally via Jest.
- **Baseline API hardening exists**:
  - Helmet enabled with HSTS and CSP.
  - CORS is explicit and defaults to deny in production when origins are not set.
  - Rate limiting is configured and uses `ipKeyGenerator`.
- **Modernized v2 API structure** and explicit v1 deprecation behavior are present.

## Release-Blocking Findings

### 1) Python test environment is not reproducible from default runtime

- Running `pytest tests -q` fails during collection with 41 import errors (e.g., `numpy`, `shapely`, `psutil`).
- `environment.yml` does list these packages, but the default execution environment in this review did not satisfy them.
- `pytest.ini` declares `asyncio_mode = auto`, but current environment emits an unknown-option warning, indicating `pytest-asyncio` mismatch/absence.

**Risk**: Inability to reliably run the full Python quality gate in common environments (including CI if not pinned/configured) is a release blocker.

### 2) Runtime expression evaluation in RAP integration

- `integrate_rap.py` computes derived fields with `eval(formula, {"__builtins__": {}}, props)`.
- Even with stripped builtins, this is still risky and brittle if formula sources expand or become externally influenced.

**Risk**: Security and reliability concern in a core data pipeline path.

### 3) Debug prints in production pipeline code

- `integrate_rap.py` includes unconditional debug `print(...)` calls around extractor initialization and batch extraction.

**Risk**: Log noise in production, potential observability dilution, and operational signal-to-noise degradation.

### 4) Release metadata/process inconsistency

- `package.json` version is `2.0.0-alpha`.
- `CHANGELOG.md` has a `2.0.0-alpha` entry.
- `README.md` still reports current release as `1.5.4`.

**Risk**: Confusing release communication for testers and adopters; increases deployment/operator error probability.

### 5) No visible CI workflow in-repo

- No `.github/workflows/*` pipeline found in repository.

**Risk**: No enforced, centralized quality gate for alpha branch hardening.

## Non-Blocking Observations

- API config creates many data directories at import/startup time. This is convenient but introduces side effects during tests and startup in non-production contexts.
- `npm audit --omit=dev` could not complete due registry endpoint access failure (`403 Forbidden`) in this environment, so dependency vulnerability status remains unverified here.

## Alpha Readiness Scorecard

- Functional API tests: **Pass**
- Python test gate: **Fail**
- Security baseline (API middleware): **Pass**
- Security posture (pipeline eval usage): **Fail**
- Release/documentation consistency: **Fail**
- CI enforcement evidence: **Fail**

**Overall: NOT READY**

## Recommended Minimum Actions Before Alpha

1. Ensure Python test environment reproducibility in CI and developer bootstrap:
   - Install and pin required Python dependencies in CI (`numpy`, `shapely`, `psutil`, etc.).
   - Add/pin `pytest-asyncio` or remove unsupported pytest config options.
2. Replace `eval`-based derived RAP formulas with a safe expression evaluator or explicit mapping.
3. Remove/guard debug prints in core integration paths; route through structured logger.
4. Align release metadata across `README.md`, package version, and changelog.
5. Add CI workflows (Node tests + Python tests + lint/type/security checks).

## Suggested Go/No-Go Decision

**No-Go for alpha release today**. Proceed after the five minimum actions above are completed and verified with green CI.
