# Changelog

## [3.0.0] 2026-08-20

### Added
- Unified, secured `/api/v3` service for EdgeWARN and EWMRS artifacts, with an
  OpenAPI contract, cursor-based collection responses, problem-detail errors,
  request IDs, conditional caching, and weather, analysis, render, radar, RAP,
  WPC, and colormap resources, including native WPC GeoJSON delivery. It adds
  configured security headers, strict proxy/origin handling, rate limiting,
  safe access logging, and artifact-path containment; consolidates the former
  Node servers; and retains deprecated v2 and product-route compatibility
  adapters.
- EWMRS binary chunk delivery: renders now publish sparse RGBA artifacts and
  gzip-compressed float16 value chunks with versioned indexes and metadata for
  client-side colormapping and GOES RGB composition.
- Schema-validated YAML configuration catalogs for runtime, historical,
  processing, ingest, rendering, API, filesystem, and tracking settings.
  Deployments can select a complete catalog tree with `--config-dir` or
  `EDGEWARN_CONFIG_DIR` and inspect effective configuration provenance. The
  catalogs are the sole source of the corresponding operational defaults.
- External CTAM module support with manifest discovery, declared-input
  readiness checks, a loopback internal API and SDK, cycle-scoped transactions,
  ordered alert publication, persistent journals, and per-module outcome
  reporting. StormCast now runs as a built-in module through the same host
  boundary.
- Coherent cycle input manifests and transactional runtime-artifact publication
  to preserve consistent detection, integration, and rendering outputs.
- Production dependency-audit and SBOM npm scripts.

### Changed
- StormCast emits `tstm_wind: "false"` when no wind assessment is available.

### Removed
- Removed the bundled MorphoWind CTAM assessment.
- Removed the legacy in-process CTAM registry and grid-module execution path;
  grid analytics use the cycle-scoped external module API.
- Removed server-side GOES RGB composite rendering; clients compose RGB from
  ABI channel data delivered through EWMRS binary chunks.
- Deprecated legacy EWMRS PNG routes in favor of binary chunk delivery.
- Deprecated `/api/v2`, render, WPC, colormap, health, RAP, and NEXRAD API
  routes as compatibility adapters; legacy v1-style `/features` and `/data`
  paths now return `410 Gone`.

### Fixed
- Corrected historical and single-frame processing semantics and prevented
  unavailable RAP data from stalling integration.
- Stabilized NEXRAD worker lifecycle, bounded realtime work, and improved
  recovery from stalled workers; corrected WPC ownership.
- Restored tracking assignment fallback behavior and corrected GOES render
  resampling.

### Testing
- Added API contract, security, compatibility, and production-readiness
  coverage for the unified service.
- Added configuration catalog, schema, override, provenance, and source-boundary
  regression coverage.
- Added CTAM internal-API, manifest, readiness, transaction, publication,
  built-in/external module, and performance coverage.
- Expanded runtime, ingestion, EWMRS chunk serialization, NEXRAD supervision,
  historical-processing, and tracking regression tests and benchmarks.
