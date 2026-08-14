# NEXRAD Pipeline Latency Benchmark Plan

**Status:** planning only; this document does not implement the benchmark.

## Objective

Add a repeatable real-time NEXRAD ingest benchmark that runs for a configured
duration and persists one JSON result file per radar site.  The benchmark must
report:

- per-elevation latency: the UTC time at which parsing/export of an elevation
  finishes minus that elevation's radar timestamp; and
- per-volume latency: the UTC time at which the volume becomes parse-complete
  minus the volume timestamp.

This measures Level-II ingest through parse/export only.  It excludes EWMRS
NEXRAD rendering, API indexing, and client delivery latency.

## Current-state evidence

- `src/common/ingest/nexrad/pipeline/__init__.py` owns the continuous
  `NexradRealtimeIngestionPipeline` loop.  Its CLI supports site filtering and
  polling intervals, but it has no run-duration option or benchmark output.
- `src/common/ingest/nexrad/service.py` calls `_run_worker_parse()` after
  stream checkpoints, a detected volume boundary, and the final pending chunk.
  The parent receives `WorkerParseResult.saved_elevations` after the worker has
  returned.
- `src/common/ingest/nexrad/worker.py:parse_and_export()` calls
  `write_elevation_artifacts()` once a complete elevation group is visible.
- `src/common/ingest/nexrad/writer.py:write_elevation_artifacts()` already
  records `ElevationArtifact.elevation_timestamp`, `scan_timestamp`,
  `volume_id`, and `file_written_at`; the latter is currently a seconds-only
  UTC string.  The per-elevation manifest serializes that artifact.
- A volume is currently considered complete in `NexradIngestService` only when
  all `INGEST_READINESS_ELEVATION_IDS` have a local public artifact.  The site
  manifest is then written, but it does not retain an explicit parse-complete
  timestamp.

## Metric definitions

All timestamps must be timezone-aware UTC ISO-8601 values with millisecond (or
better) precision.  Never use file mtime, local time, `time.monotonic()`, or
the polling-loop observation time as the parse finish time.

| Metric | Event timestamp | Reference timestamp | Calculation |
| --- | --- | --- | --- |
| Elevation latency | `parse_finished_at` recorded immediately after an elevation's artifact and manifest have been successfully written | `elevation_timestamp` (the elevation's radar/last-sweep timestamp) | `parse_finished_at - elevation_timestamp` |
| Volume latency | `volume_parse_finished_at` recorded immediately after the pipeline has verified all readiness elevations and written the site manifest | `scan_timestamp` / volume timestamp | `volume_parse_finished_at - scan_timestamp` |

`parse_finished_at` is an ingest/parser completion signal, not the time the
remote object was discovered, downloaded, first parsed, rendered, or consumed
by the benchmark.  `volume_parse_finished_at` is intentionally the end of the
complete volume's parsing, not the finish time of its last chunk download.

If either reference timestamp is absent or invalid, persist the event with
`latency_seconds: null` and a machine-readable exclusion reason.  Do not
silently substitute `download_started_at`, the filename timestamp, or the
benchmark start time.

## Proposed implementation

### 1. Make completion timestamps precise and durable

1. Add a shared UTC timestamp helper in the NEXRAD writer/service boundary
   that emits ISO-8601 UTC with milliseconds, for example
   `2026-08-13T17:42:31.482Z`.  Preserve parsing compatibility with existing
   second-resolution manifests.
2. Rename or add fields without changing the meaning of existing public fields:
   retain `file_written_at` for compatibility, and add
   `parse_finished_at` to `ElevationArtifact` and its per-elevation manifest.
   Set both from the same instant after the data artifact and elevation manifest
   write have succeeded.
3. When `_stream_ingest_volume()` and `_stream_ingest_volume_async()` establish
   `complete`, capture `volume_parse_finished_at` immediately after
   `write_site_manifest()` succeeds.  Extend the site manifest payload with
   `current_volume_parse_finished_at` (and retain existing keys).
4. Do not emit a volume-complete event on a partial/retried volume, a parser
   error, worker timeout, invalid VCP, or a local-complete skip.  The benchmark
   may separately count these as exclusions/errors.

### 2. Add a bounded benchmark runner

Create `tests/benchmarks/benchmark_nexrad_pipeline_latency.py`, following the
subprocess ownership and graceful shutdown approach in
`tests/benchmarks/benchmark_nexrad_realtime_memory.py`.

Its CLI should include:

```text
--duration-seconds SECONDS          required positive wall-clock duration
--site SITE                         repeatable; passed to the realtime pipeline
--base-dir PATH                     runtime base directory
--output-dir PATH                   default: <base-dir>/data/benchmarks/nexrad-latency
--scan-interval-seconds SECONDS     default: pipeline default
--completion-interval-seconds SECONDS
--max-candidate-volumes-per-site N
--poll-interval-seconds SECONDS     manifest observation cadence; default 0.25
```

The runner must launch `run_realtime_ingestion_pipeline` in a child process,
wait only for the requested duration, send `SIGINT`, wait a bounded grace
period, then use `SIGTERM` only if needed.  It must record an interrupted or
failed child explicitly, retain a bounded tail of child output for diagnosis,
and shut down parser children along with the pipeline.  It must not modify
production `run_forever()` just to impose the benchmark duration.

The benchmark should poll only the per-elevation and site manifests under the
configured base directory.  On every poll it should:

1. Read each manifest defensively; ignore an incomplete/invalid JSON read and
   retry on the next poll.
2. De-duplicate elevation records by
   `(site, volume_id, elevation, elevation_timestamp, parse_finished_at)` and
   volume records by `(site, volume_id, volume_parse_finished_at)`.
3. Parse timestamps as UTC, calculate the defined latency, and retain both raw
   timestamps and the derived seconds.
4. Attribute records to the site named in the manifest, rather than assuming
   the requested site list is exhaustive.

To prevent historical runtime artifacts from contaminating a run, record the
benchmark's UTC start time and accept only parse-completion events at or after
that instant.  Start with a clean, dedicated benchmark base directory or
document that stale records are excluded; never delete a general production
base directory as setup.

### 3. Persist one JSON file per site

Write atomically (temporary sibling followed by replace) at the end of the
run, and optionally checkpoint atomically during long runs, to:

```text
<output-dir>/<SITE>.json
```

The file name must use normalized uppercase four-character site IDs.  A run
timestamp is included inside the document rather than in the filename so each
site has the requested site-specific canonical result file.  Add
`--append` only if retaining multiple runs in the same file is needed; default
behavior replaces the file with one self-contained run.

Each result file should use this shape (illustrative values only):

```json
{
  "schema_version": 1,
  "site": "KTLH",
  "benchmark": {
    "started_at": "2026-08-13T17:00:00.000Z",
    "finished_at": "2026-08-13T17:30:00.000Z",
    "requested_duration_seconds": 1800,
    "actual_duration_seconds": 1800.24,
    "pipeline_exit": {"signal": "SIGINT", "return_code": 0}
  },
  "configuration": {
    "base_dir": "/path/to/runtime",
    "scan_interval_seconds": 20,
    "completion_interval_seconds": 10
  },
  "elevations": [
    {
      "volume_id": "...",
      "elevation": "0.5",
      "elevation_timestamp": "2026-08-13T17:02:31.000Z",
      "parse_finished_at": "2026-08-13T17:03:12.482Z",
      "latency_seconds": 41.482
    }
  ],
  "volumes": [
    {
      "volume_id": "...",
      "volume_timestamp": "2026-08-13T17:00:00Z",
      "volume_parse_finished_at": "2026-08-13T17:05:09.214Z",
      "latency_seconds": 309.214,
      "readiness_elevations": ["0.5", "0.9", "1.3"]
    }
  ],
  "summary": {
    "elevation_count": 0,
    "volume_count": 0,
    "excluded_elevation_count": 0,
    "excluded_volume_count": 0,
    "elevation_latency_seconds": {"min": null, "p50": null, "p95": null, "max": null},
    "volume_latency_seconds": {"min": null, "p50": null, "p95": null, "max": null}
  },
  "exclusions": []
}
```

Compute percentile summaries from non-null values using a documented nearest-
rank or interpolated method, and include the method in `schema_version` docs
or the output metadata.  Empty samples must use `null`, not zero, for latency
summary values.

## Test plan

Add focused pytest coverage, without requiring live AWS/weather.gov access:

1. Timestamp tests verify UTC parsing, fractional-second preservation, correct
   elevation and volume subtraction, and `null` plus an exclusion for missing,
   malformed, or negative/unreasonable reference times.
2. Writer/service tests verify elevation `parse_finished_at` is persisted only
   after a successful export and volume completion is persisted only after all
   readiness elevations and successful site-manifest publication.
3. Collector tests use temporary site directories and manifests to verify
   deduplication, uppercase per-site file names, stale-event filtering,
   malformed-manifest retry behavior, and atomic JSON output.
4. Runner tests replace the subprocess/pipeline with a controllable fake to
   verify configurable duration, `SIGINT` first, bounded fallback termination,
   and accurate actual duration/exit metadata.
5. Run the relevant suite in the `EdgeWARN-dev` environment, for example:

```bash
conda run -n EdgeWARN-dev python -m pytest \
  tests/benchmarks tests/core tests/integration -k nexrad
```

## Acceptance criteria

- A user can choose a positive benchmark duration and one or more sites.
- Every valid exported elevation during the run appears exactly once in its
  site's JSON with `parse_finished_at`, elevation timestamp, and calculated
  latency.
- Every successfully complete volume during the run appears exactly once in
  its site's JSON with parse-complete time, volume timestamp, and calculated
  latency.
- JSON is valid after interruption and no result mixes events from before the
  run start.
- Missing timestamps and incomplete volumes are visible as exclusions rather
  than fabricated latency values.
- The benchmark leaves normal production pipeline behavior, data retention,
  EWMRS rendering, and API contracts unchanged.
