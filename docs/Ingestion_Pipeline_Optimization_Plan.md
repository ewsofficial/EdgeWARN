# Ingestion Pipeline Optimization Plan (Metrics-Driven)

## Objective
Reduce end-to-end ingestion latency and improve throughput while preserving data completeness and resiliency across MRMS/GOES, RAP, METAR, and NWS alert ingest paths.

## Optimization Rule: No Change Ships Without Measured Improvement
All optimization work must be justified by observed latency metrics collected from the existing pipeline and validated with before/after benchmarks over the same timestamp set.

## Baseline Metrics to Collect First (Required)
Collect baseline metrics across at least **200 ingestion cycles** split across:
- 100 near-real-time cycles
- 100 historical replay cycles

For each source (MRMS/GOES, RAP, METAR, NWS), record:
- p50 / p95 / p99 stage durations (lookup, download, parse/decompress, post-process, persist)
- mean and p95 total source ingest duration
- fallback rate (% cycles using non-exact timestamp)
- error rate (% cycles failed)
- API/listing calls per cycle (S3 list/get, HTTP requests)

### Baseline Snapshot Table (fill with measured values)
| Metric | MRMS/GOES (Baseline) | RAP (Baseline) | METAR (Baseline) | NWS (Baseline) |
|---|---:|---:|---:|---:|
| p95 source ingest duration (ms) | _TBD from benchmark_ | _TBD_ | _TBD_ | _TBD_ |
| p95 lookup/listing duration (ms) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| p95 download duration (ms) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| p95 post-processing duration (ms) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| fallback rate (%) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| error rate (%) | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

## Success Criteria (Derived from Measured Baseline)
After baseline is captured, set numeric targets per source using this rule:
1. **Priority 1 bottlenecks** = any stage contributing >= 25% of p95 source latency.
2. **Required win per optimized stage** = at least 20% p95 reduction.
3. **Required win overall** = at least 15% p95 cycle reduction for the full ingest phase.
4. **Reliability guardrail** = fallback rate and error rate must not worsen by more than 0.5 percentage points.

## Current Constraints and Bottleneck Hypotheses
1. **Uneven parallelism**: Some sources are asynchronous, but key file operations and merges still serialize portions of the pipeline.
2. **High metadata/listing cost**: Repeated S3 listing/lookups per modifier likely dominate latency at scale.
3. **Thread-pool contention**: `run_in_executor` workloads (GLM merge, GeoJSON processing) may compete with other blocking tasks.
4. **I/O overhead**: Repeated open/decompress/write operations and cleanup steps can increase wall-clock time.
5. **Fallback penalty**: Strict timestamp matching can cause expensive lookback scans before fallback selection.

## Implementation Plan

### Phase 1 — Instrumentation and Baseline Benchmark (Week 1)
1. Add structured timing metrics around each ingest stage:
   - lookup/listing
   - download
   - parse/decompress
   - post-processing (merge/map/filter)
   - file persistence
2. Emit per-source and per-product p50/p95/p99 timings and fallback/error counters.
3. Introduce cycle-level trace IDs so all logs can be correlated.
4. Build repeatable benchmark harness:
   - fixed timestamp corpus (same set used for all before/after comparisons)
   - warm-cache and cold-cache runs
   - configurable network profile for stress runs
5. Produce a baseline report with ranked top contributors by absolute p95 milliseconds.

**Deliverables**: instrumentation PR, benchmark harness, baseline report with numeric table populated.

### Phase 2 — Attack Top 2 Measured Bottlenecks First (Week 2)
1. Select only the two highest p95 contributors from Phase 1 data.
2. Implement bounded concurrency controls:
   - global semaphore for external I/O
   - per-provider caps (S3/HTTP/NWS API)
3. Ensure independent source tasks execute concurrently with explicit time budgets.
4. Move CPU-heavy post-processing to dedicated executor pool.
5. Validate each change independently with A/B benchmark output.

**Exit gate**: each selected bottleneck must show >=20% p95 reduction against baseline.

### Phase 3 — Data Access Path Optimization (Week 3)
1. Reduce S3 listing calls via per-cycle prefix cache reuse.
2. Precompute candidate keys for expected minute windows to reduce lookup scans.
3. Batch metadata retrieval where possible to cut round trips.
4. Apply adaptive lookback depth based on measured product staleness patterns.
5. Add short-lived local key cache for near-real-time runs.

**Exit gate**: p95 lookup/listing latency reduced by >=25% for MRMS and RAP; no reliability regression.

### Phase 4 — Post-Processing and I/O Optimization (Week 4)
1. Stream decompression/parsing to reduce disk churn.
2. Optimize GLM merge strategy (chunking/open-count reduction).
3. Use atomic write-then-rename for output artifacts.
4. Remove duplicate filesystem scans/existence checks in hot paths.
5. Tune output compression/chunking using measured write/read tradeoff.

**Exit gate**: p95 post-processing latency reduced by >=20% where targeted.

### Phase 5 — Rollout, Regression Protection, and Continuous Verification (Week 5)
1. Add regression tests for strict timestamp matching, fallback correctness, and schema invariants.
2. Add CI performance checks using a fixed benchmark corpus.
3. Roll out via feature flags (lookup path, concurrency profile, merge strategy).
4. Canary in production and compare canary metrics against control windows.
5. Publish weekly trend reports and rollback if guardrails are breached.

**Exit gate**: overall ingest p95 improved >=15% with no guardrail breach.

## Measurement Protocol (Must Be Followed)
- Compare before/after runs using the **same timestamp corpus**.
- Run each benchmark variant at least **5 repetitions** and report median of p95 values.
- Report both **absolute deltas (ms)** and **relative deltas (%)**.
- Ignore improvements smaller than 5% unless they remove a correctness or stability risk.

## Risks and Mitigations
- **Provider throttling from higher concurrency** → adaptive rate limiting + jittered retries.
- **Memory growth from larger fan-out** → cap in-flight tasks and monitor RSS.
- **Fallback behavior regressions** → golden dataset validation + canary rollout.
- **Complex configuration drift** → centralized `IngestExecutionConfig` with safe defaults.

## Ownership Proposal
- **Ingest Core Owner**: orchestration/concurrency changes.
- **Data Access Owner**: S3/HTTP lookup path optimization.
- **Platform Owner**: telemetry, dashboards, CI performance gates.
- **QA/Validation Owner**: benchmark governance and canary validation.
