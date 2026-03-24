# EdgeWARN-Core Performance Improvements Implementation Plan

## Goal
Identify one measurable, low-risk optimization for the current pipeline, keep the change small, and verify it with existing profiling hooks before any broader refactor.

## Profiling-Oriented Findings
- [`src/EdgeWARN/pipeline.py`](src/EdgeWARN/pipeline.py) already records phase timings through [`tracker`](src/util/performance.py:129), so the safest Bolt pass is to improve a hot path that can be observed in those integration timings.
- [`main()`](src/EdgeWARN/process/integrate/main.py:13) performs repeated latest-file discovery and then hands each dataset to [`integrate_multi_stats()`](src/EdgeWARN/process/integrate/integrate.py:145).
- The highest-probability bottleneck in the inspected code is the per-cell inner loop inside [`integrate_multi_stats()`](src/EdgeWARN/process/integrate/integrate.py:145), especially repeated polygon preparation and repeated zero-filling work for every stat config.
- Larger ideas already captured in this repository plan, such as process-level parallelism or broader cache architecture, are higher risk and should not be the first Bolt optimization.

## Selected Optimization Target
### Optimize the per-cell integration hot loop in [`integrate_multi_stats()`](src/EdgeWARN/process/integrate/integrate.py:145)

### Why this target
- It sits on the hottest inspected path inside the integration phase.
- It is local to one function and can be implemented cleanly in a small patch.
- It avoids architectural change, dependency changes, and behavior changes.
- It should reduce CPU time across every grouped dataset integration, so the win compounds across a full cycle.

## Planned Change
1. Precompute invariant per-call values once in [`integrate_multi_stats()`](src/EdgeWARN/process/integrate/integrate.py:145), including the list of output keys and a shared zero-assignment helper payload.
2. For each cell, avoid repeating identical fallback loops when a polygon is missing, a subset is empty, or masked values are empty.
3. Prepare the cell geometry once before the point-in-polygon test so repeated geometric checks are cheaper for [`sv.contains`](src/EdgeWARN/process/integrate/integrate.py:257).
4. Keep the existing lazy-loading behavior for NetCDF and existing GRIB behavior unchanged.
5. Add a short optimization comment explaining that the loop reduces repeated per-cell work without changing computed outputs.

## Expected Performance Impact
- Lower CPU overhead in the integration phase by removing repeated Python-level loops from every storm-cell/stat combination.
- Best impact when many cells are processed across many grouped datasets in the same run.
- Expected measurable signal: reduced timings in the integration subtimers emitted from [`main()`](src/EdgeWARN/process/integrate/main.py:47) through [`main()`](src/EdgeWARN/process/integrate/main.py:157).

## Measurement Plan
1. Capture a baseline profile using the existing timing output in [`_write_profile_summary()`](src/EdgeWARN/pipeline.py:51).
2. Implement the small hot-loop optimization in [`integrate_multi_stats()`](src/EdgeWARN/process/integrate/integrate.py:145).
3. Re-run the same integration-oriented workflow and compare the `Integration` subtimers.
4. Document the expected impact in code comments and in the final summary.

## Verification Plan
- Run project lint/test equivalents required by the repository workflow before finalizing.
- At minimum, run the relevant test suite plus the repository lint command from [`package.json`](package.json).
- Confirm no output schema changes in the generated storm-cell JSON path handled by [`write_json()`](src/EdgeWARN/process/integrate/utils.py:172).

## Out of Scope For This Bolt Pass
- Parallel dataset integration
- New caching subsystems
- Dependency additions
- Changes to [`package.json`](package.json) or TypeScript configuration
- Broader CTAM architectural refactors

## Approval Gate
This plan is ready for review. After approval, the next step is switching to [`code`](code:1) mode to implement the selected hot-loop optimization and then verify it with lint, tests, and timing comparison.
