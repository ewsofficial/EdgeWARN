"""
CTAM Pipeline Entry Point

Provides a single entry point for running all registered CTAM modules
on storm cell data.

Supports both:
- Cell-based modules: Operate on storm cells and modify them in-place
- Grid-based modules: Operate on raster data (GRIB/NetCDF) and produce GeoJSON
"""

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import Counter
from .registry import CellModuleRegistry, GridModuleRegistry, ModuleRegistry
from .engine import initialize_modules
from EdgeWARN.alerts import AlertManager
from .util.history_cache import CellHistoryCache
from . import discovery, readiness
from common.ingest.manifest import CycleInputManifest

# Import modules to trigger auto-registration
from . import modules  # noqa: F401

# File system utilities
import util.file as fs


def _run_phase1_discovery_dry_run(
    cells: List[Dict[str, Any]],
    timestamp: str,
    json_path: Optional[str],
    input_manifest: Optional[CycleInputManifest],
) -> None:
    """Discover external modules and record per-cycle readiness. Launches nothing.

    Phase 1 of plans/modular-ctam-internal-api-plan.md adds discovery and
    readiness without execution -- the legacy registry-based modules below are
    untouched. This dry run must never block them, so every failure here is
    caught and logged rather than raised.
    """
    try:
        started_at = datetime.now(timezone.utc)
        catalog = readiness.build_catalog(
            cells=cells,
            timestamp=timestamp,
            stormcell_path=json_path,
            input_manifest=input_manifest,
        )
        discovery_result = discovery.discover_modules()

        evaluations = {}
        for module in discovery_result.modules:
            if module.manifest is None:
                continue
            evaluations[module.module_id] = readiness.evaluate_requirements(
                module.manifest, catalog
            )

        status = readiness.cycle_status(
            catalog=catalog,
            discovery=discovery_result,
            evaluations=evaluations,
            state=readiness.CYCLE_STATE_REQUIREMENTS_EVALUATED,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
        )
        readiness.write_cycle_status(status)
        print(
            f"[CTAM] Phase 1 discovery: root={discovery_result.root} "
            f"root_present={discovery_result.root_present} "
            f"runnable={len(discovery_result.runnable)}/{len(discovery_result.modules)}"
        )
    except Exception as e:
        print(f"[CTAM] Phase 1 discovery/readiness dry run failed: {e}")


def _run_external_modules(cells, timestamp, json_path, input_manifest, *, disabled=False):
    """Execute only already-discovered manifests; legacy built-ins stay separate."""
    if not timestamp or disabled:
        return cells
    try:
        from .runner import ExternalModuleRunner
        catalog = readiness.build_catalog(cells=cells, timestamp=timestamp, stormcell_path=json_path, input_manifest=input_manifest)
        discovered = discovery.discover_modules()
        manifests = {item.module_id: item.manifest for item in discovered.runnable if item.manifest is not None}
        if not manifests:
            return cells
        runner = ExternalModuleRunner(catalog=catalog, cells=cells, manifests=manifests)
        results = runner.run()
        result_states = {result.module_id: result.state for result in results}
        evaluations = {module_id: readiness.evaluate_requirements(manifest, catalog) for module_id, manifest in manifests.items()}
        required_failed = any(manifests[result.module_id].required and result.state not in {"completed", "skipped_missing_requirements"} for result in results)
        readiness.write_cycle_status(readiness.cycle_status(
            catalog=catalog, discovery=discovered, evaluations=evaluations,
            state=readiness.CYCLE_STATE_FAILED if required_failed else readiness.CYCLE_STATE_COMPLETED,
            started_at=datetime.now(timezone.utc), finished_at=datetime.now(timezone.utc),
            module_states=result_states,
        ))
        for result in results:
            print(f"[CTAM] External module {result.module_id!r}: {result.state} ({result.duration_seconds:.3f}s)")
            if result.state != "completed" and manifests[result.module_id].required:
                raise RuntimeError(f"required external CTAM module {result.module_id!r} {result.state}: {result.reason}")
        # Only sealed transactions have made it into the runner's working set.
        by_id = runner.transactions.cells
        return [by_id.get(str(cell.get("id")), cell) for cell in cells]
    except Exception as exc:
        print(f"[CTAM] External module execution failed: {exc}")
        return cells


def _run_builtin_stormcast(cells, history_cache):
    """Run the reserved built-in before any discovered external module.

    StormCast is deliberately not obtained from the import-time registry here:
    an operator-installed manifest cannot shadow it and every data dependency
    crosses the same narrow host-service boundary.
    """
    from .builtins import BuiltinStormCastAdapter, StormCastCycleService

    adapter = BuiltinStormCastAdapter(StormCastCycleService(history_cache))
    success_count = error_count = alert_count = 0
    for cell_idx, cell in enumerate(cells):
        cell.setdefault("modules", {})
        try:
            adapter.run(cell)
            success_count += 1
        except Exception as exc:
            cell["modules"][adapter.name] = {"status": "error", "error": str(exc)}
            print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: built-in StormCast FAILED: {exc}")
            error_count += 1
            continue
        try:
            alert_count += adapter.publish_alerts(adapter.alerts(cell))
        except Exception as exc:
            print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: StormCast alerts FAILED: {exc}")
    return success_count, error_count, alert_count


def run_ctam(
    cells: List[Dict[str, Any]],
    timestamp: Optional[str] = None,
    *,
    json_path: Optional[str] = None,
    input_manifest: Optional[CycleInputManifest] = None,
    disable_ctam_modules: bool = False,
) -> List[Dict[str, Any]]:
    """
    Run all registered CTAM modules on the provided storm cells.

    First runs cell-based modules, then runs grid-based modules.
    Grid results are attached to the first cell for output.

    This is the single entry point for CTAM processing in the pipeline.
    Modules are automatically discovered from the registry.

    Args:
        cells: List of storm cell dictionaries, each with 'properties' key.
        timestamp: Optional scan timestamp (e.g. YYYYMMDD-HHMMSS) to save as an alert snapshot.
        json_path: Optional path to this cycle's stormcell snapshot, used only to
            report its readiness in the Phase 1 discovery dry run below.
        input_manifest: Optional pinned CycleInputManifest, used only to build the
            Phase 1 read-only file catalog. Never executes module code.

    Returns:
        The same list of cells with 'modules' populated by each registered module.
    """
    start_time = time.time()

    if timestamp:
        _run_phase1_discovery_dry_run(cells, timestamp, json_path, input_manifest)

    # Clean up expired alerts before running modules
    # This prevents expired alerts from piling up on disk
    try:
        AlertManager.cleanup_expired()
    except Exception as e:
        print(f"[CTAM] Failed to clean up expired alerts: {e}")
    
    # GeoMapper is now integrated into NWS Ingest, so we don't need to run it here.
    # Data in fs.MRMS_NWS_DIR is already processed with polygons.

    print("[CTAM] Starting CTAM pipeline...")
    
    # Get registered modules
    # StormCast is the reserved built-in and runs through the host service
    # boundary below. Remaining legacy registry entries stay temporarily for
    # Phase 6 removal, but cannot control StormCast ordering or availability.
    cell_modules = {
        name: module for name, module in CellModuleRegistry.get_all().items()
        if name.casefold() != "stormcast"
    }
    grid_modules = GridModuleRegistry.get_all()
    
    print(f"[CTAM] Cell modules: ['StormCast', *{list(cell_modules.keys())}]")
    print(f"[CTAM] Grid modules: {list(grid_modules.keys())}")
    print(f"[CTAM] Processing {len(cells)} storm cell(s)...")
    
    # Step 1: Run cell-based modules
    
    # Pre-initialize history cache
    hist_cache = CellHistoryCache()
    active_cell_ids = [c["id"] for c in cells if "id" in c]
    hist_cache.preload_active(active_cells=active_cell_ids)
    
    cell_success_count, cell_error_count, builtin_alert_count = _run_builtin_stormcast(cells, hist_cache)
    
    for cell_idx, cell in enumerate(cells):
        # StormCast already populated its reserved namespace through the host
        # service. Initialize only the temporary legacy module namespaces.
        module_names = list(cell_modules.keys())
        initialize_modules(cell, module_names)
        pending_cell_alerts = []
        
        # Run each cell-based module
        for module in cell_modules.values():
            try:
                module_start = time.time()
                module.run(cell, environment=None, history_cache=hist_cache)
                module_elapsed = time.time() - module_start
                cell_success_count += 1
            except Exception as e:
                # Store error in modules dict
                cell["modules"][module.name] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Module '{module.name}' FAILED: {e}")
                cell_error_count += 1

            # Collect and publish alerts from cell module
            try:
                module_alerts = module.alerts(cell)
                if module_alerts:
                    pending_cell_alerts.extend(module_alerts)
            except Exception as e:
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Alerts from '{module.name}' FAILED: {e}")

        if pending_cell_alerts:
            AlertManager.publish_many(pending_cell_alerts)

    stormcast_status_counts = {}
    stormcast_alert_eligibility_counts = {
        True: 0,
        False: 0,
        None: 0,
    }
    stormcast_alert_outcome_counts = Counter()
    stormcast_alert_blocker_counts = Counter()

    for cell in cells:
        stormcast_result = cell.get("modules", {}).get("StormCast")
        if not stormcast_result:
            continue

        status = stormcast_result.get("status", "unknown")
        stormcast_status_counts[status] = stormcast_status_counts.get(status, 0) + 1

        eligibility = stormcast_result.get("can_generate_alerts")
        if eligibility is True:
            stormcast_alert_eligibility_counts[True] += 1
        elif eligibility is False:
            stormcast_alert_eligibility_counts[False] += 1
        else:
            stormcast_alert_eligibility_counts[None] += 1

        alert_outcome = stormcast_result.get("alert_outcome")
        if alert_outcome:
            stormcast_alert_outcome_counts[alert_outcome] += 1

        for blocker in stormcast_result.get("alert_blockers", []):
            stormcast_alert_blocker_counts[str(blocker)] += 1

    if stormcast_status_counts:
        status_summary = ", ".join(
            f"{status}={count}" for status, count in sorted(stormcast_status_counts.items())
        )
        eligibility_summary = (
            f"true={stormcast_alert_eligibility_counts[True]}, "
            f"false={stormcast_alert_eligibility_counts[False]}, "
            f"none={stormcast_alert_eligibility_counts[None]}"
        )
        print(
            "[CTAM] StormCast summary: "
            f"status[{status_summary}] can_generate_alerts[{eligibility_summary}]"
        )
        if stormcast_alert_outcome_counts:
            outcome_summary = ", ".join(
                f"{name}={count}" for name, count in sorted(stormcast_alert_outcome_counts.items())
            )
            print(f"[CTAM] StormCast alert outcomes: {outcome_summary}")
        if stormcast_alert_blocker_counts:
            blocker_summary = ", ".join(
                f"{name}={count}" for name, count in sorted(stormcast_alert_blocker_counts.items())
            )
            print(f"[CTAM] StormCast alert blockers: {blocker_summary}")
    
    # Step 2: Run grid-based modules
    grid_results = {}
    attachable_grid_results = {}
    grid_success_count = 0
    grid_error_count = 0
    grid_alert_count = builtin_alert_count
    
    for module in grid_modules.values():
        try:
            module_start = time.time()
            result = module.run()
            module_elapsed = time.time() - module_start
            grid_results[module.name] = result
            if result.get("attach_to_stormcells", True):
                attachable_grid_results[module.name] = result
            grid_success_count += 1
            print(f"[CTAM]   Grid module '{module.name}' completed in {module_elapsed:.3f}s")
            
            # Generate alerts from grid results
            if result.get("features") and result["features"].get("features"):
                features = result["features"]["features"]
                alerts = module.alerts(features)
                if alerts:
                    grid_alert_count += AlertManager.publish_many(alerts)
                    
        except Exception as e:
            grid_results[module.name] = {"status": "error", "error": str(e)}
            print(f"[CTAM]   Grid module '{module.name}' FAILED: {e}")
            grid_error_count += 1
    
    # Attach grid results to first cell (or create placeholder)
    if attachable_grid_results:
        if cells:
            cells[0]["modules"]["_grid_outputs"] = attachable_grid_results
        else:
            # No cells, but attachable grid modules ran - store results separately
            cells = [{"modules": {"_grid_outputs": attachable_grid_results}}]
    
    total_elapsed = time.time() - start_time
    print(f"[CTAM] Pipeline complete: {cell_success_count} cell success, {cell_error_count} cell error(s), {grid_success_count} grid success, {grid_error_count} grid error(s), {grid_alert_count} grid alert(s) in {total_elapsed:.3f}s")
    
    # Generate timestamp snapshot of active alerts if provided
    cells = _run_external_modules(
        cells,
        timestamp,
        json_path,
        input_manifest,
        disabled=disable_ctam_modules,
    )

    if timestamp:
        try:
            AlertManager.create_snapshot(timestamp)
        except Exception as e:
            print(f"[CTAM] Failed to create alert snapshot for {timestamp}: {e}")

    return cells
