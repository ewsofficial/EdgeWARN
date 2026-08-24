"""Primary EdgeWARN service functions (decomposition Phase 1).

Owns MRMS timestamp selection, the truthful cycle-state store, retry policy,
and the primary polling loop that drives ``run_tandem_cycle_once``. Extracted
verbatim from the former monolithic ``run.py`` so the future
``run_edgewarn.py`` entry point can call this module directly while the old
runner remains a temporary adapter.

The primary service must not import EWMRS or NEXRAD implementations at module
load: the EWMRS tandem worker is deferred inside ``util.runtime.cycle``, and
accessory loops live behind ``util.runtime.background``, which this module
never imports.
"""

from datetime import datetime, timezone
import time

import util.file as fs
from common.config import loader as config_loader, overlay
from common.ingest.mrms.config import get_check_modifiers
from util.runtime.cycle import (
    CycleRetryPolicy,
    CycleStateStore,
    TandemCycleConfig,
    run_tandem_cycle_once,
)
from util.runtime.config import resolve_file, section
from util.runtime.logging import drain_log_queue
from util.runtime.scheduler import load_last_processed_from_stormcells


def build_cycle_config(args):
    """Freeze one validated per-cycle configuration plus GOES coordination."""
    goes_coordination = section("goes_coordination")
    mrms_core_only = args.mrms_core_only
    return TandemCycleConfig(
        lat_limits=tuple(args.lat_limits),
        lon_limits=tuple(args.lon_limits),
        profile=args.profile,
        disable_ctam=args.disable_ctam,
        disable_ctam_modules=args.disable_ctam_modules,
        disable_tracking=args.disable_tracking,
        disable_polygon_expansion=args.disable_polygon_expansion,
        refl_threshold=args.refl_threshold,
        min_seed_percentage=args.min_seed_percentage,
        drop_offset=args.drop_offset,
        config_dir=args.config_dir,
        ewmrs_enabled=not args.disable_ewmrs and not mrms_core_only,
        goes_enabled=not args.disable_goes and not mrms_core_only,
        mrms_core_only=mrms_core_only,
        goes_render_wait_seconds=goes_coordination["render_wait_seconds"],
        goes_render_wait_interval_seconds=goes_coordination[
            "render_wait_interval_seconds"
        ],
    ), goes_coordination


def report_effective_config(config_dir=None):
    """Where this process's configuration actually came from.

    Names only the catalogs already read, not all of ``CONFIG_NAMES``:
    ``get_provenance`` loads on a miss, so naming every catalog would parse and
    schema-validate 19 files purely to describe them.

    Reports the winning layer per key rather than the value, so a key holding a
    credential cannot be disclosed by a diagnostic.
    """
    root = config_loader.config_root(config_dir)
    catalogs = ", ".join(
        f"{name}@{config_loader.get_provenance(name, config_dir=config_dir)['schema_version']}"
        for name in config_loader.loaded_config_names(config_dir=config_dir)
    )
    print(f"[Scheduler] Config root: {root}")
    print(f"[Scheduler] Catalogs loaded: {catalogs or 'none'}")
    active = overlay.overrides()
    if active:
        summary = ", ".join(f"{key} <- {layer}" for key, layer in sorted(active.items()))
    else:
        summary = "none; every resolved value came from YAML"
    print(f"[Scheduler] Active overrides: {summary}")
    all_origins = overlay.origins()
    provenance = ", ".join(f"{key} <- {layer}" for key, layer in sorted(all_origins.items())) or "none"
    print(f"[Scheduler] Resolved-key provenance: {provenance}")
    print("[Scheduler] Provenance limit: only values resolved with overlay.resolve(key=...) are key-level; direct catalog reads are covered by Catalogs loaded above.")

    # Lazy imports keep diagnostics from making optional render dependencies eager
    # at module import time.
    from common.ingest.mrms.config import get_mrms_modifiers
    from EWMRS.render.config import get_mrms_file_list, get_goes_file_list
    from EdgeWARN.process.integrate.config import get_datasets_config
    from EWMRS.rap.config import get_rap_uint16_layers
    print(
        "[Scheduler] Enabled products: "
        f"MRMS ingest={len(get_mrms_modifiers())}, "
        f"MRMS readiness={len(get_check_modifiers())}, "
        f"EWMRS MRMS={len(get_mrms_file_list())}, "
        f"GOES={len(get_goes_file_list())}, "
        f"integration datasets={len(get_datasets_config())}, "
        f"RAP layers={len(get_rap_uint16_layers())}"
    )
    print("[Scheduler] Configuration changes require a process restart to take effect.")


def log_effective_flags(args):
    mrms_core_only = args.mrms_core_only
    goes_enabled = not args.disable_goes and not mrms_core_only
    print(
        "[Scheduler] Configuration: "
        f"lat={tuple(args.lat_limits)}, lon={tuple(args.lon_limits)}, "
        f"refl_threshold={args.refl_threshold}, "
        f"min_seed_percentage={args.min_seed_percentage}, "
        f"drop_offset={args.drop_offset}, "
        f"goes_decoupled={'yes' if goes_enabled else 'no'}"
    )
    if args.disable_ctam:
        print("[Scheduler] CTAM execution disabled via --disable-ctam")
    elif args.disable_ctam_modules:
        print("[Scheduler] External CTAM modules disabled; built-in StormCast remains enabled")
    if args.disable_tracking:
        print("[Scheduler] Tracking disabled via --disable-tracking")
    if args.disable_polygon_expansion:
        print("[Scheduler] Polygon expansion disabled via --disable-polygon-expansion; using original ProbSevere polygons")
    if args.disable_ewmrs:
        print("[Scheduler] EWMRS pipeline disabled via --disable-ewmrs")
    if args.disable_nws:
        print("[Scheduler] NWS background ingest disabled via --disable-nws")
    if args.disable_metar:
        print("[Scheduler] METAR background ingest disabled via --disable-metar")
    if args.disable_goes:
        print("[Scheduler] GOES/GLM ingest and GOES rendering disabled via --disable-goes")
    if args.disable_nexrad:
        print("[Scheduler] NEXRAD ingest and rendering disabled via --disable-nexrad")
    if mrms_core_only:
        print("[Scheduler] MRMS-core-only mode: running MRMS detection, MRMS integration, and CTAM only")


def _resolve_retry_policy(cycle_settings):
    retry_settings = cycle_settings["retry"]
    return CycleRetryPolicy(
        max_attempts=max(1, int(overlay.resolve(
            None,
            env_names=["EDGEWARN_CYCLE_MAX_ATTEMPTS"],
            yaml_value=retry_settings["max_attempts"],
            key="cycle.retry.max_attempts",
        ))),
        initial_backoff_seconds=max(0.0, float(overlay.resolve(
            None,
            env_names=["EDGEWARN_CYCLE_RETRY_BACKOFF_SECONDS"],
            yaml_value=retry_settings["initial_backoff_seconds"],
            key="cycle.retry.initial_backoff_seconds",
        ))),
        max_backoff_seconds=max(0.0, float(overlay.resolve(
            None,
            env_names=["EDGEWARN_CYCLE_MAX_BACKOFF_SECONDS"],
            yaml_value=retry_settings["max_backoff_seconds"],
            key="cycle.retry.max_backoff_seconds",
        ))),
    )


def run_primary_cycle_loop(
    *,
    checker,
    cycle_config,
    goes_render_task_queue,
    goes_render_log_queue,
    nexrad_log_queue,
    goes_cycle_active_event,
    manager,
    supervisor,
):
    """Poll for MRMS timestamps and drive tandem cycles until interrupted.

    Advances ``last_successful`` and the selection cursor only after a
    validated ``CycleOutcome``; failed scans retry with bounded exponential
    backoff and are abandoned explicitly after ``max_attempts``.
    """
    stormcell_last_successful, init_message = load_last_processed_from_stormcells(fs.STORMCELL_DIR)
    print(init_message)
    cycle_settings = section("cycle")
    cycle_state_store = CycleStateStore(
        resolve_file(cycle_settings["state_file"], "cycle.state_file")
    )
    persisted_cycle_state = cycle_state_store.load()
    if stormcell_last_successful is not None:
        persisted_cycle_state = cycle_state_store.seed_last_successful(
            stormcell_last_successful
        )

    last_successful = persisted_cycle_state.last_successful
    last_abandoned = persisted_cycle_state.last_abandoned
    selection_cursor = persisted_cycle_state.selection_cursor
    pending_timestamp = persisted_cycle_state.retry_timestamp
    pending_attempt_count = (
        persisted_cycle_state.attempt_count if pending_timestamp is not None else 0
    )
    retry_not_before = 0.0
    retry_policy = _resolve_retry_policy(cycle_settings)
    report_effective_config(cycle_config.config_dir)
    print(
        "[Scheduler] Cycle progress: "
        f"last_successful={last_successful}, "
        f"last_attempted={persisted_cycle_state.last_attempted}, "
        f"last_abandoned={last_abandoned}, "
        f"pending_retry={pending_timestamp}"
    )

    supervisor_settings = section("supervisor")

    try:
        while True:
            drain_log_queue(goes_render_log_queue)
            drain_log_queue(nexrad_log_queue)
            now = datetime.now(timezone.utc)
            check_modifiers = get_check_modifiers()
            latest_common = None
            if pending_timestamp is None:
                # The selection cursor may include an explicitly abandoned
                # scan, while last_successful always remains truthful.
                latest_common = checker.latest_common_minute_1h(
                    check_modifiers,
                    last_processed=selection_cursor,
                )

                is_new_s3 = bool(
                    latest_common
                    and (selection_cursor is None or latest_common > selection_cursor)
                )
                if not is_new_s3:
                    latest_https = checker.check_https_fallback(check_modifiers, now)
                    if latest_https and (
                        selection_cursor is None or latest_https > selection_cursor
                    ):
                        print(
                            f"[Scheduler] HTTPS Fallback found NEWER timestamp: {latest_https}"
                        )
                        latest_common = latest_https

                if latest_common and (
                    selection_cursor is None or latest_common > selection_cursor
                ):
                    pending_timestamp = latest_common
                    pending_attempt_count = 0

            should_run_pipeline = (
                pending_timestamp is not None
                and time.monotonic() >= retry_not_before
            )

            if should_run_pipeline:
                dt = pending_timestamp
                pending_attempt_count += 1
                cycle_state_store.record_attempt(dt, pending_attempt_count)
                print(
                    f"[Scheduler] Starting tandem cycle for {dt} "
                    f"(attempt {pending_attempt_count}/{retry_policy.max_attempts})"
                )

                outcome = run_tandem_cycle_once(
                    dt,
                    goes_render_task_queue,
                    goes_render_log_queue,
                    manager,
                    config=cycle_config,
                    goes_cycle_active_event=goes_cycle_active_event,
                )
                if outcome.completed:
                    cycle_state_store.record_outcome(outcome, pending_attempt_count)
                    last_successful = dt
                    selection_cursor = max(
                        value
                        for value in (last_successful, last_abandoned)
                        if value is not None
                    )
                    pending_timestamp = None
                    pending_attempt_count = 0
                    retry_not_before = 0.0
                    print(
                        f"Tandem cycle for {dt} finished with "
                        f"{len(outcome.produced_artifacts)} validated artifact(s)"
                    )
                else:
                    abandon = pending_attempt_count >= retry_policy.max_attempts
                    cycle_state_store.record_outcome(
                        outcome,
                        pending_attempt_count,
                        abandoned=abandon,
                    )
                    if abandon:
                        last_abandoned = dt
                        selection_cursor = max(
                            value
                            for value in (last_successful, last_abandoned)
                            if value is not None
                        )
                        pending_timestamp = None
                        pending_attempt_count = 0
                        retry_not_before = 0.0
                        print(
                            f"[Scheduler] Tandem cycle for {dt} was explicitly "
                            f"abandoned after {retry_policy.max_attempts} attempts; "
                            f"errors={list(outcome.errors)}"
                        )
                    else:
                        delay = retry_policy.delay_after(pending_attempt_count)
                        retry_not_before = time.monotonic() + delay
                        print(
                            f"[Scheduler] Tandem cycle for {dt} failed and remains "
                            f"pending; retrying in {delay:.1f}s; "
                            f"errors={list(outcome.errors)}"
                        )

            else:
                if pending_timestamp is not None:
                    remaining = max(0.0, retry_not_before - time.monotonic())
                    print(
                        f"[Scheduler] Retry for {pending_timestamp} is pending "
                        f"for another {remaining:.1f}s"
                    )
                elif not latest_common:
                     print("[Scheduler] No new data found (S3 or HTTPS). Waiting...")
                else:
                     print(
                         f"[Scheduler] Timestamp {latest_common} is not newer than "
                         f"selection cursor {selection_cursor}. Waiting..."
                     )

            # Wait/Check loop — also monitor accessory processes
            for _ in range(supervisor_settings["check_ticks"]):
                time.sleep(supervisor_settings["tick_seconds"])
                supervisor.check()

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
