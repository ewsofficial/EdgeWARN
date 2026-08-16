"""Phase 0 tests pinning each known configuration disagreement.

These tests assert the behavior that exists **today**, including where it is
almost certainly a bug. Phase 0 is explicitly value-preserving: the point is to
make each disagreement visible and impossible to change by accident, so that a
later phase resolves it as a reviewed decision rather than a silent side effect
of moving a literal into YAML.

Every test below names the decision the migration still owes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import ssl
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from tests.core.config.source_inspect import param_default

REPO_ROOT = Path(__file__).resolve().parents[3]


# --- Kalman: the YAML is now the single copy of every value ---------------

KALMAN_CONFIG_PATH = "EdgeWARN/process/detect/kalman/config.py"


def _dataclass_field_defaults(relative_path: str, class_name: str) -> dict:
    """Map field name -> literal default, for fields that declare one."""
    import ast

    from tests.core.config.source_inspect import SRC, _literal

    tree = ast.parse((SRC / relative_path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return {
                statement.target.id: _literal(statement.value)
                for statement in node.body
                if isinstance(statement, ast.AnnAssign)
                and isinstance(statement.target, ast.Name)
                and statement.value is not None
            }
    raise AssertionError(f"{class_name} not found in {relative_path}")


def _kalman_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "kalman.yaml").read_text(encoding="utf-8"))


def test_tracking_max_prediction_time_resolves_to_the_yaml_value_only():
    """RESOLVED in Phase 4: the value is 6.0, and there is only one of it.

    The disagreement was between a 6.0 dataclass field default and a
    `.get('max_prediction_time_minutes', 10.0)` fallback that would have taken
    over if the key were ever dropped. Both copies are gone: the dataclass
    declares no default and the loader is given no fallback, so a missing key
    is a startup error rather than a silent switch to 10 minutes.
    """
    from EdgeWARN.process.detect.kalman.config import TrackingConfig

    assert _dataclass_field_defaults(KALMAN_CONFIG_PATH, "TrackingConfig") == {}

    yaml_value = _kalman_yaml()["tracking"]["max_prediction_time_minutes"]
    assert yaml_value == 6.0
    assert TrackingConfig.from_yaml().max_prediction_time_minutes == yaml_value

    source = (REPO_ROOT / "src" / KALMAN_CONFIG_PATH).read_text(encoding="utf-8")
    assert "10.0" not in source


def test_no_kalman_config_field_declares_a_default():
    """Every base default lives in the YAML, so no dataclass may restate one."""
    for class_name in (
        "KalmanConfig",
        "TrackingConfig",
        "AssignmentConfig",
        "FilterInternalsConfig",
        "ConfidenceConfig",
        "AssignmentCostsConfig",
    ):
        assert _dataclass_field_defaults(KALMAN_CONFIG_PATH, class_name) == {}, class_name


def test_kalman_config_reads_the_yaml_without_inline_fallbacks():
    """The 19 `.get(key, fallback)` calls are gone; keys are required outright."""
    import ast

    from tests.core.config.source_inspect import SRC

    tree = ast.parse((SRC / KALMAN_CONFIG_PATH).read_text(encoding="utf-8"))
    fallback_gets = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and len(node.args) >= 2
    ]
    assert fallback_gets == []


def test_every_kalman_yaml_section_is_consumed():
    """No section is inert: each one is subscripted by name in the loader."""
    import ast

    from tests.core.config.source_inspect import SRC

    tree = ast.parse((SRC / KALMAN_CONFIG_PATH).read_text(encoding="utf-8"))
    subscripted = {
        node.slice.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Subscript)
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    }

    sections = set(_kalman_yaml()) - {"schema_version"}
    assert sections == {
        "kalman_filter",
        "tracking",
        "assignment",
        "filter_internals",
        "confidence",
        "assignment_costs",
    }
    assert sections <= subscripted


# --- Lineage overlap: three concepts, three owners, still shadowed ---------

def test_each_lineage_overlap_concept_has_exactly_one_owner():
    """RESOLVED: no literal competes with the YAML for any of the three ratios.

    `overlap_threshold` named three different things. Now each has one owner and
    every declaration site defaults to `None`, so a caller can still override
    without a literal shadowing the catalog:

    - the tracked merge/split gate -> `detection.yaml tracker.lineage_overlap_ratio`
    - a directly built detector     -> `lineage.yaml event_overlap_ratio`
    - a bare spatial query          -> `lineage.yaml spatial_query_overlap_ratio`

    The shadowing itself is deliberate and still in force: the tracker always
    forwards its own value into `LineageDetector`, so `event_overlap_ratio`
    applies only to a detector built directly. That is why the two must stay
    separate keys -- collapsing them would silently retune the tracked path.
    """
    declaration_sites = {
        ("EdgeWARN/process/detect/track.py", "StormCellTracker.__init__"),
        ("EdgeWARN/process/detect/lineage/detector.py", "LineageDetector.__init__"),
        ("EdgeWARN/process/detect/lineage/detector.py", "detect_lineage_events"),
        ("EdgeWARN/process/detect/lineage/spatial.py", "find_overlapping_cells"),
    }
    for relative_path, qualified_name in declaration_sites:
        assert param_default(relative_path, qualified_name, "overlap_threshold") is None, (
            f"{relative_path}::{qualified_name} still declares a literal default"
        )

    detector_source = (
        REPO_ROOT / "src/EdgeWARN/process/detect/lineage/detector.py"
    ).read_text(encoding="utf-8")
    assert "DEFAULT_OVERLAP_THRESHOLD" not in detector_source

    assert _lineage_yaml()["lineage"]["event_overlap_ratio"] == 0.15
    assert _lineage_yaml()["lineage"]["spatial_query_overlap_ratio"] == 0.0
    assert _detection_yaml()["tracker"]["lineage_overlap_ratio"] == 0.10

    # The tracker forwards its own value, so the lineage.yaml ratio never applies
    # on the tracked path. Keep the two keys distinct.
    track_source = (REPO_ROOT / "src/EdgeWARN/process/detect/track.py").read_text(encoding="utf-8")
    assert "overlap_threshold=self.overlap_threshold" in track_source


def test_lineage_buffer_thresholds_have_no_literal_defaults():
    """`LineageBuffer()` is constructed with no kwargs in production.

    `track.py` calls `LineageBuffer.load(stormcell_dir)`, so whatever the
    constructor defaults to is what runs. Literals there would outrank
    lineage.yaml for the entire tracked path.
    """
    buffer_keys = ("min_confirmations", "max_pending", "prune_after_scans", "scan_interval_seconds")
    for key in buffer_keys:
        assert param_default(
            "EdgeWARN/process/detect/lineage/buffer.py", "LineageBuffer.__init__", key
        ) is None, f"LineageBuffer.__init__ still declares a literal {key}"

    yaml_buffer = _lineage_yaml()["lineage"]["buffer"]
    assert yaml_buffer == {
        "min_confirmations": 2,
        "max_pending": 100,
        "prune_after_scans": 5,
        "scan_interval_seconds": 120.0,
    }


def _lineage_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "lineage.yaml").read_text(encoding="utf-8"))


def _detection_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "detection.yaml").read_text(encoding="utf-8"))


# --- API index: one flag the two pipelines answer differently -------------

def test_remove_old_cells_stays_split_between_the_two_pipelines():
    """RESOLVED: three declaration sites, one resolution point, two recorded answers.

    The realtime and historical pipelines genuinely disagree, so this cannot become
    a single key. Every declaration site now defaults to `None` and forwards, and
    only `APIIndexManager` resolves -- which is what keeps `False` distinguishable
    from "caller said nothing". A literal `True` restored at any forwarding site
    would make a replay start deleting cells.
    """
    from EdgeWARN.api_integration.config import (
        remove_old_cells_historical,
        remove_old_cells_realtime,
    )

    for relative_path, qualified_name in (
        ("EdgeWARN/api_integration/index_manager.py", "APIIndexManager.__init__"),
        ("EdgeWARN/pipeline.py", "run_edgewarn_integration_phase"),
        ("EdgeWARN/process/integrate/pipeline.py", "main"),
    ):
        assert param_default(relative_path, qualified_name, "remove_old_cells") is None, (
            f"{relative_path}::{qualified_name} still declares a literal default"
        )

    recorded = _api_index_yaml()["api_index"]["remove_old_cells"]
    assert remove_old_cells_realtime() is recorded["realtime"] is True
    assert remove_old_cells_historical() is recorded["historical"] is False

    pipeline_source = (REPO_ROOT / "src/EdgeWARN/pipeline.py").read_text(encoding="utf-8")
    assert "remove_old_cells=remove_old_cells_historical()" in pipeline_source
    assert "remove_old_cells=False" not in pipeline_source


def test_inactive_cell_age_is_a_separate_owner_from_alert_cleanup_age():
    """RESOLVED: two 120s, two owners, and only one of them is in a catalog.

    One expires cells from the API index; the other prunes alert files. They are
    equal today, which is exactly why a single key would look correct right up to
    the first time one subsystem needs a different budget. CTAM alert extraction is
    deferred, so `cleanup_expired` keeps its literal and remains its sole owner --
    a later phase moves it, and this test is what stops it being folded into the
    api_index key on the way.
    """
    from EdgeWARN.api_integration.config import inactive_cell_max_age_minutes

    recorded = _api_index_yaml()["api_index"]["inactive_cell_max_age_minutes"]
    assert inactive_cell_max_age_minutes() == recorded == 120

    assert (
        param_default("EdgeWARN/alerts/manager.py", "AlertManager.cleanup_expired", "max_age_minutes")
        == 120
    ), "the alert cleanup budget moved; give it its own key rather than reusing api_index's"

    source = (REPO_ROOT / "src/EdgeWARN/api_integration/index_manager.py").read_text(encoding="utf-8")
    assert "120 * 60" not in source
    assert "inactive_cell_max_age_minutes() * 60" in source


def test_index_bootstrap_stays_split_between_the_two_pipelines():
    """RESOLVED: the audit filed this under `runtime`, but api_index owns it.

    It gates `APIIndexManager.initialize_indexes()`, so it belongs to the index
    subsystem rather than to the historical runtime that happens to switch it off.
    Same shape as `remove_old_cells`: `None` at the declaration site so an explicit
    `False` stays distinguishable from "caller said nothing", and a literal restored
    in `process_historical.py` would make every replay rebuild the realtime index.
    """
    from EdgeWARN.api_integration.config import (
        initialize_at_startup_historical,
        initialize_at_startup_realtime,
    )

    assert param_default("EdgeWARN/pipeline.py", "initialize_runtime", "initialize_indexes") is None

    recorded = _api_index_yaml()["api_index"]["initialize_at_startup"]
    assert initialize_at_startup_realtime() is recorded["realtime"] is True
    assert initialize_at_startup_historical() is recorded["historical"] is False

    historical_source = (REPO_ROOT / "src/process_historical.py").read_text(encoding="utf-8")
    assert "initialize_indexes=initialize_at_startup_historical()" in historical_source
    assert "initialize_indexes=False" not in historical_source


def test_stormcell_resync_interval_comes_from_the_catalog():
    """The resync is what reconciles deletions, so a stale literal here loses them."""
    from EdgeWARN.api_integration.config import stormcell_resync_every_updates

    assert stormcell_resync_every_updates() == _api_index_yaml()["api_index"]["resync_every_updates"] == 500

    source = (REPO_ROOT / "src/EdgeWARN/api_integration/index_manager.py").read_text(encoding="utf-8")
    assert "self.stormcell_resync_interval = stormcell_resync_every_updates()" in source


# --- Scheduler listing width: two keys, only one of them reached -----------

def _scheduler_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "scheduler.yaml").read_text(encoding="utf-8"))


def _api_index_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "api_index.yaml").read_text(encoding="utf-8"))


def test_scheduler_listing_widths_stay_two_separate_keys():
    """DECISION OWED: `mrms_update_checker_max_entries` reaches no production path.

    It is read only by `has_update`, whose only in-repo caller is
    `all_sources_available`, which nothing calls outside the scheduler tests. The
    width the live scheduler actually uses is `modifier_lookup_max_entries`,
    resolved for `_get_modifier_times`. Collapsing the two would look like a
    cleanup but would retune the live path from 20 to 10, so they stay distinct
    until the dead path is either wired up or deleted.
    """
    from EdgeWARN.schedule.config import (
        modifier_lookup_max_entries,
        mrms_update_checker_max_entries,
    )

    recorded = _scheduler_yaml()["scheduler"]
    assert mrms_update_checker_max_entries() == recorded["mrms_update_checker_max_entries"] == 10
    assert modifier_lookup_max_entries() == recorded["modifier_lookup_max_entries"] == 20

    source = (REPO_ROOT / "src/EdgeWARN/schedule/scheduler.py").read_text(encoding="utf-8")
    assert source.count("all_sources_available") == 1, "it gained a caller; revisit the dead-path note"

    # The live path must reach the catalog, not a literal that happens to match it.
    # Resolved once above the thread pool and passed in, rather than read inside
    # the per-modifier helper: that helper runs once per modifier per tick from
    # every worker thread, and `load_config` stats the catalog on every call.
    assert "max_entries = modifier_lookup_max_entries()" in source
    assert "max_entries=max_entries," in source
    assert "bucket, 20," not in source

    # A caller may still override, but no literal competes with the catalog.
    assert param_default(
        "EdgeWARN/schedule/scheduler.py", "MRMSUpdateChecker.__init__", "max_entries"
    ) is None


def test_scheduler_lookback_and_perf_gate_have_no_literals():
    from EdgeWARN.schedule.config import s3_lookback_hours, slow_check_log_threshold_ms

    recorded = _scheduler_yaml()["scheduler"]
    assert s3_lookback_hours() == recorded["s3_lookback_hours"] == 2
    assert slow_check_log_threshold_ms() == recorded["slow_check_log_threshold_ms"] == 2000

    source = (REPO_ROOT / "src/EdgeWARN/schedule/scheduler.py").read_text(encoding="utf-8")
    assert "timedelta(hours=2)" not in source
    assert "dt > 2000" not in source


# --- Historical replay: the loop literals now live in the catalog ---------

def _historical_yaml() -> dict:
    import yaml

    return yaml.safe_load(
        (REPO_ROOT / "config" / "historical.yaml").read_text(encoding="utf-8")
    )


def test_historical_replay_loop_holds_no_step_or_throttle_literals():
    """RESOLVED: the replay cadence is owned by historical.yaml.

    `step_minutes` is applied on all three branches of the scan loop, so a literal
    surviving on any one of them would desynchronize the cursor from the other two
    and silently skip or re-scan minutes.
    """
    from EdgeWARN.historical_config import (
        historical_step_minutes,
        historical_throttle_seconds,
    )

    source = (REPO_ROOT / "src/process_historical.py").read_text(encoding="utf-8")
    assert "timedelta(minutes=1)" not in source
    assert "time.sleep(1)" not in source
    assert source.count("current_time += step") == 3

    recorded = _historical_yaml()["historical"]
    assert historical_step_minutes() == recorded["step_minutes"] == 1
    assert historical_throttle_seconds() == recorded["throttle_seconds"] == 1


def test_historical_cleanup_caps_files_but_inherits_the_age_budget():
    """RESOLVED: two catalogs still govern one cleanup call, on purpose.

    `_cleanup_historical_data_dirs` passes `max_files` only, so `clean_old_files`
    applies `filesystem.yaml`'s age budget for the other half. A directory is
    therefore trimmed to `historical.cleanup_max_files` AND anything older than an
    hour is dropped, which means raising the file count alone cannot retain more
    than an hour of replay data. That is not a bug to consolidate away -- the two
    keys have different owners -- but it is surprising enough to pin.
    """
    from EdgeWARN.historical_config import historical_cleanup_max_files
    from util.file_config import cleanup_max_age_minutes

    assert historical_cleanup_max_files() == 5
    assert cleanup_max_age_minutes() == 60

    pipeline_source = (REPO_ROOT / "src/EdgeWARN/pipeline.py").read_text(encoding="utf-8")
    assert "max_files=historical_cleanup_max_files()" in pipeline_source
    assert "max_age_minutes" not in pipeline_source


def test_no_cleaner_in_util_file_restates_a_retention_number():
    """The three cleaners are not interchangeable, so each defers separately.

    `clean_old_files` applies age and count, `clean_files_by_age` applies age only,
    and `clean_idx_files` applies neither. A literal restored in any signature
    would let one of them drift from the catalog while the others tracked it.
    """
    import yaml

    from util.file_config import cleanup_max_age_minutes, cleanup_max_files

    for qualified_name in ("clean_old_files", "clean_files_by_age", "async_clean_files_by_age"):
        assert param_default("util/file.py", qualified_name, "max_age_minutes") is None

    recorded = yaml.safe_load(
        (REPO_ROOT / "config" / "filesystem.yaml").read_text(encoding="utf-8")
    )["cleanup_defaults"]
    assert cleanup_max_age_minutes() == recorded["max_age_minutes"] == 60
    assert cleanup_max_files() == recorded["max_files"] == 10

    source = (REPO_ROOT / "src/util/file.py").read_text(encoding="utf-8")
    assert "max_age_minutes=60" not in source
    assert "max_files=10" not in source


def test_rap_pre_download_sweep_keeps_its_uncapped_pass():
    """DECISION PRESERVED: `max_files=None` means "no count cap", not "unset".

    `download_rap` sweeps by age before downloading and only applies the count cap
    afterwards, so the pre-download pass must be able to say "age only". That is
    why `clean_old_files` resolves `max_files` from a sentinel rather than `None`:
    treating `None` as unset here would trim the RAP directory a download early.
    """
    import util.file as fs

    assert param_default("util/file.py", "clean_old_files", "max_files") is not None

    source = (REPO_ROOT / "src/common/ingest/synoptic/main.py").read_text(encoding="utf-8")
    assert source.count("max_files=None") == 2
    assert source.count("max_files=rap_max_files()") == 2

    file_source = (REPO_ROOT / "src/util/file.py").read_text(encoding="utf-8")
    assert "if max_files is _FROM_CATALOG:" in file_source
    assert "if max_files is None:" not in file_source.split("def clean_old_files")[1].split("now =")[0]

    assert fs._FROM_CATALOG is not None


# --- RAP filename: two keys describing one naming scheme ------------------

def _synoptic_rap_yaml() -> dict:
    import yaml

    return yaml.safe_load(
        (REPO_ROOT / "config" / "synoptic_rap.yaml").read_text(encoding="utf-8")
    )


def test_rap_filename_regex_round_trips_the_local_file_pattern():
    """RESOLVED: no literal competes with the catalog for the RAP naming scheme.

    `local_file_pattern` writes the cache file and `filename_regex` reads it back,
    so the two must describe the same name. Nothing in the schema can enforce
    that -- one is a `str.format` template and the other a regex -- so editing
    either alone would make `clean_rap_cache` stop recognizing its own downloads
    and warn-and-skip every file instead of pruning it.
    """
    from common.ingest.synoptic import config as rap_config
    from common.ingest.synoptic.main import parse_rap_analysis_time

    recorded = _synoptic_rap_yaml()["rap"]
    written = Path(
        recorded["local_file_pattern"].format(date="20260815", hour=7)
    )
    assert re.match(recorded["filename_regex"], written.name)
    assert parse_rap_analysis_time(written).strftime("%Y%m%d%H") == "2026081507"

    # The accessors are the only owners: the pre-extraction module constants are gone.
    main_source = (
        REPO_ROOT / "src/common/ingest/synoptic/main.py"
    ).read_text(encoding="utf-8")
    assert "RAP_FILENAME_RE = " not in main_source
    assert "RAP_MAX_FILES" not in main_source
    assert rap_config.rap_max_files() == recorded["max_files"] == 3


def test_generic_synoptic_download_keeps_its_own_age_default():
    """DECISION OWED: the 60-minute default in `download_synoptic` is a second copy.

    It is harmless only because `download_rap` -- the sole caller -- always passes
    `get_rap_max_age_minutes()`. A second dataset added without that argument would
    silently run a 60-minute budget instead of the catalog's 180. Left as-is because
    narrowing it to the RAP value would be retuning a shared helper for one caller.
    """
    assert param_default(
        "common/ingest/synoptic/downloader.py", "download_synoptic", "max_age_minutes"
    ) == 60
    assert _synoptic_rap_yaml()["rap"]["max_age_minutes"] == 180

    downloader_source = (
        REPO_ROOT / "src/common/ingest/synoptic/downloader.py"
    ).read_text(encoding="utf-8")
    assert "max_age_minutes=get_rap_max_age_minutes()" in downloader_source


# --- WPC: four keys describing one naming scheme --------------------------

def _wpc_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "wpc.yaml").read_text(encoding="utf-8"))


def test_wpc_cleanup_glob_matches_the_timestamped_name_but_not_latest():
    """RESOLVED: the retention sweep and the writer must agree, and no schema can check it.

    `output_filename_pattern` names the files the sweep is meant to delete and
    `latest_filename` names the one it must never touch. Broadening the glob would
    delete `latest.geojson` -- the file the API serves -- and narrowing it would
    leak every timestamped copy instead. One is a `str.format` template, one a
    glob, one a literal name, so only a round-trip test couples them.

    This also carries forward the refutation of the plan's claimed
    `surface_analysis_*.geojson` mismatch: `surface_analysis` is only a directory
    name, and the sweep and the writer have always agreed on the `wpc_sfc_` prefix.
    """
    import fnmatch

    from common.ingest.wpc.config import cleanup_glob, latest_filename
    from common.ingest.wpc.downloader import get_latest_output_filepath, get_output_filepath

    recorded = _wpc_yaml()["wpc"]
    glob = cleanup_glob()
    assert glob == recorded["cleanup_glob"] == "wpc_sfc_*.geojson"

    timestamped = recorded["output_filename_pattern"].format(date="20260815", hour=18)
    assert fnmatch.fnmatch(timestamped, glob)
    assert not fnmatch.fnmatch(latest_filename(), glob)

    written = get_output_filepath(
        datetime(2026, 8, 15, 18, 30, tzinfo=timezone.utc)
    ).name
    assert fnmatch.fnmatch(written, glob)
    assert get_latest_output_filepath().name == recorded["latest_filename"]


def test_wpc_valid_hours_are_derived_from_the_publish_interval():
    """RESOLVED (Phase 5): the hand-enumerated `valid_hours` key is gone.

    It used to be a second key holding `update_interval_hours` written out by
    hand, with nothing deriving one from the other. The downloader reads only the
    list, so changing the interval alone left a stale list behind and every run
    would request hours WPC never publishes and fall into its fallback path.

    `valid_hours()` now computes `range(0, 24, interval)`, which leaves the
    interval as the sole owner. What the catalog can no longer express, the schema
    now constrains instead: the interval is an `enum` of the divisors of 24, since
    the downloader wraps off the last publish hour to the previous day and an
    interval that does not tile the day would leave a short gap across midnight.
    """
    from common.ingest.wpc.config import update_interval_hours, valid_hours

    recorded = _wpc_yaml()["wpc"]
    assert "valid_hours" not in recorded, "the derived list must not return as a key"

    interval = recorded["update_interval_hours"]
    assert update_interval_hours() == interval == 3
    assert isinstance(interval, int), "range() cannot step by a float"
    assert 24 % interval == 0, "the publish hours must tile the day"

    # Pinned against the literal the catalog used to hold, so the derivation
    # cannot quietly start producing a different schedule than WPC publishes on.
    assert valid_hours() == (0, 3, 6, 9, 12, 15, 18, 21)
    assert valid_hours() == tuple(range(0, 24, interval))

    downloader_source = (
        REPO_ROOT / "src/common/ingest/wpc/downloader.py"
    ).read_text(encoding="utf-8")
    assert "default=hours[-1]" in downloader_source, "the wrap hour must track the list"
    assert "default=21" not in downloader_source


def test_wpc_request_url_round_trips_the_three_keys_that_build_it():
    """`coded_sfc_base_url`, `date_format` and `remote_filename_pattern` form one URL.

    None of them is meaningful alone, and a wrong one produces a 404 that the
    fallback path then masks as a stale-but-successful analysis, so the assertion
    is on the assembled string rather than on the parts.
    """
    from common.ingest.wpc import downloader as wpc_downloader

    recorded = _wpc_yaml()["wpc"]
    reference = datetime(2026, 8, 15, 18, 30, tzinfo=timezone.utc)
    built = wpc_downloader.build_url(reference, 18)

    assert built == "{}/{}/{}".format(
        recorded["coded_sfc_base_url"],
        reference.strftime(recorded["date_format"]),
        recorded["remote_filename_pattern"].format(hour=18),
    )
    assert built == "https://ftp.wpc.ncep.noaa.gov/coded_sfc/20260815/codsus18_hr"


def test_wpc_request_timeout_and_backfill_reach_the_catalog():
    from common.ingest.wpc.config import (
        http_timeout_seconds,
        previous_analysis_lookback_hours,
    )

    recorded = _wpc_yaml()["wpc"]
    assert http_timeout_seconds() == recorded["http_timeout_seconds"] == 30
    assert previous_analysis_lookback_hours() == recorded["previous_analysis_lookback_hours"] == 3

    downloader_source = (
        REPO_ROOT / "src/common/ingest/wpc/downloader.py"
    ).read_text(encoding="utf-8")
    assert downloader_source.count("timeout=http_timeout_seconds()") == 2
    assert "timeout=30" not in downloader_source

    main_source = (REPO_ROOT / "src/common/ingest/wpc/main.py").read_text(encoding="utf-8")
    assert "timedelta(hours=previous_analysis_lookback_hours())" in main_source


def test_wpc_unknown_feature_code_falls_back_to_the_catalog_color():
    """The fallback was written twice, once per feature kind, before extraction."""
    from common.ingest.wpc.converter import create_front_feature

    recorded = _wpc_yaml()["wpc"]
    feature = create_front_feature([(35.0, -97.0), (36.0, -96.0)], "NOTATYPE")

    assert feature["properties"]["color"] == recorded["fallback_geojson_color"] == "#000000"
    assert feature["properties"]["name"] == "NOTATYPE"

    known = create_front_feature([(35.0, -97.0), (36.0, -96.0)], "COLD")
    assert known["properties"]["color"] == recorded["feature_types"]["COLD"]["color"]

    source = (REPO_ROOT / "src/common/ingest/wpc/converter.py").read_text(encoding="utf-8")
    assert '"#000000"' not in source
    assert source.count("_style_for(") == 3, "one helper, two call sites"


def test_wpc_owns_its_retention_window_and_verifies_tls():
    """WPC's 360 minutes is its own; inheriting filesystem.yaml's 60 would keep nothing.

    A 3-hourly product needs a window wider than an hour, so this is a genuinely
    separate owner rather than a duplicate of the generic cleanup default.

    `verify_tls` is pinned `true` by the schema. The downloader reads it anyway and
    raises on false, so loosening the schema fails loudly rather than quietly
    downgrading the transport.
    """
    from common.ingest.wpc.config import cleanup_max_age_minutes, verify_tls
    from util.file_config import cleanup_max_age_minutes as generic_cleanup_age

    recorded = _wpc_yaml()["wpc"]
    assert cleanup_max_age_minutes() == recorded["cleanup_max_age_minutes"] == 360
    assert generic_cleanup_age() == 60

    assert verify_tls() is True
    schema = json.loads(
        (REPO_ROOT / "config/schema/wpc.schema.json").read_text(encoding="utf-8")
    )
    assert schema["properties"]["wpc"]["properties"]["verify_tls"]["const"] is True

    source = (REPO_ROOT / "src/common/ingest/wpc/downloader.py").read_text(encoding="utf-8")
    assert "ssl.CERT_NONE" not in source
    assert "if not verify_tls():" in source
    assert source.count("ssl.CERT_REQUIRED") == 1, "the two contexts were deliberately merged"

    main_source = (REPO_ROOT / "src/common/ingest/wpc/main.py").read_text(encoding="utf-8")
    assert "timedelta(hours=3)" not in main_source
    assert "wpc_sfc_*.geojson" not in main_source


# --- METAR: the one subsystem that really does skip TLS verification -------

def _metar_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "metar.yaml").read_text(encoding="utf-8"))


def _metar_source() -> str:
    return (REPO_ROOT / "src/common/ingest/metar.py").read_text(encoding="utf-8")


def test_metar_tls_verifies_from_one_place():
    """RESOLVED: `verify_tls` is true, and one key reaches all five call sites.

    Five call sites disabled verification independently -- three aiohttp
    ``ssl=False`` and two ``ssl.CERT_NONE`` -- so flipping the policy meant
    finding all five. They now share two helpers, and the count assertions below
    are what stops a sixth site from quietly reintroducing its own downgrade.

    The switch stayed false until a run confirmed both hosts present chains that
    validate against the default trust store. It is asserted here rather than by
    reaching the network, so this test does not depend on connectivity or on a
    certificate that will eventually be reissued.
    """
    from common.ingest.metar_config import aiohttp_ssl, ssl_context, verify_tls

    recorded = _metar_yaml()["metar"]
    assert verify_tls() is recorded["verify_tls"] is True

    # Unlike wpc.verify_tls, the schema leaves this free -- it is a real switch,
    # so an operator behind an intercepting proxy can still turn it off.
    schema = json.loads(
        (REPO_ROOT / "config/schema/metar.schema.json").read_text(encoding="utf-8")
    )
    assert schema["properties"]["metar"]["properties"]["verify_tls"] == {"type": "boolean"}

    # The value actually reaches the transport, in both spellings.
    assert aiohttp_ssl() is not False
    context = ssl_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True

    source = _metar_source()
    assert "CERT_NONE" not in source, "the five inline downgrades must stay collapsed"
    assert "ssl=False" not in source
    assert source.count("ssl=aiohttp_ssl()") == 3
    assert source.count("context=ssl_context()") == 2


def test_turning_metar_verify_tls_off_still_reaches_every_transport():
    """The escape hatch must work, and must be loud when used.

    Runs at ``False`` -- the opposite of the shipped value -- so the key cannot
    quietly become decorative. If a future edit hardcoded a verifying context,
    this fails while the test above still passes.

    The warning goes to the subsystem's IOManager, not ``warnings.warn``, so it
    lands in the same log stream as the rest of the ingest run. It is
    deliberately not deduplicated: an ingest run builds only a couple of
    contexts, and a single line at startup is easy to miss in a log.
    """
    from common.config import loader
    from common.ingest import metar_config

    def _not_verifying():
        return False

    original = metar_config.verify_tls
    metar_config.verify_tls = _not_verifying
    try:
        with mock.patch.object(metar_config, "_io") as recorder:
            assert metar_config.aiohttp_ssl() is False
            context = metar_config.ssl_context()
        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False

        assert recorder.write_warning.call_count == 2, "every helper must warn"
        for call in recorder.write_warning.call_args_list:
            assert "metar.verify_tls is false" in call.args[0]
    finally:
        metar_config.verify_tls = original
        loader.reset_cache()


def test_metar_accept_encoding_still_excludes_brotli():
    """DECISION PRESERVED: `br` is omitted because brotli responses failed to decode.

    This header is narrower than the client default on purpose, which is easy to
    read as an oversight and "fix" by restoring the default. Both request paths
    send it, and neither handles a brotli body, so adding `br` back would break
    the station download rather than speed it up.
    """
    from common.ingest.metar_config import accept_encoding

    recorded = _metar_yaml()["metar"]
    assert accept_encoding() == recorded["accept_encoding"] == "gzip, deflate"

    encodings = {token.strip() for token in accept_encoding().split(",")}
    assert "br" not in encodings, "nothing in metar.py decompresses brotli"
    assert "gzip" in encodings, "the sync path branches on a gzip Content-Encoding"

    source = _metar_source()
    assert "gzip, deflate" not in source
    assert source.count("accept_encoding()") == 2


def test_metar_cycle_url_needs_the_hour_placeholder():
    """A pattern without `{hour}` formats to itself, so every cycle repeats one hour.

    `str.format` does not raise on an unused keyword, so this fails silently and
    produces three identical fetches. The schema requires the placeholder.
    """
    from common.ingest.metar import _cycle_url
    from common.ingest.metar_config import observation_url_pattern

    recorded = _metar_yaml()["metar"]
    assert observation_url_pattern() == recorded["observation_url_pattern"]
    assert "{hour}" in observation_url_pattern()

    assert _cycle_url("07") == (
        "https://tgftp.nws.noaa.gov/data/observations/metar/cycles/07Z.TXT"
    )
    assert _cycle_url("07") != _cycle_url("18")

    schema = json.loads(
        (REPO_ROOT / "config/schema/metar.schema.json").read_text(encoding="utf-8")
    )
    pattern = schema["properties"]["metar"]["properties"]["observation_url_pattern"]["pattern"]
    assert re.search(pattern, observation_url_pattern())

    source = _metar_source()
    assert "tgftp.nws.noaa.gov" not in source
    assert source.count("_cycle_url(hour_str)") == 3, "one definition, two callers"


def test_metar_keeps_two_timeouts_rather_than_one():
    """RESOLVED: 60 and 30 describe different requests and must not be unified.

    The station database is one large JSON document fetched once; the observation
    cycles are small hourly text files fetched three at a time. Both were bare
    literals repeated across five call sites.
    """
    from common.ingest.metar_config import (
        observation_timeout_seconds,
        station_timeout_seconds,
    )

    recorded = _metar_yaml()["metar"]
    assert station_timeout_seconds() == recorded["station_timeout_seconds"] == 60
    assert observation_timeout_seconds() == recorded["observation_timeout_seconds"] == 30
    assert station_timeout_seconds() != observation_timeout_seconds()

    source = _metar_source()
    assert source.count("timeout=station_timeout_seconds()") == 2
    # One for the sync fetch, one for the async `_read` the two async branches share.
    assert source.count("timeout=observation_timeout_seconds()") == 2


def test_metar_retention_is_its_own_key_despite_matching_the_generic_default():
    """DECISION PRESERVED: two 60s, two owners, so retuning one cannot move the other.

    `util.file.clean_old_files` already defaults the age to `filesystem.yaml`'s 60,
    so METAR could have passed nothing. It passes its own key instead: the equal
    value is a coincidence of tuning, not a shared policy. The file count cap is
    the opposite call -- METAR does inherit that one, which is why only the age is
    forwarded.
    """
    from common.ingest.metar_config import cleanup_max_age_minutes
    from util.file_config import cleanup_max_age_minutes as generic_cleanup_age
    from util.file_config import cleanup_max_files as generic_cleanup_files

    recorded = _metar_yaml()["metar"]
    assert cleanup_max_age_minutes() == recorded["cleanup_max_age_minutes"] == 60
    assert generic_cleanup_age() == 60
    assert generic_cleanup_files() == 10

    assert "cleanup_max_files" not in recorded

    source = _metar_source()
    assert source.count("max_age_minutes=cleanup_max_age_minutes()") == 2
    assert "max_files" not in source, "METAR takes the generic count cap"


def test_metar_lookback_drives_both_entry_points():
    """The sync and async ingests must cover the same span, and did so by two 3s."""
    from common.ingest.metar_config import lookback_hours

    recorded = _metar_yaml()["metar"]
    assert lookback_hours() == recorded["lookback_hours"] == 3

    source = _metar_source()
    assert source.count("range(lookback_hours())") == 2
    assert "range(3)" not in source


def test_metar_station_parsing_rounds_once_from_the_catalog():
    """The sync and async station downloads parsed the same payload twice.

    Both rounded to 4 decimals with their own literal, so the two caches could
    have diverged. They now share one parser.
    """
    from common.ingest.metar import _parse_station_entries
    from common.ingest.metar_config import coordinate_decimals

    recorded = _metar_yaml()["metar"]
    assert coordinate_decimals() == recorded["coordinate_decimals"] == 4

    parsed = _parse_station_entries(
        [
            {"icaoId": "KJFK", "lat": 40.63992345, "lon": -73.77869012},
            {"stationId": "KORD", "lat": 41.9, "lon": -87.9},
            {"icaoId": "KNOPE", "lat": None, "lon": 1.0},
            {"lat": 1.0, "lon": 1.0},
        ]
    )
    assert parsed == {"KJFK": [40.6399, -73.7787], "KORD": [41.9, -87.9]}

    source = _metar_source()
    assert source.count("_parse_station_entries(") == 3, "one definition, two callers"


def test_metar_pressure_rounding_is_config_but_the_encoding_is_not():
    """`/100` is the AXXXX altimeter encoding; only the rounding is a setting."""
    from common.ingest.metar import parse_metar
    from common.ingest.metar_config import pressure_decimals

    recorded = _metar_yaml()["metar"]
    assert pressure_decimals() == recorded["pressure_decimals"] == 2

    parsed = parse_metar("KJFK 121756Z 31009KT A3039", "2023/01/12 17:56")
    assert parsed["pressure"] == 30.39

    source = _metar_source()
    assert "alt_value / 100, pressure_decimals()" in source


def test_metar_conus_bounds_are_the_audits_corrected_numbers():
    """The audit recorded lon -125..-67; the code has always used -66.0.

    Also pins that the filter reads the catalog once per call rather than through
    the old `CONUS_BOUNDS` module constant.
    """
    from common.ingest import metar
    from common.ingest.metar_config import conus_bounds

    recorded = _metar_yaml()["metar"]["conus_bounds"]
    assert dict(conus_bounds()) == recorded
    assert recorded == {"lat_min": 24.0, "lat_max": 50.0, "lon_min": -125.0, "lon_max": -66.0}

    assert not hasattr(metar, "CONUS_BOUNDS")
    assert not hasattr(metar, "STATION_DB_URL")

    # A station between the audit's -67 and the real -66 is kept.
    with mock.patch.object(metar, "get_station_coordinates", return_value=[40.0, -66.5]):
        kept = metar.process_content("2023/01/12 17:56\nKXYZ 121756Z 31009KT A3039\n")
    assert [entry["station"] for entry in kept] == ["KXYZ"]


def test_metar_station_cache_path_follows_the_runtime_data_dir():
    """The filename was written twice and joined to `fs.DATA_DIR` at each site."""
    from common.ingest import metar
    from common.ingest.metar_config import station_cache_file, station_db_url

    recorded = _metar_yaml()["metar"]
    assert station_cache_file() == recorded["station_cache_file"] == "stations_cache.json"
    assert station_db_url() == recorded["station_db_url"]
    assert "aviationweather.gov" in station_db_url()

    import util.file as fs

    assert metar._station_cache_path() == fs.DATA_DIR / station_cache_file()

    source = _metar_source()
    assert "stations_cache.json" not in source
    assert source.count("_station_cache_path()") == 3, "one definition, two callers"


# --- Detection thresholds duplicated across eight declaration sites --------

DETECTION_DEFAULT_FILES = {
    "EdgeWARN/pipeline.py": 0,
    "EdgeWARN/process/detect/detect.py": 0,
    "EdgeWARN/process/detect/main.py": 0,
    "EdgeWARN/process/detect/tools/gatemapper.py": 0,
    "util/io.py": 0,  # Phase 1: the argparse flag now defaults to None and is filled from detection.yaml
}


def _declaration_sites(param: str, expected: float) -> dict[str, int]:
    """Count places that declare ``param=expected`` as a default.

    Counted via ``ast`` rather than substring search so that a prose mention
    such as gatemapper's "With min_seed_percentage=0.001, ..." comment does not
    inflate the total.
    """
    import ast

    from tests.core.config.source_inspect import SRC, _literal

    flag = f"--{param.replace('_', '-')}"
    found: dict[str, int] = {}
    for relative in DETECTION_DEFAULT_FILES:
        tree = ast.parse((SRC / relative).read_text(encoding="utf-8"))
        count = 0
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                args = node.args
                positional = args.posonlyargs + args.args
                padding = len(positional) - len(args.defaults)
                pairs = [
                    (arg, args.defaults[i - padding])
                    for i, arg in enumerate(positional)
                    if i >= padding
                ]
                pairs += [(a, d) for a, d in zip(args.kwonlyargs, args.kw_defaults) if d]
                count += sum(1 for a, d in pairs if a.arg == param and _literal(d) == expected)
            elif isinstance(node, ast.Call):
                function = node.func
                if isinstance(function, ast.Attribute) and function.attr == "add_argument":
                    flags = {_literal(a) for a in node.args}
                    keywords = {k.arg: k.value for k in node.keywords if k.arg}
                    if flag in flags and _literal(keywords.get("default", ast.Constant(None))) == expected:
                        count += 1
        found[relative] = count
    return found


@pytest.mark.parametrize(
    ("param", "expected"),
    [("refl_threshold", 37.5), ("min_seed_percentage", 0.001), ("drop_offset", 10.0)],
)
def test_detection_thresholds_are_declared_only_in_yaml(param, expected):
    """RESOLVED in Phase 4: `detection.yaml` is the only declaration site.

    Phase 1 removed the argparse default in `util/io.py`; Phase 4 removed the
    remaining seven keyword-argument declarations by threading one typed
    `DetectionConfig` from the entrypoint. Every intermediate function now takes
    the resolved object, so there is nowhere left for a stale copy of the literal
    to shadow the YAML.
    """
    sites = _declaration_sites(param, expected)
    assert sites == DETECTION_DEFAULT_FILES
    assert sum(sites.values()) == 0
    assert _detection_yaml()["detection"][param] == expected


def test_gatemapper_hard_floor_makes_a_raised_threshold_ineffective():
    """RESOLVED in Phase 4: every term of the curve is read from YAML.

    The cap itself is unchanged and still a cap -- `baseline_refl_floor` is
    applied as `min(floor, refl_threshold)`, so raising `--refl-threshold` above
    the floor still cannot raise the baseline mask. What changed is that the
    floor is now editable, and the five terms of the per-cell threshold curve
    are named keys rather than literals buried in two expressions. They remain
    *coupled*: moving one shifts the whole curve.
    """
    source = (REPO_ROOT / "src/EdgeWARN/process/detect/tools/gatemapper.py").read_text(encoding="utf-8")
    assert "min(self.gm.baseline_refl_floor, self.refl_threshold)" in source

    # No term of the curve survives as a literal in the module.
    for literal in ("37.5", "40.0", "45.0", "52.0"):
        assert literal not in source

    gatemapper = _detection_yaml()["gatemapper"]
    assert gatemapper["baseline_refl_floor"] == 37.5
    assert gatemapper["dynamic_min_threshold"] == {
        "switch_max_refl": 45.0,
        "low": 37.5,
        "high": 40.0,
    }
    assert gatemapper["max_refl_clamp"] == 52.0


# --- Divergent user agents ------------------------------------------------

def test_one_user_agent_template_interpolates_the_package_version():
    """RESOLVED (Phase 5): three strings across five sites became one template.

    `runtime.yaml identity` is the sole authority. `util/release.py
    format_user_agent()` fills `{version}` from package.json, so the advertised
    version tracks the release instead of being copied into source.
    """
    from util.release import format_user_agent

    package_version = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))["version"]

    resolved = format_user_agent()
    assert resolved == f"(EdgeWARN/{package_version}, ewsbackend@gmail.com)"

    # No subsystem keeps a copy: not in YAML...
    for name in ("nws", "nexrad", "metar"):
        recorded = (REPO_ROOT / f"config/{name}.yaml").read_text(encoding="utf-8")
        assert "user_agent:" not in recorded

    # ...and not as a literal in the five sites that send it.
    for relative in (
        "src/common/ingest/nws/zone_sync.py",
        "src/common/ingest/nws/main.py",
        "src/common/ingest/nexrad/config.py",
        "src/common/ingest/nexrad/weather_api.py",
        "src/common/ingest/metar.py",
    ):
        source = (REPO_ROOT / relative).read_text(encoding="utf-8")
        assert "EdgeWARN/1.0" not in source
        assert package_version not in source


def test_every_outbound_user_agent_resolves_through_release():
    """The header is built at request time, so no site can pin a stale string."""
    from pathlib import Path

    from common.ingest.nexrad.weather_api import RadarStationCatalog
    from common.ingest.nws.zone_sync import NWSZoneSync
    from util.release import format_user_agent

    expected = format_user_agent()
    assert NWSZoneSync(Path(".")).headers["User-Agent"] == expected
    assert RadarStationCatalog()._headers()["User-Agent"] == expected

    # The NEXRAD module constant is gone, not merely unused.
    from common.ingest.nexrad import config as nexrad_config

    assert not hasattr(nexrad_config, "WEATHER_API_USER_AGENT")


# --- zone_sync pause_seconds: flag default fights constructor default ------

def test_zone_sync_pause_seconds_agrees_with_the_constructor_and_throttles():
    """RESOLVED (Phase 5): YAML now records 0.05, matching the constructor.

    Phase 1 wired the overlay but recorded the effective 0.0, which disabled
    throttling against api.weather.gov. The pause was also in the `as_completed`
    collection loop, downstream of every submitted future, so even a non-zero
    value could not slow the API down. It now runs in the worker.
    """
    from tests.core.config.source_inspect import argparse_defaults

    constructor = param_default(
        "common/ingest/nws/zone_sync.py", "NWSZoneSync.__init__", "pause_seconds"
    )
    flag = argparse_defaults("common/ingest/nws/zone_sync.py")["--pause-seconds"]["default"]

    assert constructor == 0.05
    assert flag is None

    source = (REPO_ROOT / "src/common/ingest/nws/zone_sync.py").read_text(encoding="utf-8")
    assert "pause_seconds=args.pause_seconds" in source

    # The sleep gates the request, not the result bookkeeping.
    worker = source.index("def _fetch_missing_zone")
    collect = source.index("for future in as_completed(futures):")
    assert worker < source.index("time.sleep(self.pause_seconds") < collect

    import yaml

    recorded = yaml.safe_load((REPO_ROOT / "config/nws.yaml").read_text(encoding="utf-8"))
    assert recorded["zone_sync"]["pause_seconds"] == constructor


# --- NEXRAD sites sentinel ------------------------------------------------

def test_nexrad_all_sites_sentinel_is_null_not_an_empty_list():
    """RESOLVED (Phase 3): the sentinel is `null`, matching the code's `None`.

    `pipeline/__init__.py:76` keeps `None` as-is and `station_filter.py:16` then
    applies no site filter, so `null` means "every allowed station". The file
    used to record `[]`, which would have filtered every site away.
    """
    source = (REPO_ROOT / "src/common/ingest/nexrad/pipeline/__init__.py").read_text(encoding="utf-8")
    assert "None if sites is None else" in source

    import yaml

    recorded = yaml.safe_load((REPO_ROOT / "config/nexrad.yaml").read_text(encoding="utf-8"))
    assert recorded["cli"]["sites"] is None


# --- RAP colormap authority drift ----------------------------------------

def test_mappings_json_carries_one_layer_with_no_python_producer():
    """DECISION OWED: delete the orphan, or add a producer.

    `mappings.json` is otherwise exactly `_with_colormap_key` applied to the
    43-layer RAP catalog, so the drift is a single stale entry.
    """
    from EWMRS.rap.config import get_rap_uint16_layers

    mappings = json.loads((REPO_ROOT / "src/EWMRS/mappings.json").read_text(encoding="utf-8"))
    produced = {layer["name"] for layer in get_rap_uint16_layers()}

    assert sorted(set(mappings) - produced) == ["RAP_BestLiftedIndex_180_0mbAGL"]
    assert produced - set(mappings) == set()
    assert len(mappings) == 44


def test_mappings_json_agrees_with_the_python_colormap_matcher():
    """Everything except the orphan is derivable, so it is a duplicate authority."""
    from EWMRS.rap.config import get_rap_uint16_layers

    mappings = json.loads((REPO_ROOT / "src/EWMRS/mappings.json").read_text(encoding="utf-8"))
    for layer in get_rap_uint16_layers():
        assert mappings[layer["name"]] == layer.get("colormap_key")


def test_every_configured_colormap_key_exists_in_colormaps_json():
    from EWMRS.rap.config import get_rap_uint16_layers
    from EWMRS.render.config import get_file_list

    document = json.loads((REPO_ROOT / "src/EWMRS/colormaps.json").read_text(encoding="utf-8"))
    available = {entry["name"] for entry in document[0]["colormaps"]}

    used = {layer["colormap_key"] for layer in get_file_list()}
    used |= {l["colormap_key"] for l in get_rap_uint16_layers() if "colormap_key" in l}

    assert used - available == set()


def test_reflectance_colormap_ternary_is_resolved():
    """RESOLVED: the ternary was dropped when the layers moved to the catalog.

    `get_goes_file_list()` used to compute `colormap_key` with a ternary whose
    `else` arm (`GOES_ABI_C07_Reflectance` and similar) was unreachable, because
    `reflectance_specs` held only C01-C06. Each layer now names its colormap
    outright in ewmrs_render.yaml, so there is no dead branch to transcribe.
    """
    from EWMRS.render.config import get_goes_file_list

    source = (REPO_ROOT / "src/EWMRS/render/config.py").read_text(encoding="utf-8")
    assert "reflectance_specs" not in source
    assert "GOES_ABI_{channel_id}_Reflectance" not in source

    reflectance = [l for l in get_goes_file_list() if l["name"].endswith("_Reflectance")]
    assert len(reflectance) == 6
    assert [l["channel_id"] for l in reflectance] == ["C01", "C02", "C03", "C04", "C05", "C06"]
    assert {l["colormap_key"] for l in reflectance} == {"GOES_RGB_Raw"}


# --- Import-time binding hazards -----------------------------------------

def test_render_config_snapshots_the_file_list_at_import_time():
    """Captures pre-`--base-dir` paths; the loader work must delete or defer it."""
    from EWMRS.render import config as render_config

    assert hasattr(render_config, "file_list")
    source = (REPO_ROOT / "src/EWMRS/render/config.py").read_text(encoding="utf-8")
    assert "\nfile_list = get_file_list()" in source


def test_util_file_binds_paths_at_import_time():
    """`_define_paths()` runs at module scope, before argparse sees --base_dir."""
    source = (REPO_ROOT / "src/util/file.py").read_text(encoding="utf-8")
    tail = source.split("def initialize_filesystem")[1]
    assert 'if platform.system() == "Windows":' in tail
    assert "_define_paths(Path(r\"C:\\EdgeWARN_input\"))" in tail


def test_colormap_json_resolution_depends_on_the_working_directory():
    """`Path.cwd()` is the first candidate, so the token allowlist needs <src_dir>."""
    source = (REPO_ROOT / "src/util/file.py").read_text(encoding="utf-8")
    assert "Path.cwd() / \"colormaps.json\"" in source
    assert 'Path(__file__).resolve().parents[1] / "EWMRS" / "colormaps.json"' in source


def test_run_module_scope_is_outside_a_main_guard():
    """Under Windows `spawn` this re-executes in every child process."""
    source = (REPO_ROOT / "src/run.py").read_text(encoding="utf-8")
    guard_index = source.find('if __name__ == "__main__"')
    assert guard_index != -1
    assert "get_args()" in source[:guard_index], "get_args() is expected at module scope today"


# --- Silent fallbacks ----------------------------------------------------

def test_unknown_rap_transform_name_is_rejected():
    """RESOLVED (Phase 3): an unknown transform name now aborts the run.

    The silent `TRANSFORMS.get(name, lambda x: x)` fallback meant a typo
    published raw Kelvin under a Celsius key. Names are resolved once, before
    extraction, so the failure is early and names the offending product.
    """
    from EdgeWARN.process.integrate.integrate_rap import TRANSFORMS, _transform_for

    source = (REPO_ROOT / "src/EdgeWARN/process/integrate/integrate_rap.py").read_text(encoding="utf-8")
    assert 'TRANSFORMS.get(' not in source

    assert _transform_for({"key": "x"})(5.0) == 5.0
    assert _transform_for({"key": "x", "transform": "kelvin_to_celsius"})(273.15) == 0.0

    with pytest.raises(ValueError, match="unknown RAP transform 'kelvin_to_farenheit'"):
        _transform_for({"key": "temp_2m", "transform": "kelvin_to_farenheit"})

    assert set(TRANSFORMS) == {"kelvin_to_celsius"}


def test_unparseable_derived_formula_aborts_before_extraction():
    """RESOLVED (Phase 4): a bad formula is a config error, not per-cell data.

    Formulas now come from `integration.yaml`, and an unparseable one used to be
    caught inside the per-field loop and written to every cell as None -- which
    an operator could not tell apart from a cell that had no input value. Each
    formula is parsed and grammar-checked once, before extraction.

    Missing *inputs* still null the field per cell: that is a data condition, and
    the check substitutes a stand-in number for every name so it stays out of it.
    """
    from EdgeWARN.process.integrate.integrate_rap import _calculate_derived, _compile_derived

    key, expression = _compile_derived({"key": "depression", "formula": "temp_2m - dewpoint_2m"})
    assert key == "depression"

    with pytest.raises(ValueError, match="does not parse"):
        _compile_derived({"key": "typo", "formula": "temp_2m -"})

    with pytest.raises(ValueError, match="is not evaluable"):
        _compile_derived({"key": "unsafe", "formula": "__import__('os').getcwd()"})

    cells = [{"properties": {"temp_2m": 20.0}}]
    _calculate_derived(cells, expression, key, 2)
    assert cells[0]["properties"]["depression"] is None


def test_integration_output_rounding_reads_output_decimals():
    """RESOLVED (Phase 4): `integration.yaml output.decimals` is now live.

    The same 2 was hardcoded at three separate rounding sites, so an operator
    changing precision had to find all three. They now share one YAML value.
    The AzShear `round(x, 3)` / `np.round(x, 4)` sites are deliberately excluded:
    those are fixed feature-vector precisions, not an output-formatting choice.
    """
    from EdgeWARN.process.integrate.config import output_decimals

    assert output_decimals() == 2

    for relative_path in (
        "src/EdgeWARN/process/integrate/integrate_rap.py",
        "src/EdgeWARN/process/integrate/core/stats.py",
        "src/EdgeWARN/process/integrate/core/integrator.py",
    ):
        source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert "decimals" in source, relative_path
        assert ", 2)" not in source, relative_path


# --- NEXRAD concurrency: one value, two declarations ----------------------

def test_max_chunk_downloads_has_a_single_owner_in_nexrad_yaml():
    """RESOLVED (Phase 5): 64 was declared by the service *and* the CLI.

    The plan attributed the default to `s3_chunks.py`, which never held it; it
    lived in `NexradIngestService.__init__` and was restated by `main.py`, so a
    catalog key had to displace both. Both now pass None and the service
    resolves from `nexrad.yaml`.

    The defaults must stay None rather than calling the accessor inline:
    `run.py` imports this package before `get_args()` exports
    EDGEWARN_CONFIG_DIR, and a signature default binds at import time.
    """
    from common.ingest.nexrad import config as nexrad_config

    assert nexrad_config.max_chunk_downloads() == 64

    for module, qualname in (
        ("common/ingest/nexrad/service.py", "NexradIngestService.__init__"),
        ("common/ingest/nexrad/main.py", "NexradIngestService.__init__"),
    ):
        assert param_default(module, qualname, "max_chunk_downloads") is None, module

    for relative_path in (
        "src/common/ingest/nexrad/service.py",
        "src/common/ingest/nexrad/main.py",
        "src/common/ingest/nexrad/s3_chunks.py",
    ):
        source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert "max_chunk_downloads=64" not in source, relative_path


@pytest.mark.parametrize(
    "module_name",
    ["common.ingest.nexrad.main", "common.ingest.nexrad.pipeline"],
)
def test_nexrad_entry_points_export_the_config_root_for_parse_workers(
    module_name, tmp_path, monkeypatch
):
    """RESOLVED (Phase 5): `--config-dir` never reached the parse workers.

    Both NEXRAD entry points honoured the flag by passing `config_dir=` to
    `load_config`, which configures the *parent* only. Parse workers are
    ProcessPoolExecutor children that receive no config in their submit payload
    (see `worker_pool._worker_parse`), so their sole channel is the inherited
    environment. Before this fix the parent read the override while its workers
    walked up to the repo default -- a split-brain config, not an error.

    Asserted behaviourally rather than by grepping for the call, so that moving
    or renaming it cannot pass while the export silently stops happening.

    The Namespace comes from the module's own parser rather than being assembled
    by hand: every flag the entry point defines is then present with its real
    default, so adding one cannot break this test with an AttributeError that
    says nothing about the export.
    """
    import importlib
    import shutil

    shutil.copytree(REPO_ROOT / "config", tmp_path / "config")
    config_dir = tmp_path / "config"

    monkeypatch.delenv("EDGEWARN_CONFIG_DIR", raising=False)
    module = importlib.import_module(module_name)
    args = module._build_parser().parse_args(["--config-dir", str(config_dir)])

    module._resolve_cli_args(args)

    from common.config import loader

    assert os.environ.get("EDGEWARN_CONFIG_DIR") == str(config_dir)
    # The no-argument path is the one a spawned worker takes.
    assert loader.config_root() == config_dir
    loader.reset_cache()


# --- EWMRS pipeline: three owners of 120, and one parameter nobody reads ---

def _ewmrs_pipeline_yaml() -> dict:
    import yaml

    return yaml.safe_load(
        (REPO_ROOT / "config" / "ewmrs_pipeline.yaml").read_text(encoding="utf-8")
    )


def _ewmrs_pipeline_source() -> str:
    return (REPO_ROOT / "src/EWMRS/pipeline.py").read_text(encoding="utf-8")


def _param_loads(relative_path: str, qualname: str, param: str) -> int:
    """Count reads of ``param`` inside a function body.

    The signature declares the parameter as an ``ast.arg``, not a ``Name``, so a
    count of zero means the function accepts the parameter and never uses it.
    """
    import ast

    from tests.core.config.source_inspect import SRC, _find_function

    function = _find_function(
        ast.parse((SRC / relative_path).read_text(encoding="utf-8")), qualname
    )
    return sum(
        1
        for node in ast.walk(function)
        if isinstance(node, ast.Name) and node.id == param and isinstance(node.ctx, ast.Load)
    )


EWMRS_MAX_ENTRIES_SITES = (
    "run_render_pipeline",
    "run_mrms_render_pipeline",
    "run_goes_render_pipeline",
    "ewmrs_tandem_worker",
    "ewmrs_goes_worker",
)


def test_ewmrs_max_entries_has_no_catalog_copy_because_nothing_reads_it():
    """RESOLVED: `ewmrs_pipeline.yaml` deliberately omits `render.max_entries`.

    Five EWMRS functions declared `max_entries: int = 10`, which looked like a
    tunable with five duplicate defaults. It is not a tunable at all:
    `run_render_pipeline` is where the chain terminates and it never reads the
    value, because layer selection comes from the render catalogs rather than a
    count. The parameter survives only because it rides in the GOES render task
    tuple that `cycle.py` queues and `background.py` unpacks positionally.

    So the single owner is `runtime.yaml goes_coordination.render_task_max_entries`
    and a copy in `ewmrs_pipeline.yaml` would be a second owner of a dead value.
    The five declarations are `None` so no literal shadows that owner either.
    """
    from util.runtime.config import section

    recorded = _ewmrs_pipeline_yaml()
    assert "max_entries" not in recorded["render"]

    for qualname in EWMRS_MAX_ENTRIES_SITES:
        assert param_default("EWMRS/pipeline.py", qualname, "max_entries") is None, qualname

    assert _param_loads("EWMRS/pipeline.py", "run_render_pipeline", "max_entries") == 0
    assert section("goes_coordination")["render_task_max_entries"] == 10


def test_ewmrs_declaration_defaults_all_defer_to_the_catalog():
    """RESOLVED: every EWMRS default that shadowed a catalog key is now `None`.

    Listed as a sweep so adding a sixth caller with its own literal fails here
    rather than at the next audit.
    """
    for qualname, param in (
        ("run_render_pipeline", "phase_name"),
        ("run_render_pipeline", "cleanup_after"),
        ("cleanup_old_gui_files", "max_age_minutes"),
        ("_cleanup_old_nexrad_gui_files", "max_age_minutes"),
        ("_maybe_cleanup_goes_gui_files", "max_age_minutes"),
        ("render_pending_nexrad_gui_files", "max_source_age_minutes"),
        ("run_nexrad_render_loop", "poll_interval_seconds"),
    ):
        assert param_default("EWMRS/pipeline.py", qualname, param) is None, (qualname, param)


def test_ewmrs_three_owners_of_120_stay_three_keys():
    """RESOLVED: 120 appeared at five sites meaning three different things.

    GUI output retention, GOES output retention, and NEXRAD *input* freshness all
    read 120, and two of them were restated at call sites. Collapsing them into
    one key would have coupled a retention sweep to a freshness window, so they
    remain three keys that happen to agree today. The literal is gone from the
    module, which is what stops them drifting back into one number.
    """
    from EWMRS.pipeline_config import (
        goes_cleanup_max_age_minutes,
        gui_cleanup_max_age_minutes,
        nexrad_source_max_age_minutes,
    )

    recorded = _ewmrs_pipeline_yaml()
    assert gui_cleanup_max_age_minutes() == recorded["render"]["gui_cleanup_max_age_minutes"] == 120
    assert goes_cleanup_max_age_minutes() == recorded["render"]["goes_cleanup_max_age_minutes"] == 120
    assert nexrad_source_max_age_minutes() == recorded["nexrad_gui"]["retention_minutes"] == 120

    source = _ewmrs_pipeline_source()
    assert "max_age_minutes=120" not in source
    assert "= 120" not in source


def test_ewmrs_goes_cleanup_interval_is_read_per_call_not_frozen_at_import():
    """RESOLVED: the rate limit was an import-time `os.environ` read.

    `_GOES_CLEANUP_MIN_INTERVAL_SECONDS` was computed at module scope, so a
    render worker that imported the module before the environment was arranged
    kept the wrong interval for its whole life. It is now read per sweep.

    The zero floor is kept in the accessor: a negative interval would make the
    elapsed-time comparison always true and disable cleanup entirely.
    """
    from EWMRS.pipeline_config import (
        GOES_CLEANUP_MIN_INTERVAL_ENV,
        goes_cleanup_min_interval_seconds,
    )

    recorded = _ewmrs_pipeline_yaml()
    assert (
        goes_cleanup_min_interval_seconds()
        == recorded["render"]["goes_cleanup_min_interval_seconds"]
        == 300
    )

    source = _ewmrs_pipeline_source()
    assert "_GOES_CLEANUP_MIN_INTERVAL_SECONDS" not in source
    assert "goes_cleanup_min_interval_seconds()" in source

    with mock.patch.dict("os.environ", {GOES_CLEANUP_MIN_INTERVAL_ENV: "45"}):
        assert goes_cleanup_min_interval_seconds() == 45.0
    with mock.patch.dict("os.environ", {GOES_CLEANUP_MIN_INTERVAL_ENV: "-5"}):
        assert goes_cleanup_min_interval_seconds() == 0.0
    assert goes_cleanup_min_interval_seconds() == 300.0


def test_ewmrs_worker_budget_is_coupled_to_the_goes_phase_name():
    """DECISION PRESERVED: the memory budget is selected by a phase-name prefix.

    `worker_budget_mb` dispatches on `phase_name.upper().startswith("GOES")`, so
    the phase label is not a free-form string -- renaming the GOES phase would
    silently halve its budget. `render.phase_name` is pinned here alongside the
    budgets to make that coupling fail loudly rather than quietly.
    """
    from EWMRS.pipeline_config import render_phase_name, worker_budget_mb

    recorded = _ewmrs_pipeline_yaml()
    assert render_phase_name() == recorded["render"]["phase_name"] == "EWMRS"
    assert not render_phase_name().upper().startswith("GOES")

    assert worker_budget_mb("GOES") == recorded["workers"]["budget_mb"]["goes"] == 1200.0
    assert worker_budget_mb("MRMS") == recorded["workers"]["budget_mb"]["default"] == 768.0
    assert worker_budget_mb(render_phase_name()) == 768.0

    source = _ewmrs_pipeline_source()
    assert "1200.0" not in source
    assert "768.0" not in source


def test_ewmrs_worker_memory_env_vars_still_outrank_the_catalog():
    """RESOLVED: both budgets kept their environment overrides.

    One variable covers the GOES and default budgets, which means setting it
    flattens the distinction the catalog draws. That is the pre-extraction
    behavior and is preserved rather than split, because splitting it would
    invent a variable no deployment sets.
    """
    from EWMRS.pipeline_config import (
        WORKER_BUDGET_MB_ENV,
        WORKER_RESERVE_MB_ENV,
        worker_budget_mb,
        worker_psutil_fallback_max,
        worker_reserve_mb,
    )

    recorded = _ewmrs_pipeline_yaml()
    assert worker_reserve_mb() == recorded["workers"]["reserve_mb"] == 1024.0
    assert worker_psutil_fallback_max() == recorded["workers"]["psutil_fallback_max"] == 2

    with mock.patch.dict("os.environ", {WORKER_BUDGET_MB_ENV: "256.5"}):
        assert worker_budget_mb("GOES") == 256.5
        assert worker_budget_mb("MRMS") == 256.5
    with mock.patch.dict("os.environ", {WORKER_RESERVE_MB_ENV: "64"}):
        assert worker_reserve_mb() == 64.0

    # 1024.0 is not asserted absent: the bytes-to-MiB conversion legitimately
    # uses it twice, and that arithmetic is not a tunable.
    source = _ewmrs_pipeline_source()
    assert "worker_reserve_mb()" in source
    assert "worker_psutil_fallback_max()" in source
    assert "EWMRS_WORKER" not in source


def test_ewmrs_worker_strategy_is_pinned_by_the_schema_not_an_accessor():
    """DECISION PRESERVED: `workers.strategy` has exactly one implementation.

    `_adaptive_process_worker_count` is the only strategy in the tree. Rather
    than an accessor that raises on anything else -- unreachable, since the
    loader would have to accept the value first -- the schema carries a
    single-member enum, so an unknown strategy fails at load with the file and
    key named.
    """
    recorded = _ewmrs_pipeline_yaml()
    assert recorded["workers"]["strategy"] == "adaptive_memory"

    schema = json.loads(
        (REPO_ROOT / "config/schema/ewmrs_pipeline.schema.json").read_text(encoding="utf-8")
    )
    strategy_schema = schema["properties"]["workers"]["properties"]["strategy"]
    assert strategy_schema == {"enum": ["adaptive_memory"]}


def test_ewmrs_numeric_thread_caps_are_one_value_across_a_fixed_list():
    """RESOLVED: four BLAS-family variables share one cap, set to 1.

    The catalog cannot express per-variable caps because the code sets them in a
    single loop, so the list and the value are separate keys rather than a map.
    The cap is 1 so the process pool owns the parallelism; letting each worker
    thread out would oversubscribe every core.
    """
    import os as _os

    from EWMRS.pipeline import _configure_numerical_thread_caps
    from EWMRS.pipeline_config import numeric_thread_cap_value, numeric_thread_cap_variables

    recorded = _ewmrs_pipeline_yaml()["workers"]["numeric_thread_caps"]
    assert numeric_thread_cap_value() == recorded["value"] == 1
    assert list(numeric_thread_cap_variables()) == recorded["variables"]
    assert numeric_thread_cap_variables() == (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    )

    with mock.patch.dict("os.environ", {}, clear=True):
        _configure_numerical_thread_caps()
        for variable in numeric_thread_cap_variables():
            assert _os.environ[variable] == str(numeric_thread_cap_value())

    source = _ewmrs_pipeline_source()
    assert "OMP_NUM_THREADS" not in source


def test_ewmrs_tile_threads_env_bypasses_the_cpu_cap_but_the_catalog_does_not():
    """DECISION PRESERVED: the override and the catalog value are not equivalent.

    `EWMRS_TILE_THREADS` returns `min(tile_count, cap)`, skipping the CPU count;
    the catalog value goes through `min(tile_count, cap, cpu_count)`. Routing
    both through `overlay.resolve` would have been tidier and would have changed
    behavior on any machine with fewer than 8 cores, so the two branches stay.
    """
    import os as _os

    from EWMRS.pipeline_config import TILE_THREADS_ENV, max_tile_threads
    from EWMRS.render.render import _resolve_tile_workers

    recorded = _ewmrs_pipeline_yaml()
    assert max_tile_threads() == recorded["render_threads"]["max_tile_threads"] == 8

    cpu_count = max(1, _os.cpu_count() or 1)
    with mock.patch.dict("os.environ", {}, clear=True):
        assert _resolve_tile_workers(1000) == min(8, cpu_count)
    with mock.patch.dict("os.environ", {TILE_THREADS_ENV: "1000"}):
        assert _resolve_tile_workers(1000) == 1000

    render_source = (REPO_ROOT / "src/EWMRS/render/render.py").read_text(encoding="utf-8")
    assert "min(tile_count, max_tile_threads(), cpu_cap)" in render_source


def test_ewmrs_lru_cache_sizes_come_from_the_catalog_at_import():
    """DECISION PRESERVED: two keys are read once at import, not per call.

    `maxsize` is a decorator argument, so Python evaluates it when the module
    loads and a cache cannot be resized per call. Every other key in this file is
    read per use; these two need a restart, which is why the catalog says so.
    """
    from EWMRS.pipeline import _load_timestamp_chunk_index_cached
    from EWMRS.pipeline_config import colormap_cache_entries, tile_index_cache_entries
    from EWMRS.render.render import _get_cached_cmap

    recorded = _ewmrs_pipeline_yaml()
    assert tile_index_cache_entries() == recorded["caches"]["tile_index_entries"] == 512
    assert colormap_cache_entries() == recorded["caches"]["colormap_entries"] == 128

    assert _load_timestamp_chunk_index_cached.cache_info().maxsize == tile_index_cache_entries()
    assert _get_cached_cmap.cache_info().maxsize == colormap_cache_entries()


def test_ewmrs_nexrad_render_loop_floor_and_width_come_from_the_catalog():
    """RESOLVED: the poll floor was a bare `max(1.0, ...)` beside its own default.

    The floor exists so a caller passing 0 cannot spin the loop, and it is a
    separate key from the interval because it is a safety bound rather than a
    cadence. `max_workers` is a ceiling, not the pool size -- the effective width
    is capped by the number of pending artifacts.
    """
    from EWMRS.pipeline_config import (
        nexrad_poll_interval_min_seconds,
        nexrad_poll_interval_seconds,
        nexrad_render_max_workers,
    )

    recorded = _ewmrs_pipeline_yaml()["nexrad_gui"]
    assert nexrad_poll_interval_seconds() == recorded["poll_interval_seconds"] == 30.0
    assert nexrad_poll_interval_min_seconds() == recorded["poll_interval_min_seconds"] == 1.0
    assert nexrad_render_max_workers() == recorded["max_workers"] == 8

    source = _ewmrs_pipeline_source()
    assert "max(nexrad_poll_interval_min_seconds(), float(poll_interval_seconds))" in source
    assert "min(nexrad_render_max_workers(), len(pending_metadata))" in source


def test_ewmrs_generic_render_phase_still_cleans_up_by_default():
    """RESOLVED: `cleanup_after` is a catalog key, and GOES still opts out.

    The GOES phase passes `cleanup_after=False` explicitly because it runs its
    own rate-limited sweep afterwards. That literal is a caller decision, not a
    default, so it stays in code -- but the default it overrides is now the
    catalog's.
    """
    from EWMRS.pipeline_config import render_cleanup_after

    recorded = _ewmrs_pipeline_yaml()
    assert render_cleanup_after() is recorded["render"]["cleanup_after"] is True

    source = _ewmrs_pipeline_source()
    assert "cleanup_after=False," in source
    assert "cleanup_after: bool | None = None," in source




# --- MRMS/GOES: --config-dir could not reach the ingest catalog ------------

def test_mrms_catalog_values_are_resolved_per_call_not_at_import():
    """RESOLVED (Phase 5): `--config-dir` reached none of these values.

    `config.py` bound five values at module scope -- two S3 buckets, the ABI
    product, the channel-id tuple, and `GoesIngestSpec.max_files` as a dataclass
    field default. `src/run.py:9` imports this module 23 lines before `get_args()`
    calls `export_config_root`, so every one of them froze the repo-default
    catalog. An operator passing `--config-dir` was silently ingesting from the
    default bucket, not theirs.

    Asserting the module attributes are *absent* is the load-bearing half: an
    accessor that agrees with a surviving constant still leaves two owners.
    """
    from common.ingest.mrms import config as mrms_config

    for name in ("bucket", "ABI_RADC_PRODUCT", "DEFAULT_ABI_RADC_CHANNEL_IDS"):
        assert not hasattr(mrms_config, name)

    # `goes_bucket` kept its name but is now callable rather than a bound string.
    assert callable(mrms_config.goes_bucket)

    source = (REPO_ROOT / "src/common/ingest/mrms/config.py").read_text(encoding="utf-8")
    # The field default was the subtlest of the five: it reads as per-instance but
    # Python evaluates it once, at class-definition time.
    assert 'max_files: int | None = _FROM_CATALOG' in source
    assert '_catalog()["goes"]["max_files_per_spec"]' not in source.split("def goes_max_files_per_spec")[0]


def test_config_dir_reaches_mrms_values_after_the_module_is_already_imported(tmp_path):
    """The regression test for the ordering that made the bug invisible.

    Importing first and exporting second is not artificial -- it is exactly what
    `src/run.py` does. A test that exported before importing would pass against
    the old module constants too, and so would prove nothing.
    """
    import shutil

    from common.config import loader
    from common.ingest.mrms import config as mrms_config

    config_dir = tmp_path / "config"
    shutil.copytree(REPO_ROOT / "config", config_dir)
    catalog = config_dir / "mrms_goes.yaml"
    catalog.write_text(
        catalog.read_text(encoding="utf-8")
            .replace(mrms_config.mrms_bucket(), "override-mrms")
            .replace(mrms_config.goes_bucket(), "override-goes"),
        encoding="utf-8",
    )

    previous = os.environ.get("EDGEWARN_CONFIG_DIR")
    try:
        loader.export_config_root(config_dir)
        assert mrms_config.mrms_bucket() == "override-mrms"
        assert mrms_config.goes_bucket() == "override-goes"
    finally:
        if previous is None:
            os.environ.pop("EDGEWARN_CONFIG_DIR", None)
        else:
            os.environ["EDGEWARN_CONFIG_DIR"] = previous

    assert mrms_config.mrms_bucket() != "override-mrms"


# --- Integrate: --config-dir must reach every value ------------------------


def test_config_dir_reaches_integrate_values_after_the_modules_are_imported():
    """The two module-scope reads that made --config-dir unreachable are gone.

    `EdgeWARN/pipeline.py` pulls `integrate.core.integrator` and, through it,
    `azshear.constants` in from `src/run.py:14` -- 27 lines before `get_args()`
    calls `export_config_root`. Both files used to read config at module scope,
    so an operator's `--config-dir` was silently ignored. Worse, because
    `integrate.config.section` is memoized, the frozen repo-default entry also
    defeated the correctly-written per-call reads elsewhere in the package.

    Import order here mirrors production deliberately: import first, export
    second. A module-scope read reintroduced anywhere on that chain fails this.
    """
    import shutil
    import tempfile

    from common.config import loader
    from EdgeWARN.process.integrate import config as integrate_config
    from EdgeWARN.process.integrate.azshear import constants as azshear_constants

    config_dir = Path(tempfile.mkdtemp(prefix="cfgdir-integrate-"))
    shutil.copytree(REPO_ROOT / "config", config_dir, dirs_exist_ok=True)
    catalog = config_dir / "integration.yaml"
    catalog.write_text(
        catalog.read_text(encoding="utf-8")
            .replace("buffer_km: 1.5", "buffer_km: 9.9")
            .replace("min_gate_count: 5", "min_gate_count: 77")
            .replace("ProbSevere: ProbSevere", "ProbSevere: OVERRIDDEN"),
        encoding="utf-8",
    )

    previous = os.environ.get("EDGEWARN_CONFIG_DIR")
    try:
        loader.export_config_root(config_dir)
        loader.reset_cache()
        integrate_config.reset_cache()
        assert azshear_constants.azshear_buffer_km() == 9.9
        assert azshear_constants.azshear_min_gate_count() == 77
        assert integrate_config.probsevere_field_map()["ProbSevere"] == "OVERRIDDEN"
    finally:
        if previous is None:
            os.environ.pop("EDGEWARN_CONFIG_DIR", None)
        else:
            os.environ["EDGEWARN_CONFIG_DIR"] = previous
        loader.reset_cache()
        integrate_config.reset_cache()

    assert azshear_constants.azshear_buffer_km() != 9.9


def test_probsevere_field_map_has_no_module_level_owner():
    """`integrator.py` must not rebind the mapping as a module constant.

    Deleting `PROBSEVERE_FIELD_MAP` is what fixes the import-time freeze above;
    reintroducing it as a convenience alias would restore the bug, since the
    alias is evaluated at import.
    """
    from EdgeWARN.process.integrate.core import integrator

    assert not hasattr(integrator, "PROBSEVERE_FIELD_MAP")


# --- The 120-second scan cadence: two owners, kept distinct ----------------


def test_fallback_and_default_dt_seconds_agree():
    """`detection.fallback_dt_seconds` and `assignment_costs.default_dt_seconds`.

    These are deliberately NOT collapsed: the first is the tracker's fallback
    when elapsed time cannot be derived from timestamps, and belongs to the
    subsystem that owns the timestamps; the second is a cost-function fallback in
    the kalman package, unreachable in production because every live
    `compute_cost` call site forwards a concrete dt.

    They must nonetheless agree. Within one scan the state is propagated with
    `kf.predict(dt_seconds)` and candidates are scored with the same dt, so if
    the two diverged the cost function would score an implied velocity against a
    different baseline than the state was advanced by.
    """
    from common.config.loader import load_config

    detection = load_config("detection")["detection"]["fallback_dt_seconds"]
    kalman = load_config("kalman")["assignment_costs"]["default_dt_seconds"]
    assert detection == kalman == 120.0


def test_update_cells_dt_seconds_has_no_literal_default():
    """`track.py` must not restate the fallback as a signature default.

    The production caller (`detect/main.py`) always computes and passes
    `dt_seconds`, so a literal default there was reachable only from tests --
    and it was a second owner of a catalog value.
    """
    assert param_default(
        "EdgeWARN/process/detect/track.py",
        "StormCellTracker.update_cells",
        "dt_seconds",
    ) is None


# --- Kalman: the unused assignment regularization key is gone -------------


def test_assignment_has_no_second_regularization_owner():
    """`assignment.covariance_regularization` was a duplicate, now deleted.

    It was loaded into `AssignmentConfig` but consumed by nothing; the value
    actually applied to the innovation covariance is
    `filter_internals.innovation_covariance_regularization`, read at
    `filter.py:434`. Two keys, one concept, same number -- so the unread one was
    a second owner waiting to disagree.
    """
    import dataclasses

    from common.config.loader import load_config
    from EdgeWARN.process.detect.kalman.config import AssignmentConfig

    assert "covariance_regularization" not in load_config("kalman")["assignment"]
    fields = {field.name for field in dataclasses.fields(AssignmentConfig)}
    assert "covariance_regularization" not in fields


# --- detect: concurrency and cache bounds are catalog-owned ----------------


def test_dataset_load_worker_count_has_no_literal_owner():
    """`detect.py` must take its pool size from the threaded config.

    The literal `3` matched the three loads submitted below it, so raising it
    bought nothing -- but it also meant an operator diagnosing I/O contention
    had no way to serialize the loads without editing source.
    """
    from tests.core.config.source_inspect import SRC

    source = (SRC / "EdgeWARN/process/detect/detect.py").read_text(encoding="utf-8")
    assert "max_workers=detection_config.dataset_load_max_workers" in source
    assert "max_workers=3" not in source


def test_dataset_load_worker_count_reaches_the_typed_config():
    from common.config.loader import load_config
    from EdgeWARN.process.detect.config import DetectionConfig

    expected = load_config("detection")["detection"]["dataset_load_max_workers"]
    assert DetectionConfig.from_yaml().dataset_load_max_workers == expected


def test_alert_matcher_caches_are_bounded():
    """Both module-level caches were unbounded for the process lifetime.

    They are keyed by `(registry_dir, snapshot)`, so a historical replay -- which
    walks every retained snapshot -- grew them without limit. Eviction is
    insertion-ordered because a run only ever revisits the snapshot for the scan
    it is currently processing.
    """
    from EdgeWARN.process.detect.tools import alert_matcher

    cache = {}
    for index in range(10):
        alert_matcher._store_bounded(cache, ("snapshot", index), index, 4)

    assert list(cache) == [("snapshot", index) for index in range(6, 10)]


def test_alert_matcher_cache_bounds_are_read_from_the_catalog():
    """The bounds must be read per call, not frozen at import.

    `alert_matcher` is imported from `detect/main.py`, so a module-scope read of
    either bound would freeze the repo-default config directory the same way
    `azshear/constants.py` did.
    """
    from tests.core.config.source_inspect import SRC
    from common.config.loader import load_config

    catalog = load_config("detection")["alert_matching"]
    assert catalog["snapshot_cache_max_entries"] >= 1
    assert catalog["geometry_cache_max_entries"] >= 1

    source = (SRC / "EdgeWARN/process/detect/tools/alert_matcher.py").read_text(encoding="utf-8")
    for line in source.splitlines():
        if "cache_max_entries" in line:
            assert line.startswith(" "), f"module-scope read of a cache bound: {line!r}"


# --- MRMS/GOES: the retention and depth literals now have one owner each ------


def _mrms_goes_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "mrms_goes.yaml").read_text(encoding="utf-8"))


def _src_text(relative_path: str) -> str:
    return (REPO_ROOT / "src" / relative_path).read_text(encoding="utf-8")


def test_decompress_chunk_size_has_no_module_level_owner():
    """`s3_common.DECOMPRESS_CHUNK_SIZE` was a second owner of the catalog key.

    It is distinct from `mrms.ncep_https.download_chunk_size_bytes` (8192): that
    one sizes network reads, this one sizes a local gzip expansion. Both are
    real, so the fix was to give this one an accessor, not to merge them.
    """
    from common.ingest.mrms import s3_common
    from common.ingest.mrms.config import mrms_decompress_chunk_size_bytes

    assert not hasattr(s3_common, "DECOMPRESS_CHUNK_SIZE")
    assert mrms_decompress_chunk_size_bytes() == _mrms_goes_yaml()["mrms"]["decompress_chunk_size_bytes"]


def test_goes_hour_lookback_has_one_resolution_site():
    """The five GOES entry points must not restate the window as a default.

    `_get_goes_bucket_paths` is the only consumer, so it is the only resolver;
    the entry points pass `None` through, which keeps a per-call override
    possible without making any of them a second owner.
    """
    from datetime import datetime, timezone

    from common.ingest.mrms.downloader import _get_goes_bucket_paths

    source = _src_text("common/ingest/mrms/downloader.py")
    assert "hour_lookback=3" not in source

    expected = _mrms_goes_yaml()["goes"]["hour_lookback"]
    paths = _get_goes_bucket_paths(datetime(2026, 8, 16, 12, tzinfo=timezone.utc), "ABI-L1b-RadC")
    assert len(paths) == expected


def test_goes_cleanup_retention_is_catalog_owned():
    """Six `max_age_minutes=60` literals in downloader.py, now none."""
    from common.ingest.mrms.config import goes_cleanup_max_age_minutes

    source = _src_text("common/ingest/mrms/downloader.py")
    assert "max_age_minutes=60" not in source
    assert goes_cleanup_max_age_minutes() == _mrms_goes_yaml()["goes"]["cleanup_max_age_minutes"]


def test_mrms_and_goes_cleanup_windows_stay_separate_keys():
    """Both are 60 today and must remain two keys.

    GOES cleanup additionally enforces `max_files_per_spec`, so the two windows
    bound different retention policies; collapsing them would tie a count cap to
    an age cap that has no reason to move with it.
    """
    catalog = _mrms_goes_yaml()
    assert catalog["mrms"]["cleanup_max_age_minutes"] == 60
    assert catalog["goes"]["cleanup_max_age_minutes"] == 60
    assert "max_files_per_spec" in catalog["goes"]


def test_mrms_cleanup_and_pruning_flag_are_catalog_owned():
    """main.py restated the retention window four times and the flag five."""
    from common.ingest.mrms.main import _cleanup_kwargs, _resolve_ingest_args

    source = _src_text("common/ingest/mrms/main.py")
    assert '"max_age_minutes": 60' not in source
    assert "remove_old_files=True" not in source

    catalog = _mrms_goes_yaml()["mrms"]
    assert _cleanup_kwargs() == {"max_age_minutes": catalog["cleanup_max_age_minutes"]}
    assert _resolve_ingest_args(None, None)[1] == catalog["remove_old_files"]


def test_mrms_ingest_depth_is_owned_only_by_runtime_yaml():
    """`mrms_goes.yaml download_max_entries` was a third copy of one number.

    `runtime.yaml cycle.ingest_max_entries` already owned the depth for the
    callers that pass one; main.py's seven `max_entries=10` defaults owned it for
    the callers that do not. Deleting the catalog key and defaulting the
    signatures to `None` leaves exactly one owner.
    """
    from common.config.loader import load_config
    from common.ingest.mrms.main import _ingest_max_entries

    assert "download_max_entries" not in _mrms_goes_yaml()["mrms"]
    assert "max_entries=10" not in _src_text("common/ingest/mrms/main.py")
    assert _ingest_max_entries() == load_config("runtime")["cycle"]["ingest_max_entries"]


# --- package.json is the only owner of the version string -------------------


def test_no_javascript_file_restates_the_package_version():
    """Four sites carried a literal '2.7.0' -- three services and two tests.

    Runnable without Node, which matters: the JS suite cannot run on every
    machine that touches this repo, so the invariant is pinned from Python.

    The `'2.x'` production mask is deliberately NOT covered here. It is not a
    copy of the version -- it is a decision to withhold the version -- and its
    owner is `api.yaml`, which has no production reader yet.
    """
    import json

    version = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))["version"]
    offenders = []
    for path in list((REPO_ROOT / "src").rglob("*.js")) + list((REPO_ROOT / "tests").rglob("*.js")):
        if f"'{version}'" in path.read_text(encoding="utf-8"):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == [], f"version literal restated in {offenders}"


# --- zone_sync: one CLI, two config roots -----------------------------------


def test_zone_sync_resolves_its_identity_from_the_same_root_as_its_settings():
    """RESOLVED: `--config-dir X` gave X's zone settings and the default's UA.

    `_resolve_zone_sync_args` threads `args.config_dir` into every value it
    reads, which looks complete -- but the User-Agent is not one of those values.
    It is built in `NWSZoneSync.__init__` by `format_user_agent()`, which loads
    `runtime.yaml` on its own with `config_dir=None`. Nothing bridged the gap:
    zone_sync never published the root it had resolved, so one run identified
    itself to api.weather.gov out of a config root it was not otherwise using.

    Asserting on `contact` rather than the whole header keeps the test honest
    about which root won -- the version half comes from package.json either way.
    """
    import shutil
    import tempfile

    from common.config import loader
    from common.ingest.nws.zone_sync import _resolve_zone_sync_args

    config_dir = Path(tempfile.mkdtemp(prefix="cfgdir-zonesync-"))
    shutil.copytree(REPO_ROOT / "config", config_dir, dirs_exist_ok=True)
    catalog = config_dir / "runtime.yaml"
    catalog.write_text(
        catalog.read_text(encoding="utf-8")
            .replace("contact: ewsbackend@gmail.com", "contact: relocated@example.com"),
        encoding="utf-8",
    )

    args = argparse.Namespace(
        config_dir=str(config_dir), assets_dir=None, zone_types=None,
        timeout_seconds=None, max_retries=None, max_workers=None,
        pause_seconds=None, progress=None,
    )

    previous = os.environ.get("EDGEWARN_CONFIG_DIR")
    try:
        loader.reset_cache()
        resolved = _resolve_zone_sync_args(args)
        # The settings followed --config-dir before this fix, and still must.
        assert resolved.assets_dir == config_dir.parent / "assets/nws_zones"
        from common.ingest.nws.zone_sync import NWSZoneSync

        header = NWSZoneSync(resolved.assets_dir).headers["User-Agent"]
        assert "relocated@example.com" in header
    finally:
        if previous is None:
            os.environ.pop("EDGEWARN_CONFIG_DIR", None)
        else:
            os.environ["EDGEWARN_CONFIG_DIR"] = previous
        loader.reset_cache()
