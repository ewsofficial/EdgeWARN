"""Phase 0 tests pinning each known configuration disagreement.

These tests assert the behavior that exists **today**, including where it is
almost certainly a bug. Phase 0 is explicitly value-preserving: the point is to
make each disagreement visible and impossible to change by accident, so that a
later phase resolves it as a reviewed decision rather than a silent side effect
of moving a literal into YAML.

Every test below names the decision the migration still owes.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tests.core.config.source_inspect import module_constant, param_default

REPO_ROOT = Path(__file__).resolve().parents[3]


# --- Kalman: dataclass default disagrees with its own YAML fallback --------

def test_tracking_max_prediction_time_dataclass_and_yaml_fallback_disagree():
    """DECISION OWED: is the real value 6.0 or 10.0?

    The dataclass field and `config/kalman.yaml` both say 6.0, but the
    `.get()` fallback inside `from_yaml` says 10.0. The fallback only applies
    when the key is missing, so today the effective value is 6.0 -- and would
    silently become 10.0 if the key were ever dropped from the YAML.
    """
    path = "EdgeWARN/process/detect/kalman/config.py"
    assert param_default(path, "TrackingConfig.from_yaml", "path") is None
    assert module_constant_in_class(path, "TrackingConfig", "max_prediction_time_minutes") == 6.0

    yaml_value = _kalman_yaml()["tracking"]["max_prediction_time_minutes"]
    assert yaml_value == 6

    # The inline fallback in from_yaml, which disagrees with both of the above.
    source = (REPO_ROOT / "src" / path).read_text(encoding="utf-8")
    assert "tracking_data.get('max_prediction_time_minutes', 10.0)" in source


def module_constant_in_class(relative_path: str, class_name: str, field: str):
    """Read a dataclass field default."""
    import ast

    from tests.core.config.source_inspect import SRC, _literal

    tree = ast.parse((SRC / relative_path).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for statement in node.body:
                if (
                    isinstance(statement, ast.AnnAssign)
                    and isinstance(statement.target, ast.Name)
                    and statement.target.id == field
                    and statement.value is not None
                ):
                    return _literal(statement.value)
    raise AssertionError(f"{class_name}.{field} not found in {relative_path}")


def _kalman_yaml() -> dict:
    import yaml

    return yaml.safe_load((REPO_ROOT / "config" / "kalman.yaml").read_text(encoding="utf-8"))


def test_every_kalman_from_yaml_get_call_still_passes_an_inline_fallback():
    """Phase 4 must delete these; this counts them so the removal is provable."""
    import ast

    from tests.core.config.source_inspect import SRC

    tree = ast.parse((SRC / "EdgeWARN/process/detect/kalman/config.py").read_text(encoding="utf-8"))
    with_fallback = 0
    without_fallback = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get":
            if len(node.args) >= 2:
                with_fallback += 1
            else:
                without_fallback += 1

    assert with_fallback == 19
    # Not one call trusts the YAML to supply the key.
    assert without_fallback == 0


def test_kalman_yaml_sections_consumed_versus_inert():
    """Only three of the file's sections are read by code today."""
    sections = set(_kalman_yaml())
    consumed = {"kalman_filter", "tracking", "assignment"}
    assert consumed <= sections
    assert sections - consumed == {"confidence", "assignment_costs", "filter_internals", "schema_version"}


# --- Lineage overlap: 0.15 is shadowed, not a parallel policy --------------

def test_lineage_overlap_default_is_shadowed_by_the_tracker():
    """DECISION OWED: 0.15 is currently unreachable from the tracked path.

    The plan suggests giving the two values distinct config names. That would
    preserve an inert key: `StormCellTracker.__init__` defaults to 0.10 and passes
    it explicitly into `LineageDetector`, so `DEFAULT_OVERLAP_THRESHOLD = 0.15`
    only applies when a detector is constructed directly.
    """
    detector_default = module_constant(
        "EdgeWARN/process/detect/lineage/detector.py", "DEFAULT_OVERLAP_THRESHOLD"
    )
    tracker_default = param_default(
        "EdgeWARN/process/detect/track.py", "StormCellTracker.__init__", "overlap_threshold"
    )
    spatial_default = param_default(
        "EdgeWARN/process/detect/lineage/spatial.py", "find_overlapping_cells", "overlap_threshold"
    )

    assert detector_default == 0.15
    assert tracker_default == 0.10
    assert spatial_default == 0.0
    assert detector_default != tracker_default

    # The tracker forwards its own value, so the detector default never applies.
    track_source = (REPO_ROOT / "src/EdgeWARN/process/detect/track.py").read_text(encoding="utf-8")
    assert "overlap_threshold=self.overlap_threshold" in track_source


# --- Detection thresholds duplicated across eight declaration sites --------

DETECTION_DEFAULT_FILES = {
    "EdgeWARN/pipeline.py": 3,
    "EdgeWARN/process/detect/detect.py": 1,
    "EdgeWARN/process/detect/main.py": 2,
    "EdgeWARN/process/detect/tools/gatemapper.py": 1,
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
def test_detection_thresholds_are_declared_seven_times_each(param, expected):
    """DECISION OWED: reduce to exactly one base default, in YAML.

    Seven keyword-argument declarations remain. Phase 1 removed the eighth
    (the argparse flag in `util/io.py`, which now defaults to `None` and is
    filled from `detection.yaml` via the CLI/env/YAML overlay). Because every
    remaining caller still re-declares the literal, a `detection.yaml` key
    could never win for those: Phase 4 must drive this count to one.
    """
    sites = _declaration_sites(param, expected)
    assert sites == DETECTION_DEFAULT_FILES
    assert sum(sites.values()) == 7


def test_gatemapper_hard_floor_makes_a_raised_threshold_ineffective():
    """DECISION OWED: parameterize or document as a deliberate cap.

    `min(37.5, self.refl_threshold)` means raising `--refl-threshold` above
    37.5 cannot change the baseline mask, so a `detection.yaml` key that
    appears to control it would be inert for half its range.
    """
    source = (REPO_ROOT / "src/EdgeWARN/process/detect/tools/gatemapper.py").read_text(encoding="utf-8")
    assert "min(37.5, self.refl_threshold)" in source
    # The adaptive rule is fully inline; none of these are parameters.
    assert "np.where(valid_max_refl < 45.0, 37.5, 40.0)" in source
    assert "np.minimum(valid_max_refl, 52.0)" in source

    for literal in ("40.0", "45.0", "52.0"):
        assert f"={literal}" not in source.split("def __init__")[1].split(")")[0]


# --- Two divergent user agents --------------------------------------------

def test_two_user_agent_strings_disagree_on_version_and_contact():
    """DECISION OWED: unify into one template interpolating package.json."""
    zone_sync = param_default(
        "common/ingest/nws/zone_sync.py", "NWSZoneSync.__init__", "user_agent"
    )
    nexrad = module_constant("common/ingest/nexrad/config.py", "WEATHER_API_USER_AGENT")

    assert zone_sync == "(EdgeWARN/1.0, contact@edgewarn.com)"
    assert nexrad == "(EdgeWARN/2.7.0, ewsbackend@gmail.com)"
    assert zone_sync != nexrad

    package_version = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))["version"]
    assert package_version == "2.7.0"
    # Only one of the two tracks the package version, and it does so by copy.
    assert package_version in nexrad
    assert package_version not in zone_sync


# --- zone_sync pause_seconds: flag default fights constructor default ------

def test_zone_sync_pause_seconds_flag_overrides_the_constructor_default():
    """RESOLVED (Phase 1): the flag now defaults to `None` and is filled from YAML.

    `config/nws.yaml` records 0.0 as the effective default, resolved via the
    CLI/env/YAML overlay in `_resolve_zone_sync_args`. The constructor's 0.05
    only applies to direct programmatic callers that skip that resolution.
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

    import yaml

    recorded = yaml.safe_load((REPO_ROOT / "config/nws.yaml").read_text(encoding="utf-8"))
    assert recorded["zone_sync"]["pause_seconds"] == 0.0


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


def test_unparseable_derived_formula_nulls_the_field_silently():
    """A typo in a YAML formula would produce None per cell, not a startup error."""
    source = (REPO_ROOT / "src/EdgeWARN/process/integrate/integrate_rap.py").read_text(encoding="utf-8")
    derived = source.split("def _calculate_derived")[1].split("def ")[0]
    assert 'ast.parse(formula, mode="eval")' in derived
    assert "except Exception:" in derived
    assert "props[key] = None" in derived


def test_integration_output_rounding_is_hardcoded_not_configured():
    """`integration.yaml` records `output.decimals: 2`; nothing reads it."""
    source = (REPO_ROOT / "src/EdgeWARN/process/integrate/integrate_rap.py").read_text(encoding="utf-8")
    assert source.count("round(") >= 2
    assert ", 2)" in source


# --- NEXRAD concurrency: one value, two declarations ----------------------

def test_max_chunk_downloads_is_declared_in_two_places_and_not_in_s3_chunks():
    """Corrects the plan, which attributes this default to `s3_chunks.py`.

    The value 64 is right, but it lives in the service constructor and is
    re-declared by the CLI entry point, so a `runtime.yaml` key would have to
    displace both.
    """
    service = param_default(
        "common/ingest/nexrad/service.py", "NexradIngestService.__init__", "max_chunk_downloads"
    )
    assert service == 64

    main_source = (REPO_ROOT / "src/common/ingest/nexrad/main.py").read_text(encoding="utf-8")
    assert "max_chunk_downloads=64," in main_source

    chunks = (REPO_ROOT / "src/common/ingest/nexrad/s3_chunks.py").read_text(encoding="utf-8")
    assert "max_chunk_downloads" not in chunks


# --- Plan claim that turned out to be wrong ------------------------------

def test_wpc_cleanup_glob_matches_the_generated_filenames():
    """Refutes the plan's claimed `surface_analysis_*.geojson` mismatch.

    `surface_analysis` is only a directory name. Cleanup and the writer agree
    on the `wpc_sfc_` prefix, so there is nothing to reconcile here.
    """
    cleanup = (REPO_ROOT / "src/common/ingest/wpc/main.py").read_text(encoding="utf-8")
    writer = (REPO_ROOT / "src/common/ingest/wpc/downloader.py").read_text(encoding="utf-8")

    assert 'glob("wpc_sfc_*.geojson")' in cleanup
    assert 'f"wpc_sfc_{' in writer
    assert "surface_analysis_" not in cleanup
