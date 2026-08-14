"""Phase 0 characterization of the filesystem, CLI, and environment surface.

The catalog snapshots cover *what* the pipeline processes. This file covers the
three surfaces that decide *where* it writes, *how* it is invoked, and *which
ambient state* can override a value -- the three that a config loader has to
subsume without changing anything.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path, PurePath

import pytest

from tests.core.config.baseline import assert_baseline
from tests.core.config.source_inspect import SRC, argparse_defaults

REPO_ROOT = SRC.parent

CLI_MODULES = [
    "util/io.py",
    "common/ingest/nexrad/main.py",
    "common/ingest/nexrad/pipeline/__init__.py",
    "common/ingest/nws/zone_sync.py",
]


# --- Filesystem layout ----------------------------------------------------

def _path_attributes() -> dict[str, PurePath]:
    import util.file as fs

    return {
        name: value
        for name, value in vars(fs).items()
        if not name.startswith("_") and isinstance(value, PurePath)
    }


def test_derived_directory_names_baseline():
    """Every artifact directory, as a base-relative name.

    The migration proposes moving these into `paths.yaml`. Snapshotting the
    relative names rather than the absolute paths means the baseline holds on
    any machine, and a renamed directory shows up as a diff.
    """
    import util.file as fs

    base = Path(fs.BASE_DIR)
    relative = {}
    for name, value in sorted(_path_attributes().items()):
        try:
            relative[name] = Path(value).relative_to(base).as_posix()
        except ValueError:
            relative[name] = f"abs:{Path(value).name}"

    assert_baseline("filesystem_path_names", relative)


def test_path_attribute_count_and_uniqueness():
    """113 names, no two pointing at the same directory.

    Uniqueness is what makes the snapshot harness able to render a path back to
    its attribute name unambiguously; a duplicate would silently alias two
    catalog entries onto one baseline token.
    """
    attributes = _path_attributes()
    assert len(attributes) == 113

    values = [str(value) for value in attributes.values()]
    assert len(values) == len(set(values))


def test_base_dir_is_bound_at_import_time_from_the_platform():
    """DECISION OWED: `paths.yaml` cannot win against an import-time binding.

    `_define_paths()` runs at module scope, so all 113 paths exist before
    argparse or any config file is read. Phase 1 has to defer this.
    """
    import util.file as fs

    assert isinstance(fs.BASE_DIR, PurePath)

    source = (SRC / "util/file.py").read_text(encoding="utf-8")
    tail = source.split("def initialize_filesystem")[1]
    assert "_define_paths(" in tail


def test_cleanup_retention_defaults_are_python_defaults():
    """`retention.yaml` has no way to reach these until they are parameterized."""
    import inspect

    import util.file as fs

    defaults = {}
    for name in (
        "clean_old_files",
        "async_clean_old_files",
        "clean_files_by_age",
        "async_clean_files_by_age",
    ):
        signature = inspect.signature(getattr(fs, name))
        defaults[name] = {
            key: parameter.default
            for key, parameter in signature.parameters.items()
            if parameter.default is not inspect.Parameter.empty
        }

    assert defaults == {
        "clean_old_files": {"max_age_minutes": 60, "max_files": 10},
        "async_clean_old_files": {"max_age_minutes": 60, "max_files": 10},
        "clean_files_by_age": {"max_age_minutes": 60},
        "async_clean_files_by_age": {"max_age_minutes": 60},
    }


def test_cleanup_skip_rules_are_hardcoded():
    """`.idx` is always spared and `.gz` is spared only if unzipped nearby."""
    source = (SRC / "util/file.py").read_text(encoding="utf-8")
    assert ".idx" in source
    assert ".gz" in source


def test_nexrad_manifest_staleness_is_a_python_default():
    """`writer.py:18` holds the retention window for stale site manifests.

    Named by the plan's "retention" category; not covered by
    `test_cleanup_retention_defaults_are_python_defaults` because it lives in
    `nexrad/writer.py`, not `util/file.py`.
    """
    from common.ingest.nexrad.writer import STALE_MANIFEST_MAX_AGE_HOURS

    assert STALE_MANIFEST_MAX_AGE_HOURS == 12

    # The function parameter defaults to the module constant by reference, not
    # a second inline literal, so there is exactly one place to change this.
    source = (SRC / "common/ingest/nexrad/writer.py").read_text(encoding="utf-8")
    assert "max_age_hours: int = STALE_MANIFEST_MAX_AGE_HOURS" in source


# --- Process supervision timers --------------------------------------------

def test_supervisor_restart_policy_baseline():
    """`AccessorySupervisor`'s crash-loop and backoff timers.

    Named by the plan's "timers" category; verified during the plan's
    corrections-table audit but never previously pinned by a test.
    """
    import dataclasses

    from util.runtime.processes import AccessorySupervisor

    defaults = {
        field.name: field.default
        for field in dataclasses.fields(AccessorySupervisor)
        if not isinstance(field.default, dataclasses._MISSING_TYPE)
    }

    assert_baseline("supervisor_restart_policy", defaults)


def test_supervisor_stop_process_join_timeouts_are_hardcoded():
    """`stop_process` gives a process 5s to exit, then 1s after a kill signal."""
    from tests.core.config.source_inspect import param_default

    join_timeout = param_default("util/runtime/processes.py", "stop_process", "join_timeout")
    assert join_timeout == 5

    source = (SRC / "util/runtime/processes.py").read_text(encoding="utf-8")
    assert "process.join(timeout=1)" in source


# --- CLI defaults ---------------------------------------------------------

def test_cli_default_baseline():
    """Every `add_argument` default across all four argparse surfaces.

    Phase 3 makes CLI the top of the precedence chain, which only works if a
    flag can express "unset". A flag whose default is a real value cannot, so
    this snapshot is the list Phase 3 has to convert to `default=None`.
    """
    assert_baseline(
        "cli_defaults",
        {module: argparse_defaults(module) for module in CLI_MODULES},
    )


def test_only_four_modules_define_a_cli():
    found = sorted(
        path.relative_to(SRC).as_posix()
        for path in SRC.rglob("*.py")
        if "add_argument" in path.read_text(encoding="utf-8", errors="ignore")
    )
    assert found == sorted(CLI_MODULES)


@pytest.mark.parametrize("module", CLI_MODULES)
def test_most_flags_cannot_express_unset(module):
    """DECISION OWED: which of these defaults belong in YAML instead.

    A flag with a non-``None`` default always sends a value, so YAML can never
    take effect for it. Counting the two groups per module makes the Phase 3
    conversion measurable rather than a matter of opinion.
    """
    defaults = argparse_defaults(module)
    expressive = sorted(
        flag
        for flag, spec in defaults.items()
        if spec["default"] is None or spec["action"] == "store_true"
    )
    shadowing = sorted(set(defaults) - set(expressive))

    assert_baseline(
        f"cli_shadowing_{module.replace('/', '_').removesuffix('.py')}",
        {"shadows_yaml": shadowing, "can_be_unset": expressive},
    )


# --- Environment variables ------------------------------------------------

_ENV_PATTERN = re.compile(r"[A-Z][A-Z0-9_]{2,}")


def _python_env_names() -> set[str]:
    """Collect every environment variable name read anywhere under ``src``.

    Names are taken from ``os.environ`` subscripts and ``get``/``getenv`` calls,
    including the indirect case where the name is held in a module constant such
    as ``RAP_MAX_AGE_ENV``.
    """
    names: set[str] = set()
    for path in SRC.rglob("*.py"):
        source = path.read_text(encoding="utf-8", errors="ignore")
        if "environ" not in source and "getenv" not in source:
            continue
        tree = ast.parse(source)

        constants = {
            target.id: node.value.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            for target in node.targets
            if isinstance(target, ast.Name)
        }

        for node in ast.walk(tree):
            argument = None
            if isinstance(node, ast.Subscript) and "environ" in ast.dump(node.value):
                argument = node.slice
            elif isinstance(node, ast.Call) and node.args:
                function = node.func
                is_getenv = isinstance(function, ast.Attribute) and function.attr in {"get", "getenv"}
                if is_getenv and "environ" in ast.dump(function.value) or (
                    is_getenv and function.attr == "getenv"
                ):
                    argument = node.args[0]

            if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                if _ENV_PATTERN.fullmatch(argument.value):
                    names.add(argument.value)
            elif isinstance(argument, ast.Name) and argument.id in constants:
                value = constants[argument.id]
                if _ENV_PATTERN.fullmatch(value):
                    names.add(value)
    return names


def test_python_environment_variable_inventory_baseline():
    """DECISION OWED: which of these become documented `env:` aliases.

    The plan's precedence rule is CLI > env > YAML, which requires an explicit
    allowlist. This is that list as it exists today, including the three
    third-party GDAL/PROJ names that are *not* EdgeWARN configuration.
    """
    assert_baseline("environment_variables_python", sorted(_python_env_names()))


def test_environment_variables_are_read_without_a_shared_parser():
    """Each site re-implements its own parse and clamp.

    Only `synoptic/config.py` raises on a malformed value; the rest coerce or
    fall through, so identical malformed input behaves differently per variable.
    """
    synoptic = (SRC / "common/ingest/synoptic/config.py").read_text(encoding="utf-8")
    assert "RAP_MAX_AGE_ENV = \"EDGEWARN_RAP_MAX_AGE_MINUTES\"" in synoptic
    assert "must be a non-negative integer" in synoptic

    readers = sorted(
        path.relative_to(SRC).as_posix()
        for path in SRC.rglob("*.py")
        if "os.environ" in path.read_text(encoding="utf-8", errors="ignore")
        or "getenv" in path.read_text(encoding="utf-8", errors="ignore")
    )
    assert_baseline("environment_reader_modules", readers)


def test_node_reads_only_edgewarn_base_dir():
    """The JS side shares exactly one configuration variable with Python."""
    names: set[str] = set()
    for path in (REPO_ROOT / "src").rglob("*.js"):
        if "node_modules" in path.parts:
            continue
        source = path.read_text(encoding="utf-8", errors="ignore")
        names.update(re.findall(r"process\.env\.([A-Za-z_][A-Za-z0-9_]*)", source))

    assert "EDGEWARN_BASE_DIR" in names
    assert names - {"NODE_ENV", "HOME"} == {"EDGEWARN_BASE_DIR"}


def test_base_dir_alias_is_asymmetric_between_python_and_node():
    """DECISION OWED: Python never reads `EDGEWARN_BASE_DIR`.

    Node resolves its base directory from the environment; Python resolves it
    from `platform.system()` at import time and only accepts an override through
    `initialize_filesystem(base_dir=...)`. Nothing keeps the two in agreement.
    """
    assert "EDGEWARN_BASE_DIR" not in _python_env_names()

    source = (SRC / "util/file.py").read_text(encoding="utf-8")
    assert "platform.system()" in source
    assert "environ" not in source
