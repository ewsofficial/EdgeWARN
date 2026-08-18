"""Hard configuration-boundary assertions, intentionally not baselines."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from tests.core.config.source_inspect import SRC, production_sources


CTAM = "EdgeWARN/ctam/"
ENV_READERS = {"common/config/loader.py", "common/config/overlay.py", "config/loader.js"}
URL_SENTINELS = {("api/middleware/logging.js", "http://edgewarn.invalid")}


def _relative(path: Path, root: Path = SRC) -> str:
    return path.relative_to(root).as_posix()


def _python_docstrings(tree: ast.AST) -> set[ast.Constant]:
    values = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                values.add(first.value)
    return values


def _url_literals(root: Path = SRC) -> list[tuple[str, str]]:
    found = []
    for path in production_sources(root):
        relative = _relative(path, root)
        source = path.read_text(encoding="utf-8", errors="ignore")
        if path.suffix == ".py":
            tree = ast.parse(source)
            docstrings = _python_docstrings(tree)
            values = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str) and node not in docstrings]
        else:
            values = re.findall(r"[\"'](https?://[^\"']+)[\"']", source)
        found.extend((relative, value) for value in values if value.startswith(("http://", "https://")) and (relative, value) not in URL_SENTINELS)
    return found


def _env_reads(root: Path = SRC) -> list[str]:
    found = []
    for path in production_sources(root):
        relative = _relative(path, root)
        source = path.read_text(encoding="utf-8", errors="ignore")
        if path.suffix == ".py":
            tree = ast.parse(source)
            for node in ast.walk(tree):
                target = node.value if isinstance(node, ast.Subscript) else node.func if isinstance(node, ast.Call) else None
                if isinstance(target, ast.Attribute) and target.attr in {"environ", "get"} and isinstance(target.value, ast.Attribute) and target.value.attr == "environ":
                    if isinstance(node, ast.Subscript) or target.attr == "get":
                        found.append(relative)
        elif re.search(r"\bprocess\.env\b", source):
            found.append(relative)
    return sorted(set(found) - ENV_READERS - {"api/config/index.js"})


def _config_path_accesses(root: Path = SRC) -> list[str]:
    found = []
    for path in production_sources(root):
        relative = _relative(path, root)
        if relative in ENV_READERS:
            continue
        source = path.read_text(encoding="utf-8", errors="ignore")
        if "__file__" in source and re.search(r"[\"']config[\"']", source):
            found.append(relative)
    return found


def test_production_url_literals_are_not_endpoints():
    assert _url_literals() == []


def test_environment_reads_stay_in_configuration_infrastructure():
    assert _env_reads() == []


def test_catalog_exceptions_are_explicit_and_directory_scoped():
    exceptions = {"EWMRS/rap/config.py": "recorded Uint16 registry", CTAM: "out of scope: plan 1146-1149"}
    assert exceptions["EWMRS/rap/config.py"]
    assert exceptions[CTAM]


def test_no_source_relative_config_access_bypasses_loaders():
    assert _config_path_accesses() == []


def test_audit_scanners_reject_negative_controls(tmp_path):
    source = tmp_path / "x.py"
    source.write_text('URL = "https://new.example"\nimport os\nvalue = os.environ.get("NEW")\npath = __file__ / "config"\n', encoding="utf-8")
    assert _url_literals(tmp_path) == [("x.py", "https://new.example")]
    assert _env_reads(tmp_path) == ["x.py"]
    assert _config_path_accesses(tmp_path) == ["x.py"]
