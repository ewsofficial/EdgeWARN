"""Every source citation in the CTAM docs and findings must resolve to a real line.

These documents argue from the repository: a limit is justified by an existing
constant, a schema restriction by a line in the loader. A citation that points at
a file which does not exist, or a line past the end of one, turns that argument
into an assertion nobody can check -- and it fails silently, because prose does
not run.

It happened during Phase 0: a claim about inactive-cell expiry cited
``src/EdgeWARN/process/integrate/index_manager.py``, a path that has never
existed, while the code it described lives under ``api_integration``. The claim
was true and the citation was wrong, which is the worst combination, because
following it up suggests the claim is fabricated.

Line numbers do drift when cited files are edited, and this test will fail then.
That is the intended cost: the alternative is a document whose citations decay
into noise.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.ctam

REPO_ROOT = Path(__file__).resolve().parents[4]
DOC_DIR = REPO_ROOT / "docs" / "ctam"

# Longest extension first: ".json" must win over ".js", or every JSON citation is
# truncated to a file that does not exist.
CITATION = re.compile(
    r"((?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+\.(?:jsonl|json|yaml|toml|py|js|md|ini))"
    r"((?::[0-9]+(?:-[0-9]+)?)(?:,[0-9]+)*)?"
)

# Paths a later phase creates. Listed with the phase that owns them so an entry
# cannot sit here forever pretending to be a citation.
FORWARD_REFERENCES = {
    "docs/ctam/module-manifest.md": "Phase 1 writes the manifest reference",
    "docs/ctam/module-development.md": "a later phase writes the module author guide",
}

EXPECTED_DOCS = (
    DOC_DIR / "README.md",
    DOC_DIR / "internal-api-limits.md",
    DOC_DIR / "schema" / "README.md",
    REPO_ROOT / "plans" / "modular-ctam-phase0-findings.md",
)

# These documents describe the CTAM package from the inside and name some files
# relative to it (``registry.py``, ``ctam/interface.py``), so a citation is tried
# repo-relative first and then against each of these bases.
BASES = (REPO_ROOT, REPO_ROOT / "src" / "EdgeWARN", REPO_ROOT / "src" / "EdgeWARN" / "ctam")


def resolve(path: str) -> Path | None:
    for base in BASES:
        candidate = base / path
        if candidate.is_file():
            return candidate
    return None


def iter_citations():
    for path in EXPECTED_DOCS:
        label = path.relative_to(REPO_ROOT).as_posix()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in CITATION.finditer(line):
                yield label, number, match.group(1), match.group(2) or ""


def test_the_expected_documents_exist():
    """Guards the scan: a renamed document would cite nothing and pass."""
    for path in EXPECTED_DOCS:
        assert path.is_file(), f"{path.relative_to(REPO_ROOT).as_posix()} is missing"


def test_citations_were_actually_found():
    assert sum(1 for _ in iter_citations()) >= 100


def test_every_cited_file_exists():
    broken = []
    for doc, number, path, _spec in iter_citations():
        if path in FORWARD_REFERENCES:
            continue
        if resolve(path) is None:
            broken.append(f"{doc}:{number} cites missing {path}")
    assert not broken, "\n".join(broken)


def test_every_cited_line_is_within_its_file():
    """A number past the end of the file means the citation was never followed."""
    broken = []
    for doc, number, path, spec in iter_citations():
        if not spec or path in FORWARD_REFERENCES:
            continue
        target = resolve(path)
        if target is None:
            continue
        length = len(target.read_text(encoding="utf-8", errors="replace").splitlines())
        cited = max(int(value) for value in re.findall(r"[0-9]+", spec))
        if cited > length:
            broken.append(f"{doc}:{number} cites {path}:{cited} but the file has {length} lines")
    assert not broken, "\n".join(broken)


def test_forward_references_are_still_in_the_future():
    """Once the file lands, the entry is stale and the citation should be checked."""
    arrived = [path for path in FORWARD_REFERENCES if (REPO_ROOT / path).is_file()]
    assert not arrived, f"now exists, so remove from FORWARD_REFERENCES: {arrived}"


def test_forward_references_are_actually_cited():
    cited = {path for _doc, _number, path, _spec in iter_citations()}
    unused = set(FORWARD_REFERENCES) - cited
    assert not unused, f"recorded as a forward reference but never cited: {sorted(unused)}"
