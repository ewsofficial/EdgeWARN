"""Phase 3 step 8: uniqueness and coverage invariants over the YAML catalogs.

The Phase 0 snapshots in ``test_catalog_baseline.py`` prove the YAML matches the
source *today*. These tests are the complement: they assert the properties an
editor could break tomorrow without changing any snapshot -- a duplicated
upstream modifier, an output directory claimed twice, a readiness entry that
names a product nobody ingests, a ``filepath`` naming an attribute
``util.file`` does not define.

Lengths are asserted explicitly next to each catalog for the same reason the
Phase 0 tests do it: a silently dropped entry is the likeliest regression, and a
membership test alone would not catch one.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

import util.file as fs
from common.config import loader
from tests.core.config.baseline import requires

REPO_ROOT = Path(__file__).resolve().parents[3]


def duplicates(values):
    """Return the values that appear more than once, with their counts."""
    return {value: count for value, count in Counter(values).items() if count > 1}


@pytest.fixture(scope="module")
def mrms_goes():
    return loader.load_config("mrms_goes")


@pytest.fixture(scope="module")
def render():
    return loader.load_config("ewmrs_render")


@pytest.fixture(scope="module")
def rap_uint16():
    return loader.load_config("ewmrs_rap_uint16")


@pytest.fixture(scope="module")
def integration():
    return loader.load_config("integration")


# --- MRMS ingest and readiness --------------------------------------------

def test_mrms_products_are_unique(mrms_goes):
    products = mrms_goes["mrms"]["products"]
    assert len(products) == 28
    assert duplicates([(p["region"], p["product"]) for p in products]) == {}


def test_mrms_product_outdirs_are_unique(mrms_goes):
    """Two products sharing an output directory would interleave their files."""
    outdirs = [p["outdir"] for p in mrms_goes["mrms"]["products"]]
    assert duplicates(outdirs) == {}


def test_mrms_readiness_is_a_subset_of_ingest(mrms_goes):
    """A readiness check on an un-ingested product would never be satisfied."""
    mrms = mrms_goes["mrms"]
    assert len(mrms["check_products"]) == 12

    ingested = {(p["region"], p["product"]) for p in mrms["products"]}
    orphans = [
        (p["region"], p["product"])
        for p in mrms["check_products"]
        if (p["region"], p["product"]) not in ingested
    ]
    assert orphans == []


def test_mrms_readiness_entries_are_unique(mrms_goes):
    entries = [(p["region"], p["product"]) for p in mrms_goes["mrms"]["check_products"]]
    assert duplicates(entries) == {}


def test_detection_membership_names_ingested_products(mrms_goes):
    """The detection list selects by product name, so a typo silently matches nothing.

    ``None`` is a legitimate member: it is the ProbSevere entry, which has no
    modifier component in its bucket path.
    """
    mrms = mrms_goes["mrms"]
    detection = mrms["membership_lists"]["detection"]
    available = {p["product"] for p in mrms["products"]}
    assert [name for name in detection if name not in available] == []


# --- GOES ABI channels ----------------------------------------------------

def test_goes_abi_channels_are_unique(mrms_goes):
    channels = mrms_goes["goes"]["abi_channels"]
    assert len(channels) == 16
    assert duplicates([c["id"] for c in channels]) == {}
    assert duplicates([c["name"] for c in channels]) == {}
    assert duplicates([c["outdir"] for c in channels]) == {}


# --- Render layers --------------------------------------------------------

def test_mrms_render_layers_are_unique(render):
    layers = render["mrms_layers"]
    assert len(layers) == 15
    assert duplicates([layer["name"] for layer in layers]) == {}
    assert duplicates([layer["filepath"] for layer in layers]) == {}
    assert duplicates([layer["outdir"] for layer in layers]) == {}


def test_goes_render_layers_are_unique(render):
    layers = render["goes_layers"]["layers"]
    assert len(layers) == 16
    assert duplicates([layer["name"] for layer in layers]) == {}
    assert duplicates([layer["channel_id"] for layer in layers]) == {}
    assert duplicates([layer["outdir"] for layer in layers]) == {}


def test_goes_render_layers_name_ingested_channels(render, mrms_goes):
    """A render layer for an un-ingested channel would find no input files."""
    ingested = {c["id"] for c in mrms_goes["goes"]["abi_channels"]}
    layers = render["goes_layers"]["layers"]
    assert [l["channel_id"] for l in layers if l["channel_id"] not in ingested] == []


# --- RAP uint16 layers ----------------------------------------------------

def test_rap_uint16_layers_are_unique(rap_uint16):
    layers = rap_uint16["layers"]
    assert len(layers) == 43
    assert duplicates([layer["name"] for layer in layers]) == {}
    assert duplicates([layer["outdir"] for layer in layers]) == {}


def test_rap_uint16_outdirs_are_relative_not_filesystem_attributes(rap_uint16):
    """These `outdir` values mean something different from every other catalog's.

    Elsewhere `outdir` is a ``util.file`` attribute name. Here it is a directory
    name under ``output_root`` -- ``rap/config.py:13`` builds
    ``fs.GUI_RAP_DIR / name``. Asserting the distinction keeps a future reader
    from "fixing" these into attribute names.
    """
    assert rap_uint16["output_root"] == "GUI_RAP_DIR"
    assert hasattr(fs, rap_uint16["output_root"])
    assert [l["outdir"] for l in rap_uint16["layers"] if hasattr(fs, l["outdir"])] == []


def test_rap_uint16_outdir_cannot_be_derived_from_the_layer_name(rap_uint16):
    """Three of the 43 entries hardcode an outdir that `_outdir()` would not produce.

    ``rap/config.py:13`` is ``name.removeprefix("RAP_")``, but :228, :246 and
    :255 bypass it and substitute a hyphen for an underscore. The catalog must
    therefore keep `outdir` explicit rather than deriving it.
    """
    exceptions = {
        layer["name"]: layer["outdir"]
        for layer in rap_uint16["layers"]
        if layer["outdir"] != layer["name"].removeprefix("RAP_")
    }
    assert exceptions == {
        "RAP_CAPE_0_3km": "CAPE_0-3km",
        "RAP_SRH_0-1km": "SRH-0-1km",
        "RAP_LiftedIndex_Surface_500_1000mb": "LiftedIndex_Surface_500-1000mb",
    }


# --- Integration datasets -------------------------------------------------

def test_integration_stats_dataset_names_and_keys_are_unique(integration):
    datasets = integration["stats_datasets"]
    assert len(datasets) == 25
    assert duplicates([d["name"] for d in datasets]) == {}
    assert duplicates([d["key"] for d in datasets]) == {}


def test_integration_stats_datasets_may_share_a_source_directory(integration):
    """`filepath` is deliberately NOT unique -- several statistics per source.

    Pinned so that a future uniqueness sweep does not add the wrong invariant
    here: four datasets legitimately read MRMS_VIL_DIR.
    """
    counts = duplicates([d["filepath"] for d in integration["stats_datasets"]])
    assert counts, "expected at least one source directory to feed several datasets"
    assert max(counts.values()) == 4


def test_probsevere_field_map_targets_are_unique(integration):
    """Two catalog keys reading one upstream field would be a transcription slip.

    The map does contain one known source typo (`SRH02km` -> `SRW02KM`, from
    integrator.py:31), but it is still a one-to-one mapping.
    """
    field_map = integration["probsevere_field_map"]
    assert len(field_map) == 40
    assert duplicates(list(field_map.values())) == {}


def test_rap_product_transforms_are_known(integration):
    """`transform` names are resolved by lookup, so an unknown one raises at load."""
    requires("xarray")
    from EdgeWARN.process.integrate.integrate_rap import TRANSFORMS

    named = [p["transform"] for p in integration["rap_products"]["products"] if "transform" in p]
    assert named, "expected at least one product to declare a transform"
    assert [name for name in named if name not in TRANSFORMS] == []


def test_rap_isobaric_levels_are_unique_and_descending(integration):
    levels = list(integration["rap_products"]["isobaric_levels_mb"])
    assert len(levels) == 37
    assert duplicates(levels) == {}
    assert levels == sorted(levels, reverse=True)


# --- Filesystem attribute resolution --------------------------------------

def _attribute_names():
    """Every catalog value that is meant to name a ``util.file`` attribute."""
    mrms_goes = loader.load_config("mrms_goes")
    render = loader.load_config("ewmrs_render")
    integration = loader.load_config("integration")
    runtime = loader.load_config("runtime")

    names: set[str] = set()
    for product in mrms_goes["mrms"]["products"]:
        names.add(product["outdir"])
    for product in mrms_goes["mrms"]["check_products"]:
        names.add(product["outdir"])
    for channel in mrms_goes["goes"]["abi_channels"]:
        names.add(channel["outdir"])
    names.add(mrms_goes["goes"]["glm_outdir"])

    for layer in render["mrms_layers"]:
        names.update((layer["filepath"], layer["outdir"]))
    for layer in render["goes_layers"]["layers"]:
        names.update((layer["filepath"], layer["outdir"]))

    for dataset in integration["stats_datasets"]:
        names.add(dataset["filepath"])

    names.add(loader.load_config("ewmrs_rap_uint16")["output_root"])
    names.add(runtime["cycle"]["state_file"]["dir"])
    names.add(runtime["supervisor"]["health_file"]["dir"])
    names.add(runtime["supervisor"]["nexrad_heartbeat_file"]["dir"])
    return names


def test_every_named_directory_resolves_on_util_file():
    """The catalogs address directories by attribute name, resolved with getattr.

    A renamed or dropped ``util.file`` global would otherwise surface as an
    AttributeError deep inside a render worker rather than at load time.
    """
    names = _attribute_names()
    assert len(names) >= 70
    assert sorted(name for name in names if not hasattr(fs, name)) == []


def test_named_directories_are_paths_not_arbitrary_globals():
    """Guards against a name that resolves but to something that is not a path."""
    from pathlib import PurePath

    wrong = sorted(
        name for name in _attribute_names() if not isinstance(getattr(fs, name), PurePath)
    )
    assert wrong == []


# --- API product catalog --------------------------------------------------

def test_api_product_catalog_route_keys_are_unique():
    """`id` and `legacyId` are both used to address a product over HTTP."""
    import json

    catalog_path = REPO_ROOT / "src/api/config/product-catalog.json"
    entries = json.loads(catalog_path.read_text(encoding="utf-8"))
    assert len(entries) == 31

    assert duplicates([entry["id"] for entry in entries]) == {}
    assert duplicates([entry["legacyId"] for entry in entries]) == {}
    assert duplicates([entry["legacyFilePrefix"] for entry in entries]) == {}


def test_api_catalog_matches_the_recorded_field_contract():
    """api.yaml splits the fields into required and optional; colormapId is optional."""
    import json

    api = loader.load_config("api")
    entries = json.loads(
        (REPO_ROOT / "src/api/config/product-catalog.json").read_text(encoding="utf-8")
    )
    assert len(entries) == api["product_catalog"]["entries"]

    for field in api["product_catalog"]["required_fields"]:
        assert [e for e in entries if field not in e] == [], field

    optional = set(api["product_catalog"]["optional_fields"])
    present = {key for entry in entries for key in entry}
    assert present - set(api["product_catalog"]["required_fields"]) == optional
