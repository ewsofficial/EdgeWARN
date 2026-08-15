from dataclasses import dataclass
from pathlib import Path

import util.file as fs
from common.config.loader import ConfigError, load_config

_CONFIG_NAME = "mrms_goes"


def _catalog():
    return load_config(_CONFIG_NAME)


def _resolve_outdir(attribute_name):
    """Map a catalog ``outdir`` attribute name onto the live ``util.file`` path.

    Resolved per call rather than at import so ``initialize_filesystem`` rebinds
    are picked up, and raised as ``ConfigError`` so a typo in the catalog fails
    with the offending file and key instead of a bare ``AttributeError``.
    """
    try:
        return getattr(fs, attribute_name)
    except AttributeError:
        raise ConfigError(
            f"{_CONFIG_NAME}.yaml",
            f"outdir: {attribute_name}",
            "not an attribute of util.file",
        ) from None


bucket = _catalog()["mrms"]["bucket"]
goes_bucket = _catalog()["goes"]["bucket"]


@dataclass(frozen=True)
class GoesIngestSpec:
    product: str
    outdir: Path
    channel_id: str | None = None
    channel_name: str | None = None
    filename_matcher: str | None = None
    max_files: int = _catalog()["goes"]["max_files_per_spec"]

    @property
    def label(self) -> str:
        return self.channel_name or self.channel_id or self.product

    @property
    def is_glm(self) -> bool:
        return self.channel_id is None and "GLM" in self.product


ABI_RADC_PRODUCT = _catalog()["goes"]["abi_product"]

_ABI_CHANNEL_DEFINITIONS = tuple(
    (channel["id"], channel["name"], channel["outdir"])
    for channel in _catalog()["goes"]["abi_channels"]
)

DEFAULT_ABI_RADC_CHANNEL_IDS = tuple(channel_id for channel_id, _, _ in _ABI_CHANNEL_DEFINITIONS)

_FILENAME_MATCHER_TEMPLATE = _catalog()["goes"]["filename_matcher_template"]

def _catalog_triples(key):
    return [
        (entry["region"], entry["product"], _resolve_outdir(entry["outdir"]))
        for entry in _catalog()["mrms"][key]
    ]


def get_mrms_modifiers():
    """The full MRMS ingest catalog as (region, product, outdir) triples."""
    return _catalog_triples("products")


def get_check_modifiers():
    """The readiness-check subset of :func:`get_mrms_modifiers`."""
    return _catalog_triples("check_products")


def get_goes_modifiers():
    goes = _catalog()["goes"]
    return [
        GoesIngestSpec(goes["glm_product"], _resolve_outdir(goes["glm_outdir"])),
        *get_abi_radc_channel_specs(),
    ]


def get_abi_radc_channel_specs(channel_ids=DEFAULT_ABI_RADC_CHANNEL_IDS):
    selected_channel_ids = set(channel_ids) if channel_ids is not None else None
    specs = []

    for channel_id, channel_name, outdir_attr in _ABI_CHANNEL_DEFINITIONS:
        if selected_channel_ids is not None and channel_id not in selected_channel_ids:
            continue

        specs.append(
            GoesIngestSpec(
                product=ABI_RADC_PRODUCT,
                outdir=_resolve_outdir(outdir_attr),
                channel_id=channel_id,
                channel_name=channel_name,
                filename_matcher=_FILENAME_MATCHER_TEMPLATE.format(channel_id=channel_id),
            )
        )

    return specs


def normalize_goes_modifier(spec):
    if isinstance(spec, GoesIngestSpec):
        return spec

    if isinstance(spec, tuple) and len(spec) == 2:
        product, outdir = spec
        return GoesIngestSpec(product=product, outdir=outdir)

    raise TypeError(f"Unsupported GOES modifier specification: {spec!r}")
