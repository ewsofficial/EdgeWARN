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


def mrms_bucket() -> str:
    """The S3 bucket MRMS products are read from."""
    return _catalog()["mrms"]["bucket"]


def mrms_decompress_chunk_size_bytes() -> int:
    """Copy length used when a gzipped MRMS grib is expanded to disk.

    Distinct from ``mrms.ncep_https.download_chunk_size_bytes``: that one sizes
    reads off the network, this one sizes a local ``copyfileobj``.
    """
    return _catalog()["mrms"]["decompress_chunk_size_bytes"]


def mrms_cleanup_max_age_minutes() -> int:
    """Retention window applied to MRMS output directories before a download."""
    return _catalog()["mrms"]["cleanup_max_age_minutes"]


def mrms_remove_old_files() -> bool:
    """Whether an ingest run prunes its output directories at all."""
    return _catalog()["mrms"]["remove_old_files"]


def goes_bucket() -> str:
    """The S3 bucket GOES products are read from."""
    return _catalog()["goes"]["bucket"]


def goes_cleanup_max_age_minutes() -> int:
    """Retention window applied to GOES spec output directories.

    A separate owner from :func:`mrms_cleanup_max_age_minutes` even though both
    are 60 today: GOES cleanup also enforces ``max_files`` per spec, so the two
    windows bound different retention policies and can legitimately diverge.
    """
    return _catalog()["goes"]["cleanup_max_age_minutes"]


def goes_hour_lookback() -> int:
    """How many hourly bucket prefixes a GOES lookup walks back through."""
    return _catalog()["goes"]["hour_lookback"]


def abi_radc_product() -> str:
    """The ABI product every RadC channel spec is built against."""
    return _catalog()["goes"]["abi_product"]


def _abi_channel_definitions():
    return tuple(
        (channel["id"], channel["name"], channel["outdir"])
        for channel in _catalog()["goes"]["abi_channels"]
    )


def default_abi_radc_channel_ids() -> tuple:
    """Every ABI channel id in the catalog, in catalog order."""
    return tuple(channel_id for channel_id, _, _ in _abi_channel_definitions())


def goes_max_files_per_spec() -> int:
    """The retained-file count cap applied per GOES spec during cleanup."""
    return _catalog()["goes"]["max_files_per_spec"]


_FROM_CATALOG = object()


@dataclass(frozen=True)
class GoesIngestSpec:
    product: str
    outdir: Path
    channel_id: str | None = None
    channel_name: str | None = None
    filename_matcher: str | None = None
    # Resolved in __post_init__, not as a field default: a default is evaluated at
    # class-definition time, which is before get_args() exports EDGEWARN_CONFIG_DIR,
    # so --config-dir could never reach it. A distinct sentinel rather than None
    # because None is a meaningful value for a count cap -- it means "no cap" to
    # util.file.clean_old_files.
    max_files: int | None = _FROM_CATALOG

    def __post_init__(self):
        if self.max_files is _FROM_CATALOG:
            object.__setattr__(self, "max_files", goes_max_files_per_spec())

    @property
    def label(self) -> str:
        return self.channel_name or self.channel_id or self.product

    @property
    def is_glm(self) -> bool:
        return self.channel_id is None and "GLM" in self.product


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


def get_goes_max_entries():
    """The S3 listing depth for every GOES lookup."""
    return _catalog()["goes"]["max_entries"]


def get_abi_radc_channel_specs(channel_ids=None):
    """Build a spec per ABI channel, optionally restricted to ``channel_ids``.

    ``None`` means every channel in the catalog. It used to mean that indirectly,
    via a signature default of ``DEFAULT_ABI_RADC_CHANNEL_IDS`` -- which selected
    every catalog channel and so produced the same result, but bound the channel
    list at import time and put it out of reach of ``--config-dir``.
    """
    selected_channel_ids = set(channel_ids) if channel_ids is not None else None
    specs = []
    product = abi_radc_product()
    matcher_template = _catalog()["goes"]["filename_matcher_template"]

    for channel_id, channel_name, outdir_attr in _abi_channel_definitions():
        if selected_channel_ids is not None and channel_id not in selected_channel_ids:
            continue

        specs.append(
            GoesIngestSpec(
                product=product,
                outdir=_resolve_outdir(outdir_attr),
                channel_id=channel_id,
                channel_name=channel_name,
                filename_matcher=matcher_template.format(channel_id=channel_id),
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
