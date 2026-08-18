import util.file as fs
from common.config.loader import ConfigError, load_config

_CONFIG_NAME = "ewmrs_render"


def _render_config():
    return load_config(_CONFIG_NAME)


def _resolve_dir(attribute_name):
    """Map a catalog directory attribute name onto the live ``util.file`` path.

    Resolved per call so ``initialize_filesystem`` rebinds are picked up, and
    raised as ``ConfigError`` so a typo in the catalog names the offending key.
    """
    try:
        return getattr(fs, attribute_name)
    except AttributeError:
        raise ConfigError(
            f"{_CONFIG_NAME}.yaml",
            f"outdir/filepath: {attribute_name}",
            "not an attribute of util.file",
        ) from None


def goes_transform_resampling():
    """The ``rasterio`` resampling method for the GOES ABI reprojection.

    Returns the enum member rather than its name so no caller has to repeat the
    lookup, and read per call so ``--config-dir`` can reach it -- ``run.py``
    imports the render package before ``get_args()`` exports
    ``EDGEWARN_CONFIG_DIR``.

    Owns the GOES ABI path only. ``EWMRS/pipeline.py`` reprojects the non-GOES
    layers with ``nearest`` on purpose, to keep radar edges crisp; that is a
    different policy and deliberately not this key.
    """
    from rasterio.enums import Resampling
    from rasterio.warp import SUPPORTED_RESAMPLING

    name = _render_config()["goes_transform"]["resampling"]
    try:
        method = Resampling[name]
    except KeyError:
        raise ConfigError(
            f"{_CONFIG_NAME}.yaml",
            f"goes_transform.resampling: {name}",
            "not a rasterio.enums.Resampling member",
        ) from None

    # The schema enum already excludes `gauss`, but rasterio decides what warp
    # accepts and the two lists are maintained by different projects.
    if method not in SUPPORTED_RESAMPLING:
        raise ConfigError(
            f"{_CONFIG_NAME}.yaml",
            f"goes_transform.resampling: {name}",
            "not supported for reprojection by this rasterio build",
        )
    return method


def tile_size() -> int:
    """Chunk edge length, resolved after entry points select a config root."""
    return _render_config()["tiles"]["tile_size"]


def chunk_schema_version() -> int:
    """Schema version written into both EWMRS chunk index levels."""
    return _render_config()["chunk_format"]["wire_version"]


def chunk_format_descriptor(*, include_media_type: bool = False) -> dict:
    """Return the JSON-serializable float16 value-chunk contract.

    EWMRS serves raw single-channel science values; derived color products
    (for example GOES RGB composites) are a client-side concern.
    """
    chunk = _render_config()["chunk_format"]
    value = {
        "version": chunk["format_version"],
        "encoding": chunk["encoding"],
        "file_suffix": chunk["file_suffix"],
        "compression": chunk["compression"],
        "data_type": chunk["data_type"],
        "channels": chunk["channels"],
        "value_kind": chunk["value_kind"],
        "no_data": chunk["no_data"],
        "bytes_per_component": chunk["bytes_per_component"],
        "pixel_row_order": chunk["pixel_row_order"],
        "grid_origin": chunk["grid_origin"],
    }
    if include_media_type:
        value["media_type"] = chunk["media_type"]
    return value

def nexrad_variable_colormaps() -> dict:
    """Map each served radar moment to the colormap the GUI draws it with.

    Resolved per call rather than bound at module scope: this package is imported
    before ``get_args()`` exports ``EDGEWARN_CONFIG_DIR``, so an import-time read
    would freeze the repo-default config directory and ``--config-dir`` could
    never reach it.

    Hoist the result out of per-sweep and per-moment loops. ``load_config`` stats
    the catalog on every call, cache hit included, so calling this once per
    rendered moment is a measurable cost rather than a style question.

    A moment absent from this mapping is still served by the API; it simply has no
    colormap, so the GUI does not draw it. ``CCORH`` is exactly that case.
    """
    return dict(_render_config()["nexrad_gui"]["variable_colormaps"])


def get_mrms_file_list():
    """Return the MRMS-backed render configuration list."""
    return [
        {
            "name": layer["name"],
            "colormap_key": layer["colormap_key"],
            "filepath": _resolve_dir(layer["filepath"]),
            "outdir": _resolve_dir(layer["outdir"]),
        }
        for layer in _render_config()["mrms_layers"]
    ]


def get_goes_file_list():
    """Return the GOES-backed render configuration list."""
    goes = _render_config()["goes_layers"]
    common = goes["common"]
    return [
        {
            "name": layer["name"],
            "colormap_key": layer["colormap_key"],
            "filepath": _resolve_dir(layer["filepath"]),
            "outdir": _resolve_dir(layer["outdir"]),
            "source_type": common["source_type"],
            "variable_name": common["variable_name"],
            "fallback_variable_names": list(common["fallback_variable_names"]),
            "channel_id": layer["channel_id"],
            "value_transform": layer["value_transform"],
            "mask_min": dict(layer["mask_min"]),
            "mask_max": dict(layer["mask_max"]),
        }
        for layer in goes["layers"]
    ]


def get_file_list():
    """Return the combined render configuration list."""
    return get_mrms_file_list() + get_goes_file_list()
