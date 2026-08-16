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


_TILES = _render_config()["tiles"]
TILE_SIZE = _TILES["tile_size"]
TILE_GRID_ROWS = _TILES["grid_rows"]
TILE_GRID_COLS = _TILES["grid_cols"]

# EWMRS value-chunk wire-format invariants. Keep these values together: they
# are written to both index levels and consumed by API clients.
_CHUNK = _render_config()["chunk_format"]
CHUNK_SCHEMA_VERSION = _CHUNK["wire_version"]
CHUNK_FORMAT_VERSION = _CHUNK["format_version"]
CHUNK_ENCODING = _CHUNK["encoding"]
CHUNK_MEDIA_TYPE = _CHUNK["media_type"]
CHUNK_FILE_SUFFIX = _CHUNK["file_suffix"]
CHUNK_COMPRESSION = _CHUNK["compression"]
CHUNK_BYTES_PER_COMPONENT = _CHUNK["bytes_per_component"]
CHUNK_PIXEL_ROW_ORDER = _CHUNK["pixel_row_order"]
CHUNK_GRID_ORIGIN = _CHUNK["grid_origin"]


def chunk_format_descriptor(*, include_media_type: bool = False) -> dict:
    """Return the JSON-serializable float16 value-chunk contract.

    EWMRS serves raw single-channel science values; derived color products
    (for example GOES RGB composites) are a client-side concern.
    """
    value = {
        "version": CHUNK_FORMAT_VERSION,
        "encoding": CHUNK_ENCODING,
        "file_suffix": CHUNK_FILE_SUFFIX,
        "compression": CHUNK_COMPRESSION,
        "data_type": _CHUNK["data_type"],
        "channels": _CHUNK["channels"],
        "value_kind": _CHUNK["value_kind"],
        "no_data": _CHUNK["no_data"],
        "bytes_per_component": CHUNK_BYTES_PER_COMPONENT,
        "pixel_row_order": CHUNK_PIXEL_ROW_ORDER,
        "grid_origin": CHUNK_GRID_ORIGIN,
    }
    if include_media_type:
        value["media_type"] = CHUNK_MEDIA_TYPE
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
            "display_name": layer["display_name"],
            "value_transform": layer["value_transform"],
            "mask_min": dict(layer["mask_min"]),
            "mask_max": dict(layer["mask_max"]),
        }
        for layer in goes["layers"]
    ]


def get_file_list():
    """Return the combined render configuration list."""
    return get_mrms_file_list() + get_goes_file_list()

# For backward compatibility - returns list at import time (use get_file_list() for dynamic paths)
file_list = get_file_list()
