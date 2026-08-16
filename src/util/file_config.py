"""Filesystem retention settings read from ``config/filesystem.yaml``.

Accessors rather than module constants so the catalog is read per call: a
``--config-dir`` may be resolved after this module is imported, and a
module-level read would have frozen the repo default at import time.

``base_dir`` is deliberately not exposed. ``util.file`` picks it by platform at
import time, before any ``--config-dir`` or environment override could be
resolved, so the catalog records those three paths for visibility without owning
them; reconfiguration goes through ``initialize_filesystem(base_dir)``.

The scan skip rules are also absent. See ``test_scan_skip_rules_stay_in_code``
for why they are not operator tunables.
"""

from common.config.loader import load_config

_CONFIG_NAME = "filesystem"


def _cleanup_defaults():
    """The ``cleanup_defaults`` section. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)["cleanup_defaults"]


def cleanup_max_age_minutes() -> int:
    """Fallback age budget for the cleaners in ``util.file``.

    This is the floor every caller inherits, not a ceiling: subsystems that own
    their own retention pass it explicitly, so widening this does not widen them.
    """
    return _cleanup_defaults()["max_age_minutes"]


def cleanup_max_files() -> int:
    """Fallback count cap for ``clean_old_files``.

    ``clean_files_by_age`` has no equivalent -- it applies age only -- which is
    why the two cleaners are not interchangeable.
    """
    return _cleanup_defaults()["max_files"]
