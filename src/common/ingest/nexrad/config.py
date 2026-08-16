"""NEXRAD ingest settings read from ``config/nexrad.yaml``.

Accessors rather than module constants, and for a sharper reason than most of
the other catalogs: NEXRAD ingest runs in a child spawned with no argv, and
``src/run.py`` imports this module at line 32 -- before ``get_args()`` exports
``EDGEWARN_CONFIG_DIR`` at ``util/io.py:105``. A module-level read here would
freeze the repo-walk default even in the parent process, so ``--config-dir``
could never reach NEXRAD at all.

For the same reason none of these may be used as a default-argument expression
or wrapped in ``lru_cache``: both bind at import time. Resolve inside the
function body instead. ``load_config`` is memoized on the resolved root, so
per-call reads are cheap.
"""

from common.config import overlay
from common.config.loader import load_config

_CONFIG_NAME = "nexrad"


def _nexrad():
    """The whole document. ``load_config`` is memoized, so this is cheap."""
    return load_config(_CONFIG_NAME)


def chunks_bucket() -> str:
    """S3 bucket holding the live per-volume chunk stream."""
    return _nexrad()["buckets"]["chunks"]


def station_catalog_url() -> str:
    """api.weather.gov endpoint listing radar stations and their current VCP."""
    return _nexrad()["stations"]["catalog_url"]


def station_api_timeout_seconds() -> float:
    """Timeout for one station-catalog fetch."""
    return _nexrad()["stations"]["api_timeout_seconds"]


def station_api_cache_ttl_seconds() -> float:
    """How long a fetched station catalog stays usable before a refetch."""
    return _nexrad()["stations"]["api_cache_ttl_seconds"]


def allowed_vcps() -> frozenset[int]:
    """Volume Coverage Patterns this pipeline will ingest.

    A frozenset because every consumer does a membership test, and because an
    immutable return cannot be mutated by one caller on behalf of the rest.
    """
    return frozenset(_nexrad()["selection"]["allowed_vcps"])


def volume_discovery_timeout_seconds() -> float:
    """Deadline for listing a site's recent volumes.

    Application-level, not an SDK default: a lister that never resolves would
    otherwise wedge a scan cycle indefinitely.
    """
    return _nexrad()["timeouts"]["volume_discovery_seconds"]


def chunk_list_timeout_seconds() -> float:
    """Deadline for listing one volume's chunks."""
    return _nexrad()["timeouts"]["chunk_list_seconds"]


def ingest_timeout_seconds() -> float:
    """Deadline for downloading and writing one volume."""
    return _nexrad()["timeouts"]["ingest_seconds"]


def scan_timeout_seconds() -> float:
    """Deadline for a whole scan cycle across all sites."""
    return _nexrad()["timeouts"]["scan_seconds"]


def cancellation_grace_seconds() -> float:
    """How long a cancelled task is awaited before the cycle moves on.

    Zero is legal and means "do not wait", which is why the pipeline guards on
    truthiness before awaiting.
    """
    return _nexrad()["timeouts"]["cancellation_grace_seconds"]


def heartbeat_stale_seconds() -> float:
    """Age at which the supervisor treats the ingest heartbeat as stale."""
    return _nexrad()["timeouts"]["heartbeat_stale_seconds"]


def heartbeat_startup_grace_seconds() -> float:
    """Grace period before a freshly started child is held to the heartbeat.

    NEXRAD Ingest is the only supervised child with a heartbeat path, which is
    why this lives here rather than in ``runtime.yaml`` beside the other
    supervisor settings -- a value there could never reach a consumer.
    """
    return _nexrad()["timeouts"]["heartbeat_startup_grace_seconds"]


def max_site_tasks() -> int:
    """Ceiling on sites scanned concurrently."""
    return _nexrad()["concurrency"]["max_site_tasks"]


def max_chunk_downloads() -> int:
    """Ceiling on concurrent chunk downloads across all sites."""
    return _nexrad()["concurrency"]["max_chunk_downloads"]


def parse_checkpoint_chunk_interval() -> int:
    """Re-parse the accumulated volume every N chunks."""
    return _nexrad()["concurrency"]["parse_checkpoint_chunk_interval"]


def in_volume_prefetch() -> int:
    """Chunks fetched ahead within one volume.

    The effective value is ``min(this, pending chunks)``; that cap is a bound
    against the data on hand, not a second owner of this number.
    """
    return _nexrad()["concurrency"]["in_volume_prefetch"]


def worker_pool_size() -> int:
    """Parse-worker process count.

    Routed through :mod:`common.config.overlay` so the documented
    ``NEXRAD_WORKER_POOL_SIZE`` override wins over the catalog rather than
    replacing it: the variable used to be read raw, which left the YAML key with
    no way to act as the fallback.
    """
    return overlay.resolve(
        None,
        env_names=("NEXRAD_WORKER_POOL_SIZE",),
        yaml_value=_nexrad()["concurrency"]["worker_pool_size"],
    )


def worker_recycle_interval() -> int:
    """Volumes a parse worker handles before it is replaced. ``<= 0`` disables."""
    return overlay.resolve(
        None,
        env_names=("NEXRAD_WORKER_RECYCLE_INTERVAL",),
        yaml_value=_nexrad()["concurrency"]["worker_recycle_interval"],
    )


def worker_timeout_seconds() -> float:
    """Deadline for one parse-worker task."""
    return overlay.resolve(
        None,
        env_names=("NEXRAD_WORKER_TIMEOUT_SECONDS",),
        yaml_value=_nexrad()["concurrency"]["worker_timeout_seconds"],
    )


def scan_interval_seconds() -> float:
    """Cadence of the new-volume scan cycle."""
    return _nexrad()["realtime"]["scan_interval_seconds"]


def completion_interval_seconds() -> float:
    """Cadence of the pending-volume completion cycle."""
    return _nexrad()["realtime"]["completion_interval_seconds"]


def max_candidate_volumes_per_site() -> int:
    """How many recent volumes per site discovery considers before choosing."""
    return _nexrad()["realtime"]["max_candidate_volumes_per_site"]


def format_perf_ms(started_at: float) -> float:
    """Return elapsed wall-clock time in milliseconds."""
    import time
    return (time.perf_counter() - started_at) * 1000
