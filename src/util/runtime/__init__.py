"""Runtime support for the realtime entry points.

Exports are resolved lazily (PEP 562 ``__getattr__``): importing
``util.runtime`` — or any single submodule such as ``util.runtime.cli`` or
``util.runtime.services`` — no longer eagerly loads every loop and worker.
``.background`` pulls the GOES/METAR/NWS/WPC/NEXRAD ingest stacks and
``.cycle`` pulls the EdgeWARN detection stack, so an eager re-export of both
forced the full scientific import graph on every consumer, which defeats
import isolation between the decomposed services.

The public names are unchanged; ``from util.runtime import X`` keeps working.
"""

_LAZY_EXPORTS = {
    # .background — accessory and NEXRAD loops
    "goes_loop": ".background",
    "goes_render_loop": ".background",
    "metar_loop": ".background",
    "nexrad_ingest_loop": ".background",
    "nexrad_render_loop": ".background",
    "nws_loop": ".background",
    "wpc_loop": ".background",
    # .cycle — primary cycle state and orchestration
    "CycleOutcome": ".cycle",
    "CycleRetryPolicy": ".cycle",
    "CycleStageResult": ".cycle",
    "CycleStateStore": ".cycle",
    "CycleStatus": ".cycle",
    "PersistedCycleState": ".cycle",
    "PrimaryCycleConfig": ".cycle",
    "run_primary_cycle_once": ".cycle",
    # .logging
    "drain_log_queue": ".logging",
    # .processes
    "AccessorySupervisor": ".processes",
    "StartedProcessRegistry": ".processes",
    "stop_process": ".processes",
    # .scheduler
    "load_last_processed_from_stormcells": ".scheduler",
}

__all__ = [
    "AccessorySupervisor",
    "CycleOutcome",
    "CycleRetryPolicy",
    "CycleStageResult",
    "CycleStateStore",
    "CycleStatus",
    "PersistedCycleState",
    "PrimaryCycleConfig",
    "StartedProcessRegistry",
    "drain_log_queue",
    "goes_loop",
    "goes_render_loop",
    "load_last_processed_from_stormcells",
    "metar_loop",
    "nexrad_ingest_loop",
    "nexrad_render_loop",
    "nws_loop",
    "run_primary_cycle_once",
    "stop_process",
    "wpc_loop",
]


def __getattr__(name):
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    return getattr(import_module(module_name, __name__), name)


def __dir__():
    return sorted(set(globals()) | set(_LAZY_EXPORTS))
