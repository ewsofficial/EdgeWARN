import time

from common.ingest.mrms.config import get_goes_modifiers
from common.ingest.mrms.downloader import download_goes_product
from common.pipeline.goes_readiness import (
    check_local_glm_ready as _check_local_glm_ready_impl,
    check_local_goes_ready as _check_local_goes_ready_impl,
    get_ewmrs_goes_render_specs as _get_ewmrs_goes_render_specs_impl,
)

from .timing import sleep_for


def get_ewmrs_goes_render_specs():
    return _get_ewmrs_goes_render_specs_impl()


def check_local_goes_ready(dt, *, specs=None):
    candidate_specs = get_ewmrs_goes_render_specs() if specs is None else specs
    return _check_local_goes_ready_impl(dt, specs=candidate_specs)


def wait_for_local_goes_ready(
    dt,
    *,
    specs=None,
    timeout_seconds,
    interval_seconds,
    activity_event=None,
):
    candidate_specs = get_ewmrs_goes_render_specs() if specs is None else specs
    if not candidate_specs:
        return False, None

    timeout_seconds = max(0.0, float(timeout_seconds))
    interval_seconds = max(0.1, float(interval_seconds))
    deadline = time.time() + timeout_seconds

    while True:
        goes_ready, goes_path = check_local_goes_ready(dt, specs=candidate_specs)
        if goes_ready and (activity_event is None or not activity_event.is_set()):
            return True, goes_path

        if time.time() >= deadline:
            return False, None

        sleep_for(min(interval_seconds, max(0.0, deadline - time.time())), interval=0.2)


def check_local_glm_ready(dt):
    return _check_local_glm_ready_impl(dt, specs=get_goes_modifiers())


def download_glm_for_scan(dt):
    glm_spec = next((spec for spec in get_goes_modifiers() if spec.is_glm), None)
    if glm_spec is None:
        return []

    return download_goes_product(glm_spec, dt)
