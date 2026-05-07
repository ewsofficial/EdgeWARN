from __future__ import annotations

import inspect
import threading

from aiobotocore.args import AioClientArgsCreator


_PATCH_LOCK = threading.Lock()


def ensure_aiobotocore_endpoint_compat() -> bool:
    method = AioClientArgsCreator.compute_endpoint_resolver_builtin_defaults
    if getattr(method, "_edgewarn_compat", False):
        return False

    signature = inspect.signature(method)
    parameter = signature.parameters.get("s3_disable_express_session_auth")
    if parameter is None or parameter.default is not inspect.Parameter.empty:
        return False

    with _PATCH_LOCK:
        method = AioClientArgsCreator.compute_endpoint_resolver_builtin_defaults
        if getattr(method, "_edgewarn_compat", False):
            return False

        def _patched(
            self,
            region_name,
            service_name,
            s3_config,
            endpoint_bridge,
            client_endpoint_url,
            legacy_endpoint_url,
            credentials,
            account_id_endpoint_mode,
            s3_disable_express_session_auth=False,
        ):
            return method(
                self,
                region_name,
                service_name,
                s3_config,
                endpoint_bridge,
                client_endpoint_url,
                legacy_endpoint_url,
                credentials,
                account_id_endpoint_mode,
                s3_disable_express_session_auth,
            )

        _patched._edgewarn_compat = True
        AioClientArgsCreator.compute_endpoint_resolver_builtin_defaults = _patched
        return True
