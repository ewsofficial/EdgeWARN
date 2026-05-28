import inspect

import pytest

from common.ingest.aws_async_compat import ensure_aiobotocore_endpoint_compat
from aiobotocore.args import AioClientArgsCreator


def test_ensure_aiobotocore_endpoint_compat_adds_default_for_express_session_flag():
    ensure_aiobotocore_endpoint_compat()

    signature = inspect.signature(AioClientArgsCreator.compute_endpoint_resolver_builtin_defaults)
    parameter = signature.parameters.get("s3_disable_express_session_auth")

    if parameter is None:
        pytest.skip("s3_disable_express_session_auth parameter not present in this aiobotocore version")

    assert parameter.default is False
