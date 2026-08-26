"""Startup preflight tests for the standalone EWMRS service."""

from unittest.mock import patch

import run_ewmrs


def test_nws_zone_assets_are_required_when_nws_is_enabled():
    with patch("common.ingest.nws.geomapper.ensure_zone_assets") as ensure:
        run_ewmrs._require_nws_zone_assets(nws_enabled=True)

    ensure.assert_called_once_with()


def test_nws_zone_assets_are_not_required_when_nws_is_disabled():
    with patch("common.ingest.nws.geomapper.ensure_zone_assets") as ensure:
        run_ewmrs._require_nws_zone_assets(nws_enabled=False)

    ensure.assert_not_called()
