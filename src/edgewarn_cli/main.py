"""Top-level ``edgewarn`` console command.

Phase 1 establishes the installed command, help, and version contract. Service
dispatch and configuration editing are intentionally introduced by later
phases of ``plans/package-command-implementation.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Sequence


_DISTRIBUTION_NAME = "edgewarn-core"
_SOURCE_PACKAGE_JSON = Path(__file__).resolve().parents[2] / "package.json"


def _release_version() -> str:
    """Return installed metadata first, with a source-checkout fallback."""
    try:
        return version(_DISTRIBUTION_NAME)
    except PackageNotFoundError:
        try:
            with _SOURCE_PACKAGE_JSON.open("r", encoding="utf-8") as handle:
                value = json.load(handle).get("version")
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return "unknown"
        return str(value) if value else "unknown"


def _not_implemented(args: argparse.Namespace) -> int:
    args.parser.error(
        f"'edgewarn {args.command}' is registered but will be implemented in "
        "a later package-command phase"
    )
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edgewarn",
        description="Run and configure EdgeWARN backend services.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_release_version()}",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run",
        help="run EdgeWARN services (dispatch is added in Phase 2)",
    )
    run_parser.set_defaults(handler=_not_implemented, parser=run_parser)

    configure_parser = subparsers.add_parser(
        "configure",
        help="modify EdgeWARN configuration (editing is added in Phase 3)",
    )
    configure_parser.set_defaults(handler=_not_implemented, parser=configure_parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the package command and return its process exit status."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
