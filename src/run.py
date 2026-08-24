"""Temporary primary runner adapter (decomposition Phase 4).

The monolith is now fully decomposed into three independently operable
services (plans/realtime-runner-decomposition-plan.md):

- ``src/run_edgewarn.py`` — the supported primary command (this runner is a
  thin alias over the same service functions);
- ``src/run_ewmrs.py`` — EWMRS rendering, GOES ABI ingest/render, and
  METAR/NWS/WPC accessories;
- ``src/run_nexrad.py`` — NEXRAD ingest + rendering.

Since Phase 4 this process runs primary work only: MRMS selection/ingest,
scan-time GLM, detection/integration, cycle state, and publication of the
durable ``mrms-ready``/``rap-ready`` records that ``run_ewmrs.py`` consumes.
It starts no accessory children. It performs no import-time work.
"""

import sys

from common.config import loader as config_loader
from EdgeWARN import initialize_runtime
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from util.io import TimestampedOutput, IOManager
from util.release import get_release_version
from util.runtime.primary_service import (
    build_cycle_config,
    log_effective_flags,
    report_effective_config,
    run_primary_cycle_loop,
)


def main():
    """Run the primary EdgeWARN cycle loop until interrupted."""
    sys.stdout = TimestampedOutput(sys.stdout)
    sys.stderr = TimestampedOutput(sys.stderr)

    io_manager = IOManager("[Main]")
    args = io_manager.get_args()

    initialize_runtime(base_dir=args.base_dir, io_manager=io_manager)

    print("Scheduler started. Press CTRL+C to exit.")
    print("[Scheduler] EWMRS and its accessories are owned by run_ewmrs.py; NEXRAD by run_nexrad.py.")
    log_effective_flags(args)

    cycle_config = build_cycle_config(args)
    run_primary_cycle_loop(
        checker=MRMSUpdateChecker(verbose=True),
        cycle_config=cycle_config,
        supervisor=None,
    )


if __name__ == "__main__":
    try:
        print(f"Running EdgeWARN v{get_release_version()}")
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
    except config_loader.ConfigError:
        # This catches configuration failures reached after argument parsing
        # (for example a malformed filesystem attribute in cycle.state_file).
        # Import-time ConfigError instances cannot be caught here; CI's
        # validate-config gate is responsible for rejecting those before startup.
        report_effective_config()
        raise
