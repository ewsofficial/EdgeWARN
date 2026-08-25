"""Deprecated primary runner alias (decomposition Phase 5).

The supported primary command is ``run_edgewarn.py``; this module forwards to
it unchanged so existing automation keeps working during the migration window.
It performs no import-time work and no ingest, scientific work, or rendering
of its own.
"""

import sys

from run_edgewarn import main


if __name__ == "__main__":
    print("[Main] run.py is a deprecated alias; use run_edgewarn.py instead.")
    try:
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
