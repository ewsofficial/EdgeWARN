"""Inert placeholder. This file is never executed in Phase 1.

Discovery reads ``module.toml`` and never launches anything; Phase 4 owns
launching. The file exists only so the manifest's ``entrypoint`` names a payload
that really ships inside the module directory, which is what the containment
check validates.
"""

import sys

if __name__ == "__main__":
    sys.exit(0)
