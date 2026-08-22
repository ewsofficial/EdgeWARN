"""Inert placeholder. This file is never executed in Phase 1.

Its manifest is deliberately invalid, so nothing would launch it even in a later
phase. It exists so the invalid fixture looks like a real installation rather
than an empty directory.
"""

import sys

if __name__ == "__main__":
    sys.exit(0)
