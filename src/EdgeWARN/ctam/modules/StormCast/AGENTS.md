# AGENTS Guide for `src/EdgeWARN/ctam/modules/StormCast/`

## Purpose
StormCast module entrypoints and package exports.

## Agent guidance
- The heavy logic lives in `core/`; keep package-level exports stable.
- Forecast/history behavior should remain compatible with lineage and CTAM execution.
