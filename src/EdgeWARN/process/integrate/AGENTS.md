# AGENTS Guide for `src/EdgeWARN/process/integrate/`

## Purpose
Post-detection enrichment using GLM, RAP, statistical datasets, history updates, and integration utilities.

## Agent guidance
- This is a hot path for per-cell data enrichment.
- Prefer localized optimizations that keep outputs identical.
- Be careful with lazy loading, grid slicing, and property naming because API/CTAM/history consumers depend on them.
