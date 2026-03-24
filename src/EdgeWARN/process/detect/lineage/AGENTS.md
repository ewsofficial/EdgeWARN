# AGENTS Guide for `src/EdgeWARN/process/detect/lineage/`

## Purpose
Merge/split lineage detection, buffering, event models, and spatial helpers.

## Agent guidance
- Favor deterministic behavior and careful edge-case handling.
- Lineage changes can affect history continuity and downstream CTAM behavior.
