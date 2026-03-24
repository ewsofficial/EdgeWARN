# AGENTS Guide for `src/EdgeWARN/ingest/`

## Purpose
Compatibility re-export layer for shared ingest implementations.

## Agent guidance
- Keep these files thin; put real ingest logic in `src/common/ingest/` unless the behavior is EdgeWARN-specific.
