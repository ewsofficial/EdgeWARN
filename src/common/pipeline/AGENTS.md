# AGENTS Guide for `src/common/pipeline/`

## Purpose
Shared tandem ingest coordination.

## Agent guidance
- This is orchestration code. Favor clarity over cleverness.
- Readiness state changes here can block the entire pipeline, so keep failure paths explicit.
