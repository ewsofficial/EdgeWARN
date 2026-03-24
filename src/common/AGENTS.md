# AGENTS Guide for `src/common/`

## Scope
Shared implementations used by both EdgeWARN and EWMRS.

## Contents
- `ingest/`: shared remote/local ingest flows.
- `pipeline/`: tandem coordination and staged readiness.

## Agent guidance
- Prefer changes here only when the behavior is intentionally shared by both services.
- Preserve tandem readiness ordering: detection inputs first, render readiness second, EdgeWARN integration readiness last.
