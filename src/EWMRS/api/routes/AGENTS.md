# AGENTS Guide for `src/EWMRS/api/routes/`

## Purpose
EWMRS route handlers.

## Agent guidance
- Protect against path traversal and malformed parameters.
- Keep response semantics stable because external consumers may fetch files directly.
