# AGENTS Guide for `src/common/ingest/mrms/`

## Purpose
MRMS discovery, timestamp parsing, async/sync S3 access, HTTPS fallback, and download orchestration.

## Agent guidance
- Preserve fallback behavior between S3 and HTTPS.
- Be careful with modifier-specific bucket paths and minute-level timestamp matching.
- Validate ingest changes with MRMS-focused tests and tandem/integration flows when relevant.
