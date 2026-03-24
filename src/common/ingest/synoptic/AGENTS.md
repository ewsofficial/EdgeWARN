# AGENTS Guide for `src/common/ingest/synoptic/`

## Purpose
Synoptic/METAR retrieval helpers.

## Agent guidance
- Keep network behavior resilient and easy to mock in tests.
- Preserve timestamp and station-data compatibility for downstream API consumers.
