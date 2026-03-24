# AGENTS Guide for `src/EdgeWARN/alerts/`

## Purpose
Alert payload schema plus alert creation, storage, and cleanup.

## Agent guidance
- Maintain stable alert file layout and serialization contracts.
- Cleanup logic must remain safe for the configured runtime base directory.
