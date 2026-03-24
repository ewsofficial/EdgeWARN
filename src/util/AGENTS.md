# AGENTS Guide for `src/util/`

## Purpose
Shared filesystem, handler, I/O, performance, GRIB, and release helpers.

## Agent guidance
- Changes here have wide blast radius across the whole repository.
- File/path cleanup logic must remain safe for the configured base directory.
