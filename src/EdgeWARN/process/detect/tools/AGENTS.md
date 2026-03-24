# AGENTS Guide for `src/EdgeWARN/process/detect/tools/`

## Purpose
Detection helpers such as gate mapping, morphology, save logic, vector math, and alert matching.

## Agent guidance
- This folder mixes geometry, morphology, and serialization helpers; keep responsibilities clear.
- Performance work here should be benchmarked because these helpers sit in hot paths.
