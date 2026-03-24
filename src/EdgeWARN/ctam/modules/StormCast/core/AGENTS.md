# AGENTS Guide for `src/EdgeWARN/ctam/modules/StormCast/core/`

## Purpose
StormCast forecasting internals: blending, Kalman logic, diagnostics, uncertainty, and types.

## Agent guidance
- This is numerically sensitive code; prefer correctness-first changes with tight tests.
- Small math changes can affect forecasts, diagnostics, and tracker interactions across the pipeline.
