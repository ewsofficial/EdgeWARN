# VCP Scan Elevations Reference

Generated: 2026-05-06T09:55:29-04:00

This file summarizes the sweep elevations and waveform types observed for the VCPs explored in this session.

## Notes
- Elevations are taken from parsed `xradar` DataTree sweep metadata.
- Some VCPs split the same nominal elevation into separate sweep groups for different waveform/product types.
- Values shown are empirical from sampled volumes, not copied from a static ROC table.

## VCP-212

Sweep layout observed:
- `sweep_00` -> `0.5273°` -> `contiguous_surveillance`
- `sweep_01` -> `0.5273°` -> `contiguous_doppler`
- `sweep_02` -> `0.8350°` -> `contiguous_surveillance`
- `sweep_03` -> `0.8789°` -> `contiguous_doppler`
- `sweep_04` -> `1.3184°` -> `contiguous_surveillance`
- `sweep_05` -> `1.3184°` -> `contiguous_doppler`
- `sweep_06` -> `1.8018°` -> `staggered_pulse_pair`
- `sweep_07` -> `2.4170°` -> `staggered_pulse_pair`
- `sweep_08` -> `3.1201°` -> `staggered_pulse_pair`
- `sweep_09` -> `3.9990°` -> `staggered_pulse_pair`
- `sweep_10` -> `5.0977°` -> `staggered_pulse_pair`
- `sweep_11` -> `6.4160°` -> `staggered_pulse_pair`
- `sweep_12` -> `7.9541°` -> `batch`
- `sweep_13` -> `9.9756°` -> `batch`
- `sweep_14` -> `12.5244°` -> `batch`

Lowest unique elevations observed:
- `0.5273°`
- `0.8350°`
- `0.8789°`
- `1.3184°`
- `1.8018°`
- `2.4170°`

## VCP-215

Observed from:
- `KDDC` file `KDDC20260506_134401_V06`
- `KOKX` volume `600`
- `KILX` volume `963` used for first-6-sweeps chunk timing

Sweep layout observed:
- `sweep_00` -> `0.4395°` -> `contiguous_surveillance`
- `sweep_01` -> `0.4834°` -> `contiguous_doppler`
- `sweep_02` -> `0.8350°` -> `contiguous_surveillance`
- `sweep_03` -> `0.8350°` -> `contiguous_doppler`
- `sweep_04` -> `1.2305°` -> `contiguous_surveillance`
- `sweep_05` -> `1.2305°` -> `contiguous_doppler`
- `sweep_06` -> `0.5273°` -> `contiguous_surveillance`
- `sweep_07` -> `0.5273°` -> `contiguous_doppler`
- `sweep_08` -> `1.7578°` -> `staggered_pulse_pair`
- `sweep_09` -> `2.3730°` -> `staggered_pulse_pair`
- `sweep_10` -> `3.0322°` -> `staggered_pulse_pair`
- `sweep_11` -> `3.9551°` -> `staggered_pulse_pair`
- `sweep_12` -> `5.0537°` -> `staggered_pulse_pair`
- `sweep_13` -> `6.3721°` -> `staggered_pulse_pair`

Lowest unique elevations observed:
- `0.4395°`
- `0.4834°`
- `0.5273°`
- `0.8350°`
- `1.2305°`
- `1.7578°`
- `2.3730°`

Chunk thresholds confirmed in session:
- lowest 3 sweep groups complete by chunk `019`
- first 6 sweep groups complete by chunk `037`

Scan-method metadata checks (`xradar` root attrs):
- `KDDC/468` (`VCP-215`): `dynamic_scan_type = SAILS x 1`
- `KDDC/100` (`VCP-215`): `dynamic_scan_type = SAILS x 1`
- `KILX/963` (`VCP-215`): `dynamic_scan_type = standard`

Interpretation:
- MESO-SAILS behavior is present in sampled `KDDC` VCP-215 volumes (single supplemental low-level cut).
- No MRLE indicator was observed in the sampled metadata.

## VCP-35

Sweep layout observed:
- `sweep_00` -> `0.4834°` -> `contiguous_surveillance`
- `sweep_01` -> `0.4834°` -> `contiguous_doppler`
- `sweep_02` -> `0.9229°` -> `contiguous_surveillance`
- `sweep_03` -> `0.9229°` -> `contiguous_doppler`
- `sweep_04` -> `1.3184°` -> `contiguous_surveillance`
- `sweep_05` -> `1.3184°` -> `contiguous_doppler`
- `sweep_06` -> `0.5273°` -> `contiguous_surveillance`
- `sweep_07` -> `0.5273°` -> `contiguous_doppler`
- `sweep_08` -> `1.8457°` -> `staggered_pulse_pair`
- `sweep_09` -> `2.4170°` -> `staggered_pulse_pair`
- `sweep_10` -> `3.0322°` -> `staggered_pulse_pair`
- `sweep_11` -> `3.9990°` -> `staggered_pulse_pair`
- `sweep_12` -> `5.0537°` -> `staggered_pulse_pair`
- `sweep_13` -> `6.4160°` -> `staggered_pulse_pair`

Lowest unique elevations observed:
- `0.4834°`
- `0.5273°`
- `0.9229°`
- `1.3184°`
- `1.8457°`
- `2.4170°`

## VCP-12

Observed from:
- `KGRK` file `KGRK20260506_134701_V06`

Sweep layout observed:
- `sweep_00` -> `0.5273°` -> `contiguous_surveillance`
- `sweep_01` -> `0.5273°` -> `contiguous_doppler`
- `sweep_02` -> `0.8789°` -> `contiguous_surveillance`
- `sweep_03` -> `0.8789°` -> `contiguous_doppler`
- `sweep_04` -> `1.2305°` -> `contiguous_surveillance`
- `sweep_05` -> `1.2305°` -> `contiguous_doppler`
- `sweep_06` -> `1.8018°` -> `staggered_pulse_pair`
- `sweep_07` -> `2.4170°` -> `staggered_pulse_pair`
- `sweep_08` -> `3.0762°` -> `staggered_pulse_pair`
- `sweep_09` -> `3.9990°` -> `staggered_pulse_pair`
- `sweep_10` -> `5.0977°` -> `staggered_pulse_pair`
- `sweep_11` -> `6.3721°` -> `staggered_pulse_pair`

Lowest unique elevations observed:
- `0.5273°`
- `0.8789°`
- `1.2305°`
- `1.8018°`
- `2.4170°`
- `3.0762°`

## Quick Operational Summary
- `VCP-212`: low and medium tilts include `0.5`, `0.8/0.9`, `1.3`, `1.8`, `2.4`
- `VCP-215`: low and medium tilts include about `0.4/0.5`, `0.8`, `1.2`, `1.8`, `2.4`
- `VCP-35`: low and medium tilts include about `0.5`, `0.9`, `1.3`, `1.8`, `2.4`
- `VCP-12`: low and medium tilts include about `0.5`, `0.9`, `1.2`, `1.8`, `2.4`

## Related Chunk Thresholds From Session
- `VCP-212`
  - lowest 3 sweep groups: chunk `019`
  - lowest 3 unique elevations at sampled KILX case: chunk `025`
  - first 6 sweep groups: chunk `037`
- `VCP-215`
  - lowest 3 sweep groups: chunk `019`
  - first 6 sweep groups: chunk `037`
- `VCP-35`
  - first 6 sweep groups: chunk `037`

# Operational Implementation
- Group `sweeps` into separate files:
  - `Low`: `sweep_00` to `sweep_03`
  - `High`: `sweep_04` to `sweep_09` (or up to 4.0 deg; deduplicate scan elevations within 0.1 deg)

## Chunk Download Thresholds for Operational Split

Using `EdgeWARN-dev` and live `unidata-nexrad-level2-chunks` samples, the following chunk thresholds were confirmed for the `Low`/`High` split above:

- `KILX/824` (`VCP-212`)
  - `Low` (`sweep_00..03`) complete at chunk `025`
  - `High` (`sweep_04..09`) complete at chunk `055`

- `KDDC/468` (`VCP-215`)
  - `Low` (`sweep_00..03`) complete at chunk `025`
  - `High` (`sweep_04..09`) complete at chunk `055`

- `KDDC/100` (`VCP-215`)
  - `Low` (`sweep_00..03`) complete at chunk `025`
  - `High` (`sweep_04..09`) complete at chunk `055`

Recommended operational thresholds:
- Download through chunk `025` to guarantee all `Low` sweeps.
- Download through chunk `055` to guarantee all `High` sweeps.
