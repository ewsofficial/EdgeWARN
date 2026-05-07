# Findings: NEXRAD Chunking and Sweep Availability

Generated: 2026-05-04T22:42:01-04:00

## Data Sources Used
- AWS bucket: `s3://unidata-nexrad-level2-chunks`
- AWS archive bucket: `s3://unidata-nexrad-level2`
- Parsing library: `xradar`
- Documentation checked:
  - AWS Open Data `noaa-nexrad` registry entry
  - `awslabs/open-data-docs` NEXRAD README

## Confirmed Bucket Structure
Real-time chunk keys follow:
`<SITE>/<VOLUME_ID>/<YYYYMMDD-HHMMSS-CHUNKNUM-CHUNKTYPE>`

Where:
- `CHUNKTYPE`: `S` (start), `I` (intermediate), `E` (end)
- `CHUNKNUM`: sequential within volume

## Documentation-Based Conclusions
- Chunk number is sequential only; docs do **not** define a fixed universal mapping between chunk number and sweep completion.
- Therefore, "lowest 3 sweeps always at chunk 019" is **not** an official standard.

## Practical Findings From Live Data

### KOKX (VCP-215)
- For tested recent volumes, lowest 3 sweeps by index (`sweep_0..2`) became complete at chunk `019`.

### KILX latest tested volumes (VCP-212)
- Latest tested volume IDs included `748`, `819`, and `824` in the session.
- For lowest 3 sweeps by index (`sweep_0..2`): completion at chunk `019`.
- For lowest 3 **unique elevation angles**: completion at chunk `025`.

#### KILX volume `824` details
- `scan_name`: `VCP-212`
- `dynamic_scan_type`: `standard`
- First-to-last chunk upload latency: `263 s`
- First-to-chunk-025 upload latency: `79 s`

## Why sweep_0 and sweep_1 can share low elevation
In VCP-212, low tilts are split by waveform/product type:
- `sweep_0`: low elevation, `contiguous_surveillance`
- `sweep_1`: similar low elevation, `contiguous_doppler`

So they are different sweep groups despite near-identical elevation angles.

## Waveform Types Observed and Variables (KILX VCP-212)

### `contiguous_surveillance`
Variables:
- `DBZH`, `ZDR`, `PHIDP`, `RHOHV`, `CCORH`

### `contiguous_doppler`
Variables:
- `DBZH`, `VRADH`, `WRADH`

### `staggered_pulse_pair`
Variables:
- `DBZH`, `VRADH`, `WRADH`

### `batch`
Variables:
- `DBZH`, `VRADH`, `WRADH`

Common sweep metadata variables also appear (e.g., `sweep_mode`, `sweep_number`, `prt_mode`, `follow_mode`, `sweep_fixed_angle`).

## Full Sweep Inventory Example (KILX volume 824)
- `sweep_00` elev `0.5273`, waveform `contiguous_surveillance`, rays `720`
- `sweep_01` elev `0.5273`, waveform `contiguous_doppler`, rays `720`
- `sweep_02` elev `0.8350`, waveform `contiguous_surveillance`, rays `720`
- `sweep_03` elev `0.8789`, waveform `contiguous_doppler`, rays `720`
- `sweep_04` elev `1.3184`, waveform `contiguous_surveillance`, rays `720`
- `sweep_05` elev `1.3184`, waveform `contiguous_doppler`, rays `720`
- `sweep_06` elev `1.8018`, waveform `staggered_pulse_pair`, rays `360`
- `sweep_07` elev `2.4170`, waveform `staggered_pulse_pair`, rays `360`
- `sweep_08` elev `3.1201`, waveform `staggered_pulse_pair`, rays `360`
- `sweep_09` elev `3.9990`, waveform `staggered_pulse_pair`, rays `357`
- `sweep_10` elev `5.0977`, waveform `staggered_pulse_pair`, rays `360`
- `sweep_11` elev `6.4160`, waveform `staggered_pulse_pair`, rays `360`
- `sweep_12` elev `7.9541`, waveform `batch`, rays `360`
- `sweep_13` elev `9.9756`, waveform `batch`, rays `360`
- `sweep_14` elev `12.5244`, waveform `batch`, rays `360`

(`xradar` reported one incomplete terminal sweep dropped in this volume.)

## Real-Time Pipeline Recommendation
- Only download radars that are in VCPs 12, 212, and 215
- Stream chunks per `(site, volume_id)`.
- Reassemble ordered byte stream.
- Incrementally parse and check sweep completeness.
- Trigger early product when required sweeps are complete.
- Optionally continue to `E` chunk for full-volume final product.
- Download the first **6** sweeps **ONLY!!!** to optimize latency
- Use chunk `019` only as an optimization hint, not as a hard rule.

## First-6-Sweeps Stop Chunks by VCP
Use these tested stop chunks when the target is the first 6 sweep groups (`sweep_0..5`):

- `VCP-212` -> stop at chunk `037``
  - Latency `001` to `037`: approx. `137 s`

- `VCP-215` -> stop at chunk `037`
  - Latency `001` to `037`: approx. `152 s`

- `VCP-35` -> stop at chunk `037`
  - Latency `001` to `037`: approx. `270 s`

Note: These are empirical results from tested volumes and should still be validated by sweep-completeness checks in production.
