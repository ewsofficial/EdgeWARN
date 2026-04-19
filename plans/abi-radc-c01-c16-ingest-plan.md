# Plan: Ingest GOES-19 ABI-L1b-RadC Channels (C01-C16)

## Objective
Add reliable ingestion for all ABI-L1b-RadC channel files (C01-C16) from `noaa-goes19` while reusing existing ingest code paths (async-first + sync fallback, staged coordinator, cleanup, timestamp matching) as much as possible.

## Confirmed Source Pattern
- Bucket: `noaa-goes19`
- Prefix pattern: `ABI-L1b-RadC/{YYYY}/{DDD}/{HH}/`
- Filename pattern includes channel token: `...-M6C01_...` through `...-M6C16_...`
- One scan time should yield a 16-file channel set.

## Reuse-First Strategy
Keep these existing structures unchanged where possible:
- `parse_goes_bucket_path()` for GOES path formatting.
- `download_all_goes_files()` / `download_all_goes_files_async()` orchestration.
- `FileFinder` / `AsyncFileFinder` + existing S3 clients and fallback behavior.
- Existing cleanup (`clean_old_files` / async cleanup), logging, and performance timers.
- Shared staged ingest flow in `src/common/pipeline/coordinator.py`.

## Severe Nowcasting Selection
For severe thunderstorm nowcasting, use this reduced ABI RadC set as the default ingest target:

- `C02` - Visible (red)
- `C05` - Snow/Ice (NIR)
- `C06` - Particle Size (NIR)
- `C07` - Shortwave IR
- `C08` - Upper-level WV
- `C09` - Mid-level WV
- `C10` - Lower-level WV
- `C13` - Clean (LWIR)
- `C15` - Dirty (LWIR)
- `C16` - CO2 (LWIR)

Channels intentionally excluded from the default severe-nowcasting ingest set:
- `C01` - Visible (blue)
- `C03` - Veggie (NIR)
- `C04` - Cirrus (NIR)
- `C11` - CLD Top Phase
- `C12` - Ozone
- `C14` - Long-wave IR

Rationale:
- Keeps the visible, microphysics, water-vapor, and thermal-difference bands most useful for convective nowcasting.
- Avoids pulling channels that are more specialized, more presentation-oriented, or largely overlapping for this workflow.

## Implementation Plan

### 1) Extend filesystem destinations (minimal, explicit)
Update `src/util/file.py` with ABI RadC output directories:
- Add one base directory for ABI RadC ingestion (optional): `GOES_ABI_RADC_DIR`.
- Add per-channel directories named by channel meaning, not raw band id.

Recommended directory naming:
- `GOES_ABI_VISIBLE_BLUE_DIR`
- `GOES_ABI_VISIBLE_RED_DIR`
- `GOES_ABI_VEGGIE_DIR`
- `GOES_ABI_CIRRUS_DIR`
- `GOES_ABI_SNOW_ICE_DIR`
- `GOES_ABI_PARTICLE_SIZE_DIR`
- `GOES_ABI_SHORTWAVE_IR_DIR`
- `GOES_ABI_UPPER_LEVEL_WV_DIR`
- `GOES_ABI_MID_LEVEL_WV_DIR`
- `GOES_ABI_LOWER_LEVEL_WV_DIR`
- `GOES_ABI_CLD_TOP_PHASE_DIR`
- `GOES_ABI_OZONE_DIR`
- `GOES_ABI_CLEAN_LWIR_DIR`
- `GOES_ABI_LONGWAVE_IR_DIR`
- `GOES_ABI_DIRTY_LWIR_DIR`
- `GOES_ABI_CO2_LWIR_DIR`

Rationale:
- Keeps file management and downstream consumption simple (`latest_files()` per channel).
- Reuses current path initialization pattern already used by MRMS/GLM.
- Makes downstream code and runtime directories easier to understand than `C01` ... `C16`.

### 2) Extend GOES ingest config without breaking existing GLM behavior
Update `src/common/ingest/mrms/config.py` to describe ABI channel ingest targets in addition to GLM.

Preferred low-risk approach:
- Keep `get_goes_modifiers()` as the source of truth.
- Add channel entries that include:
  - product: `ABI-L1b-RadC`
  - channel id (`C01`..`C16`)
  - channel name (`visible_blue`, `visible_red`, etc.)
  - output dir (matching the semantic per-channel directory)
  - filename matcher (regex or token) for `_M\dCxx_`.

If tuple shape becomes too limiting:
- Introduce a small new spec structure (dataclass/dict), while supporting legacy 2-tuple entries so GLM continues to work unchanged.

Configuration decision:
- Make the severe-nowcasting set above the default ABI RadC ingest list.
- Optional future enhancement: support a broader `full` profile if all 16 channels are later needed.

### 3) Add channel-aware filtering in GOES downloader (shared sync + async)
Update `src/common/ingest/mrms/downloader.py` to optionally filter listed S3 keys before download:
- Reuse current listing by product prefix (`ABI-L1b-RadC/YYYY/DDD/HH/`).
- Apply channel matcher (`_M\dC01_` etc.) per ingest spec.
- Reuse current target selection behavior for timestamp matching.
- Keep GLM merge logic only for GLM products.

Important behavior requirement:
- For each configured channel, ingest exactly one best-match file per cycle (or explicit warning if missing), instead of downloading mixed-channel files into one directory.
- Downloaded filenames can remain NOAA-native; only the local directory names need to use semantic channel names.
- If the target file already exists on disk, stop immediately and skip the download. Reuse the current existing-file short-circuit in the sync/async downloaders rather than adding duplicate logic.

Retention requirement:
- Keep only the latest 2 files in each ABI/GOES output directory.
- Reuse `util.file.clean_old_files(..., max_files=2)` / `async_clean_old_files(..., max_files=2)` rather than introducing a new pruning path.
- Apply this retention during the existing pre-download cleanup stage so no extra cleanup pass is needed.

### 4) Preserve staged readiness and cleanup behavior
No coordinator redesign is required:
- `run_tandem_ingest_cycle()` already treats GOES ingest as one staged dependency.
- `get_output_dirs(..., goes_modifiers=...)` will pick up new GOES output dirs automatically once config is expanded.
- Narrow GOES cleanup retention from the current broader default to `max_files=2` for the new ABI channel directories and existing GOES product directories touched by this ingest flow.

### 5) Tests and validation
Add/adjust tests under `tests/core/ingest/mrms/`:
- Config coverage: confirm ABI RadC includes all `C01..C16` channels.
- Downloader selection/filter tests: channel token filtering + timestamp pick.
- Keep existing `parse_goes_bucket_path` tests unchanged (path format is already correct).

Add a lightweight integration check (mocked) if needed:
- Ensure `download_all_goes_files_async()` iterates GLM + ABI channel specs without regression.

## Rollout Validation
1. Run targeted tests for ingest modules.
2. Run one live ingest cycle against a known hour (for example `2026/109/00`).
3. Verify each selected channel directory (`C02`, `C05`, `C06`, `C07`, `C08`, `C09`, `C10`, `C13`, `C15`, `C16`) has a fresh file for the cycle timestamp.
   Expected directories should use semantic names like `VisibleRed`, `ParticleSize`, `CleanLWIR`, `CO2LWIR`, not raw `C02`, `C06`, `C16` labels.
4. Re-run the same ingest timestamp and confirm the downloader skips files already present on disk.
5. Confirm each ABI/GOES output directory retains only the newest 2 files after cleanup.
6. Verify GLM ingest still works and no staged-ingest regressions occur.

## Out of Scope (This Plan)
- Using ABI RadC channels in integration/CTAM/render logic.
- New derived RGB/composite generation.
- API exposure of ABI channel artifacts.

## Expected Outcome
- EdgeWARN-Core ingests the selected severe-nowcasting ABI-L1b-RadC channels using existing ingest architecture.
- GLM ingest remains backward compatible.
- New code surface is limited to config + optional downloader filtering/spec plumbing only.
