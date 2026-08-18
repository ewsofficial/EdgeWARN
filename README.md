# EdgeWARN Core

EdgeWARN Core is the mixed Python and Node.js backend for the EdgeWARN analysis pipeline and the EWMRS rendering service.

It ingests operational weather datasets, processes storm-cell products, renders GUI layers, and serves generated artifacts through REST APIs.

## What This Repository Provides

- Shared staged ingest orchestration for EdgeWARN + EWMRS tandem processing
- EdgeWARN storm-cell detection, optional tracking/lineage, integration, CTAM analytics, and alert generation
- EWMRS raster rendering, tiling, WPC surface-analysis serving, and colormap delivery
- Historical reprocessing via `src/process_historical.py`
- One versioned file-backed API at `/api/v3`, with legacy EdgeWARN and EWMRS paths retained as temporary compatibility adapters

## Requirements

- Conda or Miniconda
- Node.js/npm
- git

## Installation

1. Clone the repository:

```bash
git clone https://www.github.com/ewsofficial/EdgeWARN-Core
cd EdgeWARN-Core
```

2. Create and activate the Conda environment:

```bash
conda env create -f environment.yml
conda activate EdgeWARN-dev
```

3. Install Node dependencies:

```bash
npm install
```

Detailed setup and runtime notes are in `INSTALLATION.md`.

## Running Services

From repository root:

```bash
npm run api
npm run debug:api
```

## Running Python Pipelines

From `src/`:

```bash
python run.py --lat_limits 20 55 --lon_limits 230 300
python process_historical.py --start 2024-01-01T00:00:00 --end 2024-01-01T01:00:00 --lat 20 55 --lon -130 -60
```

Key realtime flags include `--disable-ewmrs`, `--disable-nws`, `--disable-metar`, `--disable-goes`, `--disable-nexrad`, `--disable-ctam`, `--disable-tracking`, `--disable-polygon-expansion`, `--mrms-core-only`, `--refl-threshold`, `--min-seed-percentage`, and `--drop-offset`.

Historical processing supports `--base_dir` / `--base-dir`, `--profile`, `--disable-ctam`, `--disable-tracking`, `--disable-polygon-expansion`, `--refl-threshold`, `--min-seed-percentage`, and `--drop-offset`.

`run.py` normalizes `--lon_limits` into the `0-360` domain internally.

Historical runs persist the final stormcell artifacts to `<BASE_DIR>/data/stormcells/` using the runtime timestamped filenames.

## Runtime Base Directory

Runtime output defaults to:

- Linux/macOS: `~/EdgeWARN_input`
- Windows: `C:\EdgeWARN_input`

The unified API uses the same platform default when no override is supplied.

`config/filesystem.yaml` owns these platform defaults. Resolution is CLI,
`EDGEWARN_BASE_DIR`, legacy `BASE_DIR`, then YAML. Use `--config-dir` or
`EDGEWARN_CONFIG_DIR` to select a complete alternate 19-file `config/` tree;
run `npm run validate-config` before deployment. See
`docs/core/configuration.md` for catalog ownership.

Supported overrides:

- Python CLI: `--base_dir` / `--base-dir`
- Unified API: `--base-dir` or `EDGEWARN_BASE_DIR`
- Temporary aliases: `--base_dir` and `BASE_DIR`
- RAP maximum analysis age: `EDGEWARN_RAP_MAX_AGE_MINUTES` (default `180`)

RAP ingest checks the configured runtime cache first, then searches NOAA S3
newest-to-oldest within this analysis-age limit. Freshness is based on the RAP
analysis timestamp, not the local file modification time.

The unified API honors `PORT`, `--debug-server`, `RATE_LIMIT_MAX_SEC`, and `RATE_LIMIT_MAX_MIN`. Use `ALLOWED_ORIGINS` and `TRUST_PROXY_IPS` to configure browser and proxy trust.

See `INSTALLATION.md` for the full CLI reference, including API debug and rate-limit flags plus the `zone_sync.py` maintenance utility.

## Testing

Node:

```bash
npm test
npm run test:watch
npm run test:coverage
```

Python (with `EdgeWARN-dev` active):

```bash
python -m pytest tests/
```

## Release

Current package version: **2.7.0**

See `CHANGELOG.md` for release history.
