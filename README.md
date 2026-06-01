# EdgeWARN Core

EdgeWARN Core is the mixed Python and Node.js backend for the EdgeWARN analysis pipeline and the EWMRS rendering service.

It ingests operational weather datasets, processes storm-cell products, renders GUI layers, and serves generated artifacts through REST APIs.

## What This Repository Provides

- Shared staged ingest orchestration for EdgeWARN + EWMRS tandem processing
- EdgeWARN storm-cell detection, optional tracking/lineage, integration, CTAM analytics, and alert generation
- EWMRS raster rendering, tiling, WPC surface-analysis serving, and colormap delivery
- Historical reprocessing via `src/process_historical.py`
- File-backed APIs for EdgeWARN (`/api/v2`) and EWMRS (`/renders`, `/wpc`, `/colormaps`)

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
npm run api:edgewarn
npm run debug:edgewarn
npm run api:ewmrs
npm run debug:ewmrs
```

## Running Python Pipelines

From `src/`:

```bash
python run.py --lat_limits 20 55 --lon_limits 230 300
python process_historical.py --start 2024-01-01T00:00:00 --end 2024-01-01T01:00:00 --lat 20 55 --lon -130 -60
```

Key realtime flags include `--disable-ewmrs`, `--disable-nws`, `--disable-metar`, `--disable-goes`, `--disable-ctam`, `--disable-tracking`, `--refl-threshold`, `--min-seed-percentage`, and `--drop-offset`.

Historical processing supports `--output`, `--base_dir` / `--base-dir`, `--profile`, `--disable-ctam`, `--disable-tracking`, `--refl-threshold`, `--min-seed-percentage`, and `--drop-offset`.

## Runtime Base Directory

Runtime output defaults to:

- Linux/macOS: `~/EdgeWARN_input`
- Windows: `C:\EdgeWARN_input`

Supported overrides:

- Python CLI: `--base_dir` / `--base-dir`
- EdgeWARN API: `--base-dir` or `EDGEWARN_BASE_DIR`
- EWMRS API: `--base_dir` or `BASE_DIR`

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

Current package version: **2.6.1**

See `CHANGELOG.md` for release history.
