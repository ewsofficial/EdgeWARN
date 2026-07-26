# EdgeWARN Core Installation and Runtime

## Requirements

- Conda or Miniconda
- npm
- git-scm

## Setup

1. Clone the repository:

```bash
git clone https://www.github.com/ewsofficial/EdgeWARN-Core
cd EdgeWARN-Core
```

2. Create and activate the Python environment:

```bash
conda env create -f environment.yml
conda activate EdgeWARN-dev
```

3. Install Node.js dependencies:

```bash
npm install
```

## Runtime Base Directory

Most generated data is written outside the repository into a base directory.

Defaults:

- Linux/macOS: `~/EdgeWARN_input`
- Windows: `C:\EdgeWARN_input`

Notes:

- Python CLIs and the EWMRS API use the platform defaults above when no override is supplied.
- The EdgeWARN API has a broader Linux fallback chain: `~/EdgeWARN_input`, then `/home/EdgeWARN_input`, then `/workspaces/EdgeWARN_input`, then `./EdgeWARN_input`.

Overrides:

- Python CLIs (`run.py`, `process_historical.py`): `--base_dir` or `--base-dir`
- EdgeWARN API (`src/EdgeWARN/api/server.js`): `--base-dir` or `EDGEWARN_BASE_DIR`
- EWMRS API (`src/EWMRS/api/server.js`): `--base_dir` or `BASE_DIR`
- RAP maximum analysis age: `EDGEWARN_RAP_MAX_AGE_MINUTES` (non-negative
  integer minutes; default `180`)

The RAP setting controls both local-cache eligibility and the bounded backward
search of NOAA RAP analysis hours. Analysis timestamps, rather than download
times or filesystem modification times, determine freshness.

## Running API Services

Run from repository root:

```bash
npm run api:edgewarn
npm run debug:edgewarn
npm run api:ewmrs
npm run debug:ewmrs
```

- EdgeWARN API default port: `5000`
- EdgeWARN debug mode port: `3001`
- EWMRS API default port: `3003`
- EWMRS debug mode port: `3004`

Current API surfaces:

- EdgeWARN: `/health`, `/api/v2/features/cells`, `/api/v2/features/timestamps`, `/api/v2/features/alerts/*`, `/api/v2/data/metar`
- EWMRS: `/renders/*`, `/nexrad/*`, `/rap/*`, `/wpc/*`, `/colormaps`, `/healthz`

CLI and environment overrides:

- EdgeWARN API base directory: `--base-dir <path>` or `EDGEWARN_BASE_DIR`
- EdgeWARN API port override: `PORT`
- EdgeWARN API debug mode: `--debug_server`
- EdgeWARN API rate limits: `--edgewarn-rate-limit-1s <count>`, `--edgewarn-rate-limit-1m <count>`
- EdgeWARN API rate-limit env vars: `RATE_LIMIT_WINDOW_MS_SEC`, `RATE_LIMIT_MAX_SEC`, `RATE_LIMIT_WINDOW_MS_MIN`, `RATE_LIMIT_MAX_MIN`
- EWMRS API base directory: `--base_dir <path>` or `BASE_DIR`
- EWMRS API port override: `PORT`
- EWMRS API debug mode: `--debug-server` or `--debug_server`
- EWMRS API rate limits: `--ewmrs-rate-limit-1s <count>`, `--ewmrs-rate-limit-1m <count>`
- EWMRS API rate-limit env vars: `EWMRS_RATE_LIMIT_MAX_SEC`, `EWMRS_RATE_LIMIT_MAX_MIN`
- For both APIs, a rate-limit value of `0` disables that limiter window

## Running Real-Time Tandem Processing

Run from `src/`:

```bash
python run.py --lat_limits 20 55 --lon_limits 230 300
```

Common optional flags:

- `--lat_limits <LAT_MIN> <LAT_MAX>` default `20 55`
- `--lon_limits <LON_MIN> <LON_MAX>` default `230 300`
- `--base_dir` / `--base-dir`
- `--profile`
- `--disable-ctam`
- `--disable-tracking`
- `--disable-polygon-expansion`
- `--disable-ewmrs`
- `--disable-nws`
- `--disable-metar`
- `--disable-goes`
- `--disable-nexrad`
- `--mrms-core-only`
- `--refl-threshold`
- `--min-seed-percentage`
- `--drop-offset`

Notes:

- `run.py` normalizes `--lon_limits` into the `0-360` domain internally
- `--disable-goes` disables GOES ingest, GLM ingest, and GOES rendering
- `--disable-nexrad` disables NEXRAD Level II ingestion entirely
- `--mrms-core-only` runs MRMS ingestion for EWMRS rendering without waiting on EdgeWARN detection outputs
- `--disable-ewmrs` skips EWMRS workers and rendering while leaving EdgeWARN realtime processing enabled

## Running Historical Reprocessing

Run from `src/`:

```bash
python process_historical.py --start 2024-01-01T00:00:00 --end 2024-01-01T01:00:00 --lat 20 55 --lon -130 -60
```

Common optional flags:

- `--start <ISO8601>` required
- `--end <ISO8601>` required
- `--lat <LAT_MIN> <LAT_MAX>` default `20 55`
- `--lon <LON_MIN> <LON_MAX>` default `-130 -60`
- `--output <path>` compatibility argument; currently does not redirect the final runtime stormcell output path
- `--base_dir` / `--base-dir`
- `--profile`
- `--disable-ctam`
- `--disable-tracking`
- `--disable-polygon-expansion`
- `--refl-threshold`
- `--min-seed-percentage`
- `--drop-offset`

Historical-processing note:

- `process_historical.py` still writes its actual stormcell products to `<BASE_DIR>/data/stormcells/stormcells_{timestamp}.json` through the normal detection and integration pipeline. `--output` is parsed and later checked for existence logging, but is not currently used to relocate the final persisted runtime artifact.

## Maintaining NWS Zone Assets

`src/common/ingest/nws/zone_sync.py` refreshes `assets/nws_zones` from the NWS zone and UGC APIs.

Run from repository root:

```bash
python src/common/ingest/nws/zone_sync.py --apply
```

Flags:

- `--assets-dir <path>` custom `assets/nws_zones` location
- `--zone-types <types...>` defaults to `forecast fire public county marine`
- `--timeout-seconds <int>` default `30`
- `--max-retries <int>` default `3`
- `--max-workers <int>` default `16`
- `--pause-seconds <float>` default `0.0`
- `--no-progress` disable progress output
- `--apply` write updates; without it the command is a dry run
- `--report-path <path>` write the sync report JSON to a file

## Tests

Node.js tests:

```bash
npm test
npm run test:watch
npm run test:coverage
```

Python tests (with `EdgeWARN-dev` active):

```bash
python -m pytest tests/
```
