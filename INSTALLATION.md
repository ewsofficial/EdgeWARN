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

The unified API resolves the same platform default when no override is set.

`config/filesystem.yaml` is the sole authority for these defaults. Precedence
is CLI (`--base-dir` or `--base_dir`), then `EDGEWARN_BASE_DIR`, then legacy
`BASE_DIR`, then YAML. The legacy spellings remain supported for compatibility.

## Configuration tree

The application validates all 18 YAML documents and their schemas before
starting workers or an HTTP listener. Select a deployed tree with `--config-dir
/path/to/config` or `EDGEWARN_CONFIG_DIR=/path/to/config`; otherwise discovery
walks up from the installed source tree. Copy the whole `config/` directory,
including `config/schema/`, for deployment.

```bash
npm run validate-config
PYTHONPATH=src python -m common.config.validate
```

See `docs/core/configuration.md` for the authoritative owner of each setting.

Overrides:

- Python CLIs (`run.py`, `process_historical.py`): `--base_dir` or `--base-dir`
- Unified API: `--base-dir` or `EDGEWARN_BASE_DIR`
- `--base_dir` and `BASE_DIR` remain temporary compatibility aliases
- RAP maximum analysis age: `EDGEWARN_RAP_MAX_AGE_MINUTES` (non-negative
  integer minutes; default `180`)

The RAP setting controls both local-cache eligibility and the bounded backward
search of NOAA RAP analysis hours. Analysis timestamps, rather than download
times or filesystem modification times, determine freshness.

## Running API Services

Run from repository root:

```bash
npm run api
npm run debug:api
```

- Unified API default port: `5000`
- Unified API debug mode port: `3001`

Current API surfaces:

- v3: `/api/v3` and `/api/v3/openapi.json`
- Health: `/health/live`, `/health/ready`
- Legacy EdgeWARN and EWMRS paths remain compatibility adapters during migration

CLI and environment overrides:

- Base directory: `--base-dir <path>` or `EDGEWARN_BASE_DIR`
- Port override: `PORT`; debug mode: `--debug-server`
- Rate-limit env vars: `RATE_LIMIT_MAX_SEC`, `RATE_LIMIT_MAX_MIN`
- Browser and proxy policy: `ALLOWED_ORIGINS`, `TRUST_PROXY_IPS`

See `docs/api/unified_v3.md` for migration details and the complete contract.

## Running Real-Time Tandem Processing

Run from `src/`:

```bash
python run.py --lat_limits 20 55 --lon_limits 230 300
```

Common optional flags:

- `--lat_limits <LAT_MIN> <LAT_MAX>` default `20 55`
- `--lon_limits <LON_MIN> <LON_MAX>` default `230 300`
- `--base_dir` / `--base-dir`
- `--config-dir`
- `--profile`
- `--disable-ctam`
- `--ctam-module-dir`
- `--list-ctam-modules`
- `--check-ctam-modules`
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
- `--base_dir` / `--base-dir`
- `--config-dir`
- `--profile`
- `--disable-ctam`
- `--ctam-module-dir`
- `--list-ctam-modules`
- `--check-ctam-modules`
- `--disable-tracking`
- `--disable-polygon-expansion`
- `--refl-threshold`
- `--min-seed-percentage`
- `--drop-offset`

Historical-processing note:

- `process_historical.py` writes its stormcell products to `<BASE_DIR>/data/stormcells/stormcells_{timestamp}.json` through the normal detection and integration pipeline.

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
- `--pause-seconds <float>` default `0.05`
- `--progress` / `--no-progress` progress output, default on
- `--apply` write updates; without it the command is a dry run
- `--report-path <path>` write the sync report JSON to a file
- `--config-dir <path>` select the `config/` tree to read defaults from

The listed defaults are owned by `config/nws.yaml` under `zone_sync`, not by the
parser, so a deployed tree can change them. `--pause-seconds` is scaled by
`--max-workers` to hold a whole-job rate: `0.05` is roughly 20 requests/second
regardless of thread count. `--apply` and `--report-path` have no YAML keys.

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
