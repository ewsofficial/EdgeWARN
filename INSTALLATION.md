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

Overrides:

- Python CLIs (`run.py`, `process_historical.py`): `--base_dir` or `--base-dir`
- EdgeWARN API (`src/EdgeWARN/api/server.js`): `--base-dir` or `EDGEWARN_BASE_DIR`
- EWMRS API (`src/EWMRS/api/server.js`): `--base_dir` or `BASE_DIR`

## Running API Services

Run from repository root:

```bash
npm run api:edgewarn
npm run debug:edgewarn
npm run api:ewmrs
```

- EdgeWARN API default port: `5000`
- EdgeWARN debug mode port: `3001`
- EWMRS API default port: `3003`

## Running Real-Time Tandem Processing

Run from `src/`:

```bash
python run.py --lat_limits 20 55 --lon_limits 230 300
```

Common optional flags:

- `--base_dir` / `--base-dir`
- `--profile`
- `--disable-ctam`
- `--disable-tracking`
- `--refl-threshold`
- `--min-seed-percentage`
- `--drop-offset`

## Running Historical Reprocessing

Run from `src/`:

```bash
python process_historical.py --start 2024-01-01T00:00:00 --end 2024-01-01T01:00:00 --lat 20 55 --lon -130 -60
```

Common optional flags:

- `--output`
- `--base_dir` / `--base-dir`
- `--profile`
- `--disable-ctam`
- `--disable-tracking`
- `--refl-threshold`
- `--min-seed-percentage`
- `--drop-offset`

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
