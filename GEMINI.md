# EdgeWARN-Core

## Project Overview

EdgeWARN-Core is the backend server for the EdgeWARN severe weather nowcasting system. It processes meteorological data from various sources (NOAA MRMS, ProbSevere v3, RAP, GOES-19 GLM) to provide real-time and historical weather analysis.

The system consists of two main components:
1.  **Python Core:** Handles data ingestion, processing, and analysis (real-time and historical).
2.  **Node.js API:** A RESTful API server (Express.js) that serves the processed data to frontends.

### Key Technologies
*   **Python:** 3.13+ (Managed via Conda)
*   **Data Processing:** `numpy`, `xarray`, `scikit-image`, `scipy`, `shapely`, `rasterio`, `netcdf4`
*   **Cloud/Network:** `boto3`, `aioboto3`
*   **Node.js:** Express.js, CORS, Helmet, Compression

## Building and Running

### Prerequisites
*   Conda or Miniconda
*   npm (Node.js)
*   git

### Installation
1.  Clone the repository:
    ```bash
    git clone https://www.github.com/ewsofficial/EdgeWARN-Core
    cd EdgeWARN-Core
    ```
2.  Create and activate the Conda environment:
    ```bash
    conda env create -f environment.yml
    conda activate EdgeWARN-dev
    ```
3.  Install Node.js dependencies:
    ```bash
    npm install
    ```

### Running the Application

#### 1. Real-Time Analysis
Run the core python script to pull and analyze live data:
```bash
python src/run.py [options]
```
**Options:**
*   `--lat_limits <min> <max>`: Latitude limits
*   `--lon_limits <min> <max>`: Longitude limits
*   `--nogui`: Disable server monitor GUI
*   `--base_dir <path>`: Output directory (Default: `EdgeWARN_input`)

#### 2. Historical Analysis
Process historical data (available back to Jan 1, 2021):
```bash
python src/process_historical.py --start <ISO8601> --end <ISO8601> [options]
```

#### 3. API Server
Start the Node.js backend server:
```bash
npm start
```
*   **Default Port:** 5000
*   **Debug Port:** 3001 (if using `--debug_server`)
*   **Health Check:** `http://localhost:5000/health`

## Development Conventions

### Branching Strategy
*   **Features:** `<yourname>/feat/<feature-name>`
*   **Bug Fixes:** `<yourname>/fix/<bug-name>`

### Commit Messages
Use the following prefixes:
*   `ADD`: New files/modules
*   `FTR`: New features
*   `IMP`: Improvements
*   `FIX`: Bug fixes
*   `HOT`: Hotfixes
*   `REF`: Refactoring
*   `CLN`: Cleanup
*   `OPT`: Optimization
*   `DOC`: Documentation
*   `STL`: Style changes
*   `TST`: Testing
*   `CIC`: CI/CD
*   `BLD`: Build/Tooling

### Testing
*   **Python:** Run tests using `pytest`. Configuration is in `tests/conftest.py`.
    ```bash
    pytest
    ```
*   **Node.js:** Currently, no test script is defined in `package.json`.

### Code Style
*   Follow existing code styles.
*   Ensure no sensitive data (API keys, passwords) is committed.
