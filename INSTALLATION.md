# EdgeWARN Installation Instructions

## Requirements
- ``Conda or Miniconda``
- ``npm``
- ``git-scm``

## Installation Instructions
1. Clone the repository into the local machine by running ``git clone https://www.github.com/ewsofficial/EdgeWARN-Core``
2. Navigate to the folder where you downloaded ``EdgeWARN-Core``
3. Install Node.js dependencies by running ``npm install``
4. Install the EdgeWARN environment by running ``conda env create -f environment.yml``
5. Activate the EdgeWARN environment by running ``conda activate EdgeWARN-dev``

## Running EdgeWARN

**NOTE**: If data ingestion cannot reach URLs, try removing ``aiodns`` from your environment

Run the API commands from the repository root. Run the Python analysis commands from the ``src`` folder.

### Real-Time Analysis (``run.py``)

The real-time analysis (``run.py``) pulls live data and analyses it in real-time

#### Args
- **``--lat_limits``** (``lat_min``, ``lat_max``): Latitude limits for data processing
- **``--lon_limits``** (``lon_min``, ``lon_max``): Longitude limits for data processing
- **``--base_dir``** / **``--base-dir``**: Base directory to store data (Default is ``~/EdgeWARN_input`` on Unix-like systems)
- **``--profile``**: Enable performance profiling

### Historical Analysis (``process_historical.py``)

Historical analysis capabilities all the way up to January 1, 2021 using the ``noaa-mrms-pds`` AWS S3 Bucket

#### Args
- **``--start``**: Start time in ISO 8601 (``YYYY-MM-DDTHH:MM:SS``)
- **``--end``**: End time in ISO 8601 (``YYYY-MM-DDTHH:MM:SS``)
- **``--lat``** (``lat_min``, ``lat_max``): Latitude limits for data processing
- **``--lon``** (``lon_min``, ``lon_max``): Longitude limits for data processing
- **``--base_dir``** / **``--base-dir``**: Base directory to store data
- **``--output``**: Output JSON file path (Default: ``stormcell_test.json``)
- **``--profile``**: Enable performance profiling

### Start Data Server
- ``npm run api:edgewarn``

### Start Debug Server
- ``npm run debug:edgewarn``
