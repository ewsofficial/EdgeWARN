<h1 align="center">EdgeWARN</h1>

<p align="center">
<img src="assets/EdgeWARN.png" alt="EWS-logo" width="15%"/>
</p>

<h2 align="center">Severe Weather Nowcasting</h2>

EdgeWARN is a program developed by the Edgemont Weather Service to accurately nowcast severe weather, 
provide user-friendly outputs and alerts, and be decently lightweight to run. 
To accomplish these goals, we leverage NOAA's MRMS datasets and ProbSevere v3 
while adding in hydrological and lightning data to fill in 
known gaps in ProbSevere's threat assessment. This repository serves as 
EdgeWARN's core server that processes raw data and serves it to
the GUI frontend.

<h2 align="center">Installation Instructions</h2>

#### This is the EdgeWARN Core Server! GUIs will be developed separately for web and desktop applications

#### Requirements
1. Conda/Miniconda with Python 3.13+

#### Installation Instructions
1. Clone the repository
2. Run `conda env create -f environment.yml` at the repository's root
3. Navigate to the src directory
4. Run `python run.py --lat_limits lat_min lat_max --lon_limits lon_min lon_max`

- lat_min, lat_max = latitude bounds (defaults are ``(36, 46)``)
- lon_min, lon_max = longitude bounds (defaults are ``(-83, -73)``)

<h2 align="center">Build Info</h2>

## Version History for 0.6.x-alpha

### 0.6.0-alpha
- Changed `ingest` module to use AWS S3 Buckets
- Added `GOES-19` data downloading
- Added metadata tags `source`, `product`, `version`, `latest_timestamp` to storm cell JSON
- Storm cell data is now moved into `features` key
- Dataset integration now only integrates for cells that have a latest history timestamp

### 0.6.1-alpha
- Cache file lists to reduce lookup overhead
- Reduce integration overhead by subsetting grids per cell and skipping irrelevant timestamps

# 0.6.2-alpha (current)
- Add ``INFO: `` prints to command-line output
- Condense ``find_timestamp`` and dataset loading into singular file

<h2 align="center">Credits</h2>

#### Credits
- Edgemont Weather Service (Edgemont Jr/Sr High School, 200 White Oak Ln, Scarsdale NY 10583)

#### Coders
- Yuchen Wei (Project Lead)
- Sammy Reifel

#### Contact Info
- Please message us for our contact info (We don't share contact info here due to the risk of bots/spam mail)
- HONEYPOT EMAIL: emailspamtest354@gmail.com (Do NOT email this)
