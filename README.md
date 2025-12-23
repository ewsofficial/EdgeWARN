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

Check INSTALLATION.md for installation and run instructions

<h2 align="center">Build Info</h2>

## Version History for 1.0.x-alpha

### 1.0.0-alpha (current)
- Added a server API
- New JSON format, stormcells is a list of stormcells and their properties for a specific timestamp while cells is the history of a specific cell ID
- Add support to run in GitHub Workspaces

<h2 align="center">Credits</h2>

#### Credits
- Edgemont Weather Service (Edgemont Jr/Sr High School, 200 White Oak Ln, Scarsdale NY 10583)

#### Coders
- Yuchen Wei (Project Lead)
- Sammy Reifel

#### Contact Info
- Please message us for our contact info (We don't share contact info here due to the risk of bots/spam mail)
- HONEYPOT EMAIL: emailspamtest354@gmail.com (Do NOT email this)
