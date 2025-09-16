<h1 align="center">EdgeWARN</h1>

<p align="center">
<img src="assets/EWS_logo_072025.png" alt="EWS-logo" width="15%"/>
</p>

<h2 align="center">An Edgemont Weather Service Project</h2>

EdgeWARN is a program developed by the Edgemont Weather Service to accurately nowcast severe weather, provide a user friendly GUI, and the capability to be run on any modern PC. As a high school organization, our goals are to provide accurate and timely weather forecasts and to raise weather awareness across the community. EdgeWARN utilizes storm cell detection and tracking, threat analysis, and an interactive GUI (Under development!) to deliver an accurate but easily readable interface to the public.

<h2 align="center">Installation Instructions</h2>
EdgeWARN is NOT ready for deployment. Use code at own risk.

1. Clone the repository
2. Run `pip install -r requirements.txt` to install dependencies
3. Navigate to EdgeWARN/src to run scripts
4. Run scripts as `python -B -m path.to.script`

<h2 align="center">Current Build Info</h2>

#### Build Version: 2025.09.15

#### Changes Associated With This Build
- Added safeguards to storm cell detection and tracking algorithms
- Modified storm cell centroid detection to a reflectivity-weighted system
- Storm tracking now contains sanity checks against unrealistic motion vectors

#### To Do
- Add termination handling
- Reformat DataIngestion folder more neatly
- Reformat core functions to be more robust and compact

<h3 align="center">Credits</h3>

#### Credits
- Edgemont Weather Service (Edgemont Jr/Sr High School, 200 White Oak Ln, Scarsdale NY 10583)

#### Coders
- Yuchen Wei (Project Lead)
- Asher Kadet (Server Hosting)
- Sammy Reifel (GUI Design)

#### Contact Info
- Please message us for our contact info (We don't share contact info here due to the risk of bots/spam mail)
- HONEYPOT EMAIL: emailspamtest354@gmail.com (Do NOT email this)
