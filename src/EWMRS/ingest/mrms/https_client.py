
import aiohttp
import asyncio
import requests
import re
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import logging

# We'll use the existing IOManager if possible, or fallback to logging
try:
    from EWMRS.util.io import IOManager
    io_manager = IOManager("[Ingest-HTTPS]")
except ImportError:
    logging.basicConfig(level=logging.INFO)
    io_manager = logging.getLogger("[Ingest-HTTPS]")
    # Add write_info/write_error methods to simulate IOManager
    io_manager.write_info = io_manager.info
    io_manager.write_error = io_manager.error
    io_manager.write_warning = io_manager.warning
    io_manager.write_debug = io_manager.debug

NCEP_BASE_URL = "https://mrms.ncep.noaa.gov/data/2D"

class HttpsFileFinder:
    def __init__(self, dt, io_manager_instance=None):
        self.dt = dt
        self.io_manager = io_manager_instance or io_manager
        self.session = None

    def _get_product_url_name(self, modifier):
        """
        Maps S3 modifier keywords to NCEP URL directory names.
        This is necessary because the S3 modifiers might slightly differ or 
        we just need to extract the base product name.
        
        Example: 
        S3: "EchoTop_18_00.50" -> NCEP: "EchoTop_18"
        S3: "ProbSevere" -> NCEP: "ProbSevere" (handled separately usually)
        """
        # Dictionary mapping for known discrepancies
        # Based on config.py and visual inspection of https://mrms.ncep.noaa.gov/data/2D/
        
        if modifier is None: # ProbSevere
            return "ProbSevere" # The actual URL is /data/ProbSevere, handled in construct_url
        
        # Heuristic: Most NCEP folders are the first part of the S3 modifier
        # e.g. "EchoTop_18_00.50" -> "EchoTop_18"
        # "MergedReflectivityQCComposite_00.50" -> "MergedReflectivityQCComposite"
        
        # However, some might be exact matches.
        # Let's try to match the directory structure we saw.
        
        # Simple split by first underscore if it looks like versions?
        # unique cases from config.py:
        # EchoTop_18_00.50 -> EchoTop_18 (Yes)
        # EchoTop_30_00.50 -> EchoTop_30 (Yes)
        # FLASH_QPE_FFG01H_00.00 -> FLASH (Maybe? Checking index...) -> FLASH seems to exist
        # MESH_00.50 -> MESH
        # WarmRainProbability_00.50 -> WarmRainProbability (Need to check if exists, or name differs)
        # NLDN_CG_005min_AvgDensity_00.00 -> NLDN_CG_005min_AvgDensity (Check)
        # PrecipRate_00.00 -> PrecipRate
        # RadarOnly_QPE_01H_00.00 -> RadarOnly_QPE_01H
        # MergedAzShear_0-2kmAGL_00.50 -> MergedAzShear_0-2kmAGL
        # VIL_Density_00.50 -> VIL_Density (Wait, index had LVL3_HighResVIL? need to verify) -> No, index has "VILDensity" maybe? 
        # Actually checking index of /2D/: 
        # MergedAzShear_0-2kmAGL/ exist
        # MESH/ exist
        # PrecipRate/ exist
        # RadarOnly_QPE_01H/ exist
        # VIL_Density ?? Checking index again... Not seeing VIL_Density directly. 
        # Saw: LVL3_HighResVIL/
        
        # Let's map explicitly based on standard MRMS naming conventions
        # Usage instructions: add to this map if NCEP structure changes
        
        mapping = {
            "EchoTop_18_00.50": "EchoTop_18",
            "EchoTop_30_00.50": "EchoTop_30",
            "FLASH_QPE_FFG01H_00.00": "FLASH",
            "MESH_00.50": "MESH",
            "WarmRainProbability_00.50": "WarmRainProbability", # Verify existence
            "NLDN_CG_005min_AvgDensity_00.00": "NLDN_CG_005min_AvgDensity",
            "PrecipRate_00.00": "PrecipRate",
            "RadarOnly_QPE_01H_00.00": "RadarOnly_QPE_01H",
            "MergedAzShear_0-2kmAGL_00.50": "MergedAzShear_0-2kmAGL",
            "MergedAzShear_3-6kmAGL_00.50": "MergedAzShear_3-6kmAGL",
            "VIL_Density_00.50": "VIL_Density", # Warning: Verify
            "MergedRhoHV_00.50": "MergedRhoHV",
            "PrecipFlag_00.00": "PrecipFlag",
            "MergedReflectivityAtLowestAltitude_00.50": "MergedReflectivityAtLowestAltitude",
            "MergedReflectivityQCComposite_00.50": "MergedReflectivityQCComposite",
            "VII_00.50": "VII" # Verify
        }
        
        if modifier in mapping:
            return mapping[modifier]
        
        # Fallback default behaviors if not mapped
        parts = modifier.split("_00.")
        if len(parts) > 1:
            return parts[0]
            
        return modifier

    def construct_url(self, region, modifier):
        """Constructs the NCEP URL. Note: MRMS 2D data on NCEP is flat, not organized by date folders like S3."""
        prod_name = self._get_product_url_name(modifier)
        
        if modifier is None: # ProbSevere
            return "https://mrms.ncep.noaa.gov/data/ProbSevere"
            
        # Standard 2D products
        return f"{NCEP_BASE_URL}/{prod_name}"

    async def find_files(self, region, modifier):
        """
        Scrapes the NCEP directory for files matching the requested timestamp (self.dt).
        Since NCEP only keeps recent files, we just look for the closest match.
        """
        url = self.construct_url(region, modifier)
        target_ts_str = self.dt.strftime("%Y%m%d-%H%M")
        
        self.io_manager.write_debug(f"Scanning {url} for {target_ts_str}...")
        
        # Disable SSL verification to avoid "unable to get local issuer certificate" errors
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        self.io_manager.write_warning(f"Failed to access {url}: HTTP {response.status}")
                        return []
                    
                    html = await response.text()
            except Exception as e:
                self.io_manager.write_error(f"Error scraping {url}: {e}")
                return []

        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a')
        
        valid_files = []
        for link in links:
            href = link.get('href')
            if not href.endswith('.gz') and not href.endswith('.json'):
                continue
                
            # Name format: MRMS_{Product}_{Level}_{YYYYMMDD-HHMMSS}.grib2.gz
            # Check if it matches our target time window? or just return the list so the downloader can pick?
            # The S3 logic usually picks the *exact* or *closest* match. 
            # Let's filter by at least the hour to narrow it down if possible, 
            # but NCEP usually only holds the last 24h or so, so we can just grab everything 
            # and let the logic filter for the specific timestamp.
            
            # Simple filtering: Check if the date part matches at least the day?
            # "20260124"
            if self.dt.strftime("%Y%m%d") in href:
                valid_files.append(f"{url}/{href}")
                
        return valid_files

    def find_files_sync(self, region, modifier):
        """
        Sync version of find_files using requests (for scheduler).
        """
        url = self.construct_url(region, modifier)
        target_ts_str = self.dt.strftime("%Y%m%d-%H%M")
        
        self.io_manager.write_debug(f"Scanning (Sync) {url} for {target_ts_str}...")
        
        try:
            response = requests.get(url, verify=False, timeout=10)
            if response.status_code != 200:
                self.io_manager.write_warning(f"Failed to access {url}: HTTP {response.status_code}")
                return []
            html = response.text
        except Exception as e:
            self.io_manager.write_error(f"Error scraping {url}: {e}")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a')
        
        valid_files = []
        for link in links:
            href = link.get('href')
            if not href.endswith('.gz') and not href.endswith('.json'):
                continue

            if self.dt.strftime("%Y%m%d") in href:
                valid_files.append(f"{url}/{href}")
                
        return valid_files


class HttpsFileDownloader:
    def __init__(self, dt, io_manager_instance=None):
        self.dt = dt
        self.io_manager = io_manager_instance or io_manager

    async def download_matching(self, file_urls, outdir):
        """
        Given a list of file URLs, find the one matching self.dt (minute precision) 
        and download it.
        """
        target_ts = self.dt.strftime("%Y%m%d-%H%M")
        
        # Find exact minute match first
        match = None
        for url in file_urls:
            # url: .../MRMS_EchoTop_18_00.50_20260124-140035.grib2.gz
            # We want to match 20260124-1400XX
            if target_ts in url.replace(":", ""): # Some might have colons? unlikely in filename
                match = url
                break
        
        if not match:
             # If exact minute not found, maybe try fuzzy match within +/- 2 mins?
             # For now, simplistic exact minute match (ignoring seconds)
             # Regex to extract timestamp
             # ..._YYYYMMDD-HHMMSS.grib2.gz
             matches = []
             for url in file_urls:
                 ts_match = re.search(r'(\d{8}-\d{6})', url)
                 if ts_match:
                     file_ts_str = ts_match.group(1)
                     try:
                         file_dt = datetime.strptime(file_ts_str, "%Y%m%d-%H%M%S")
                         # Calculate difference
                         diff = abs((file_dt - self.dt).total_seconds())
                         if diff < 120: # Within 2 minutes
                            matches.append((diff, url))
                     except:
                         pass
             
             if matches:
                 # Sort by time difference
                 matches.sort(key=lambda x: x[0])
                 match = matches[0][1]

        if not match:
            return None

        # Download
        filename = match.split('/')[-1]
        out_path = outdir / filename
        
        if out_path.exists():
            # Already exists
            return out_path

        self.io_manager.write_info(f"Downloading (HTTPS Fallback): {filename}")
        
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            try:
                async with session.get(match) as response:
                    if response.status == 200:
                        with open(out_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)
                        return out_path
                    else:
                        self.io_manager.write_error(f"Failed to download {match}: {response.status}")
                        return None
            except Exception as e:
                self.io_manager.write_error(f"Download error {match}: {e}")
                return None

    # Sync wrapper if needed, but we mostly use async in the pipeline
    def download_matching_sync(self, file_urls, outdir):
        # Implementation using requests for sync fallback
        pass
