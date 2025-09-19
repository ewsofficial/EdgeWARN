# Config file for Data Ingestion
from util.core.file import *

# Downloader Configs

base_dir = "https://mrms.ncep.noaa.gov/"

mrms_modifiers = [
    ("2D/EchoTop_18/", MRMS_ECHOTOP18_DIR),
    ("2D/FLASH/QPE_FFG01H/", MRMS_FLASH_DIR),
    ("2D/NLDN_CG_005min_AvgDensity/", MRMS_NLDN_DIR),
    ("2D/PrecipRate/", MRMS_PRECIPRATE_DIR),
    ("2D/RadarOnly_QPE_01H/", MRMS_QPE15_DIR),
    ("2D/RotationTrack30min/", MRMS_ROTATIONT_DIR),
    ("2D/VIL_Density/", MRMS_VIL_DIR),
    ("ProbSevere/PROBSEVERE/", MRMS_PROBSEVERE_DIR)
]
