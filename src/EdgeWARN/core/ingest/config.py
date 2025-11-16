from util.file import *

mrms_modifiers = [
    ("CONUS", "EchoTop_18_00.50", MRMS_ECHOTOP18_DIR), # Region / Product / Outdir
    ("CONUS", "EchoTop_30_00.50", MRMS_ECHOTOP30_DIR),
    ("CONUS", "FLASH_QPE_FFG01H_00.00", MRMS_FLASH_DIR),
    ("CONUS", "NLDN_CG_005min_AvgDensity_00.00", MRMS_NLDN_DIR),
    ("CONUS", "PrecipRate_00.00", MRMS_PRECIPRATE_DIR),
    ("CONUS", "RadarOnly_QPE_01H_00.00", MRMS_QPE_DIR),
    ("CONUS", "RotationTrack30min_00.50", MRMS_ROTATIONT_DIR),
    ("CONUS", "VIL_Density_00.50", MRMS_VIL_DIR),
    ("ProbSevere", None, MRMS_PROBSEVERE_DIR),
    ("CONUS", "MergedRhoHV_00.50", MRMS_RHOHV_DIR),
    ("CONUS", "PrecipFlag_00.00", MRMS_PRECIPTYP_DIR),
    ("CONUS", "MergedReflectivityAtLowestAltitude_00.00", MRMS_RALA_DIR),
    ("CONUS", "MergedReflectivityQCComposite_00.50", MRMS_COMPOSITE_DIR),
    ("CONUS", "VII_00.50", MRMS_VII_DIR)
]