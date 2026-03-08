
import util.file as fs

bucket = "noaa-mrms-pds"
goes_bucket = "noaa-goes19"

def get_mrms_modifiers():
    return [
        ("CONUS", "EchoTop_18_00.50", fs.MRMS_ECHOTOP18_DIR), # Region / Product / Outdir
        ("CONUS", "EchoTop_30_00.50", fs.MRMS_ECHOTOP30_DIR),
        ("CONUS", "FLASH_CREST_MAXUNITSTREAMFLOW_00.00", fs.MRMS_FLASH_CREST_MAXUNIT_DIR),
        ("CONUS", "FLASH_QPE_ARIMAX_00.00", fs.MRMS_FLASH_ARIMAX_DIR),
        ("CONUS", "FLASH_QPE_ARI30M_00.00", fs.MRMS_FLASH_ARI30M_DIR),
        ("CONUS", "FLASH_QPE_ARI01H_00.00", fs.MRMS_FLASH_ARI01H_DIR),
        ("CONUS", "FLASH_HP_MAXUNITSTREAMFLOW_00.00", fs.MRMS_FLASH_HP_MAXUNIT_DIR),
        ("CONUS", "FLASH_SAC_MAXSOILSAT_00.00", fs.MRMS_FLASH_SAC_MAXSOIL_DIR),
        ("CONUS", "FLASH_QPE_FFGMAX_00.00", fs.MRMS_FLASH_FFGMAX_DIR),
        ("CONUS", "RadarQualityIndex_00.00", fs.MRMS_RQI_DIR),
        ("CONUS", "MESH_00.50", fs.MRMS_MESH_DIR),
        ("CONUS", "WarmRainProbability_00.50", fs.MRMS_RAIN_DIR),
        ("CONUS", "NLDN_CG_005min_AvgDensity_00.00", fs.MRMS_NLDN_DIR),
        ("CONUS", "PrecipRate_00.00", fs.MRMS_PRECIPRATE_DIR),
        ("CONUS", "RadarOnly_QPE_01H_00.00", fs.MRMS_QPE_DIR),
        ("CONUS", "MergedAzShear_0-2kmAGL_00.50", fs.MRMS_AZSHEARLOW_DIR),
        ("CONUS", "MergedAzShear_3-6kmAGL_00.50", fs.MRMS_AZSHEARMID_DIR),
        ("CONUS", "VIL_Density_00.50", fs.MRMS_DVIL_DIR),
        ("ProbSevere", None, fs.MRMS_PROBSEVERE_DIR),
        ("CONUS", "MergedRhoHV_00.50", fs.MRMS_RHOHV_DIR),
        ("CONUS", "PrecipFlag_00.00", fs.MRMS_PRECIPTYP_DIR),
        ("CONUS", "MergedReflectivityAtLowestAltitude_00.50", fs.MRMS_RALA_DIR),
        ("CONUS", "MergedReflectivityQCComposite_00.50", fs.MRMS_COMPOSITE_DIR),
        ("CONUS", "VII_00.50", fs.MRMS_VII_DIR),
        ("CONUS", "VIL_00.50", fs.MRMS_VIL_DIR),
        ("CONUS", "Reflectivity_0C_00.50", fs.MRMS_REF_0C_DIR),
        ("CONUS", "Reflectivity_-5C_00.50", fs.MRMS_REFM5C_DIR),
        ("CONUS", "Reflectivity_-15C_00.50", fs.MRMS_REFM15C_DIR)
    ]

def get_check_modifiers():
    return [
        ("CONUS", "EchoTop_18_00.50", fs.MRMS_ECHOTOP18_DIR), # Region / Product / Outdir
        ("CONUS", "EchoTop_30_00.50", fs.MRMS_ECHOTOP30_DIR),
        ("CONUS", "PrecipRate_00.00", fs.MRMS_PRECIPRATE_DIR),
        ("CONUS", "MergedAzShear_0-2kmAGL_00.50", fs.MRMS_AZSHEARLOW_DIR),
        ("CONUS", "MergedAzShear_3-6kmAGL_00.50", fs.MRMS_AZSHEARMID_DIR),
        ("CONUS", "VIL_Density_00.50", fs.MRMS_DVIL_DIR),
        ("ProbSevere", None, fs.MRMS_PROBSEVERE_DIR),
        ("CONUS", "PrecipFlag_00.00", fs.MRMS_PRECIPTYP_DIR),
        ("CONUS", "MergedReflectivityAtLowestAltitude_00.50", fs.MRMS_RALA_DIR),
        ("CONUS", "MergedReflectivityQCComposite_00.50", fs.MRMS_COMPOSITE_DIR),
        ("CONUS", "VII_00.50", fs.MRMS_VII_DIR)
    ]


def get_goes_modifiers():
    return [
       ("GLM-L2-LCFA", fs.GOES_GLM_DIR)
    ]