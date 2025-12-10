from util.file import *

file_list = [
    {
        "name": "MRMS_MergedReflectivityQC",
        "colormap_key": "NWS_Reflectivity",
        "filepath": MRMS_COMPOSITE_DIR,
        "outdir": GUI_COMPOSITE_DIR
    },
    {
        "name": "MRMS_EchoTop18",
        "colormap_key": "EnhancedEchoTop",
        "filepath": MRMS_ECHOTOP18_DIR,
        "outdir": GUI_ECHOTOP18_DIR
    },
    {
        "name": "MRMS_EchoTop30",
        "colormap_key": "EnhancedEchoTop",
        "filepath": MRMS_ECHOTOP30_DIR,
        "outdir": GUI_ECHOTOP30_DIR
    },
    {
        "name": "MRMS_ReflectivityAtLowestAltitude",
        "colormap_key": "NWS_Reflectivity",
        "filepath": MRMS_RALA_DIR,
        "outdir": GUI_RALA_DIR
    },
    {
        "name": "MRMS_PrecipRate",
        "colormap_key": "PrecipRate",
        "filepath": MRMS_PRECIPRATE_DIR,
        "outdir": GUI_PRECIPRATE_DIR
    },
    {
        "name": "MRMS_VILDensity",
        "colormap_key": "VILDensity",
        "filepath": MRMS_VIL_DIR,
        "outdir": GUI_VIL_DIR
    },
    {
        "name": "MRMS_QPE",
        "colormap_key": "QPE_01H",
        "filepath": MRMS_QPE_DIR,
        "outdir": GUI_QPE_DIR
    }
]