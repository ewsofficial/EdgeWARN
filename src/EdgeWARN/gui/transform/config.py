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
    }
]