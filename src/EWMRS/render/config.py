import util.file as fs

# Tile configuration constants
TILE_SIZE = 250  # pixels
TILE_GRID_ROWS = 14  # 3500 / 250
TILE_GRID_COLS = 28  # 7000 / 250

def get_mrms_file_list():
    """Return the MRMS-backed render configuration list."""
    return [
        {
            "name": "MRMS_MergedReflectivityQC",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_COMPOSITE_DIR,
            "outdir": fs.GUI_COMPOSITE_DIR
        },
        {
            "name": "MRMS_EchoTop18",
            "colormap_key": "EnhancedEchoTop",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "outdir": fs.GUI_ECHOTOP18_DIR
        },
        {
            "name": "MRMS_EchoTop30",
            "colormap_key": "EnhancedEchoTop",
            "filepath": fs.MRMS_ECHOTOP30_DIR,
            "outdir": fs.GUI_ECHOTOP30_DIR
        },
        {
            "name": "MRMS_ReflectivityAtLowestAltitude",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_RALA_DIR,
            "outdir": fs.GUI_RALA_DIR
        },
        {
            "name": "MRMS_ReflectivityAt0C",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_REF_0C_DIR,
            "outdir": fs.GUI_REF_0C_DIR
        },
        {
            "name": "MRMS_ReflectivityAtM5C",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_REFM5C_DIR,
            "outdir": fs.GUI_REFM5C_DIR
        },
        {
            "name": "MRMS_ReflectivityAtM15C",
            "colormap_key": "NWS_Reflectivity",
            "filepath": fs.MRMS_REFM15C_DIR,
            "outdir": fs.GUI_REFM15C_DIR
        },
        {
            "name": "MRMS_PrecipRate",
            "colormap_key": "PrecipRate",
            "filepath": fs.MRMS_PRECIPRATE_DIR,
            "outdir": fs.GUI_PRECIPRATE_DIR
        },
        {
            "name": "MRMS_VILDensity",
            "colormap_key": "VILDensity",
            "filepath": fs.MRMS_DVIL_DIR,
            "outdir": fs.GUI_VILD_DIR
        },
        {
            "name": "MRMS_QPE",
            "colormap_key": "QPE_01H",
            "filepath": fs.MRMS_QPE_DIR,
            "outdir": fs.GUI_QPE_DIR
        },
        {
            "name": "MRMS_VIL",
            "colormap_key": "VIL",
            "filepath": fs.MRMS_VIL_DIR,
            "outdir": fs.GUI_VIL_DIR
        },
        {
            "name": "MRMS_VII",
            "colormap_key": "VIL",
            "filepath": fs.MRMS_VII_DIR,
            "outdir": fs.GUI_VII_DIR
        },
        {
            "name": "MRMS_MergedAzShear_0-2kmAGL",
            "colormap_key": "AzShear",
            "filepath": fs.MRMS_AZSHEARLOW_DIR,
            "outdir": fs.GUI_AZSHEARLOW_DIR
        },
        {
            "name": "MRMS_MergedAzShear_3-6kmAGL",
            "colormap_key": "AzShear",
            "filepath": fs.MRMS_AZSHEARMID_DIR,
            "outdir": fs.GUI_AZSHEARMID_DIR
        },
        {
            "name": "MRMS_MESH",
            "colormap_key": "MESH",
            "filepath": fs.MRMS_MESH_DIR,
            "outdir": fs.GUI_MESH_DIR
        }
    ]


def get_goes_file_list():
    """Return the GOES-backed render configuration list."""
    return [
        {
            "name": "GOES_ABI_C02_Reflectance",
            "colormap_key": "GOES_ABI_C02_Reflectance",
            "filepath": fs.GOES_ABI_VISIBLE_RED_DIR,
            "outdir": fs.GUI_GOES_C02_DIR,
            "source_type": "goes_abi",
            "variable_name": "CMI",
            "fallback_variable_names": ["Rad"],
            "channel_id": "C02",
            "display_name": "GOES ABI C02 Reflectance",
            "value_transform": "reflectance_from_rad",
            "mask_min": {
                "C02": 0.0,
                "default": 0.0,
            },
            "mask_max": {
                "C02": 1.2,
                "default": 1.2,
            },
        },
        {
            "name": "GOES_ABI_C13_BrightnessTemp",
            "colormap_key": "GOES_ABI_C13_BrightnessTemp",
            "filepath": fs.GOES_ABI_CLEAN_LWIR_DIR,
            "outdir": fs.GUI_GOES_C13_DIR,
            "source_type": "goes_abi",
            "variable_name": "CMI",
            "fallback_variable_names": ["Rad"],
            "channel_id": "C13",
            "display_name": "GOES ABI C13 Brightness Temperature",
            "value_transform": "brightness_temp_from_rad",
            "mask_min": {
                "C13": 180.0,
                "default": 180.0,
            },
            "mask_max": {
                "C13": 330.0,
                "default": 330.0,
            },
        },
    ]


def get_file_list():
    """Return the combined render configuration list."""
    return get_mrms_file_list() + get_goes_file_list()

# For backward compatibility - returns list at import time (use get_file_list() for dynamic paths)
file_list = get_file_list()
