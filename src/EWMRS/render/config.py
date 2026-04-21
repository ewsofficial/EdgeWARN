import util.file as fs

# Tile configuration constants
TILE_SIZE = 350  # pixels
TILE_GRID_ROWS = 10  # 3500 / 350
TILE_GRID_COLS = 20  # 7000 / 350

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
    reflectance_specs = [
        ("C01", fs.GOES_ABI_VISIBLE_BLUE_DIR, fs.GUI_GOES_C01_DIR),
        ("C02", fs.GOES_ABI_VISIBLE_RED_DIR, fs.GUI_GOES_C02_DIR),
        ("C03", fs.GOES_ABI_VEGGIE_DIR, fs.GUI_GOES_C03_DIR),
        ("C04", fs.GOES_ABI_CIRRUS_DIR, fs.GUI_GOES_C04_DIR),
        ("C05", fs.GOES_ABI_SNOW_ICE_DIR, fs.GUI_GOES_C05_DIR),
        ("C06", fs.GOES_ABI_PARTICLE_SIZE_DIR, fs.GUI_GOES_C06_DIR),
    ]
    brightness_temp_specs = [
        ("C07", fs.GOES_ABI_SHORTWAVE_IR_DIR, fs.GUI_GOES_C07_DIR, "GOES_ABI_C07_BrightnessTemp", 180.0, 330.0),
        ("C08", fs.GOES_ABI_UPPER_LEVEL_WV_DIR, fs.GUI_GOES_C08_DIR, "GOES_ABI_C08_BrightnessTemp", 180.0, 300.0),
        ("C09", fs.GOES_ABI_MID_LEVEL_WV_DIR, fs.GUI_GOES_C09_DIR, "GOES_ABI_C09_BrightnessTemp", 180.0, 310.0),
        ("C10", fs.GOES_ABI_LOWER_LEVEL_WV_DIR, fs.GUI_GOES_C10_DIR, "GOES_ABI_C10_BrightnessTemp", 185.0, 320.0),
        ("C11", fs.GOES_ABI_CLD_TOP_PHASE_DIR, fs.GUI_GOES_C11_DIR, "GOES_ABI_C11_BrightnessTemp", 180.0, 330.0),
        ("C12", fs.GOES_ABI_OZONE_DIR, fs.GUI_GOES_C12_DIR, "GOES_ABI_C12_BrightnessTemp", 180.0, 330.0),
        ("C13", fs.GOES_ABI_CLEAN_LWIR_DIR, fs.GUI_GOES_C13_DIR, "GOES_IR", 180.0, 330.0),
        ("C14", fs.GOES_ABI_LONGWAVE_IR_DIR, fs.GUI_GOES_C14_DIR, "GOES_IR", 180.0, 330.0),
        ("C15", fs.GOES_ABI_DIRTY_LWIR_DIR, fs.GUI_GOES_C15_DIR, "GOES_IR", 180.0, 330.0),
        ("C16", fs.GOES_ABI_CO2_LWIR_DIR, fs.GUI_GOES_C16_DIR, "GOES_ABI_C16_BrightnessTemp", 180.0, 330.0),
    ]

    layers = []
    for channel_id, filepath, outdir in reflectance_specs:
        colormap_key = "GOES_RGB_Raw" if channel_id in {"C01", "C02", "C03", "C04", "C05", "C06"} else f"GOES_ABI_{channel_id}_Reflectance"
        layers.append(
            {
                "name": f"GOES_ABI_{channel_id}_Reflectance",
                "colormap_key": colormap_key,
                "filepath": filepath,
                "outdir": outdir,
                "source_type": "goes_abi",
                "variable_name": "CMI",
                "fallback_variable_names": ["Rad"],
                "channel_id": channel_id,
                "display_name": f"GOES ABI {channel_id} Reflectance",
                "value_transform": "reflectance_from_rad",
                "mask_min": {
                    channel_id: 0.0,
                    "default": 0.0,
                },
                "mask_max": {
                    channel_id: 1.2,
                    "default": 1.2,
                },
            }
        )

    for channel_id, filepath, outdir, colormap_key, min_temp, max_temp in brightness_temp_specs:
        layers.append(
            {
                "name": f"GOES_ABI_{channel_id}_BrightnessTemp",
                "colormap_key": colormap_key,
                "filepath": filepath,
                "outdir": outdir,
                "source_type": "goes_abi",
                "variable_name": "CMI",
                "fallback_variable_names": ["Rad"],
                "channel_id": channel_id,
                "display_name": f"GOES ABI {channel_id} Brightness Temperature",
                "value_transform": "brightness_temp_from_rad",
                "mask_min": {
                    channel_id: min_temp,
                    "default": min_temp,
                },
                "mask_max": {
                    channel_id: max_temp,
                    "default": max_temp,
                },
            }
        )

    return layers


def get_file_list():
    """Return the combined render configuration list."""
    return get_mrms_file_list() + get_goes_file_list()

# For backward compatibility - returns list at import time (use get_file_list() for dynamic paths)
file_list = get_file_list()
