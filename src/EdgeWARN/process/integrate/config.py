import util.file as fs

def get_datasets_config():
    return [
        # === Reflectivity Layers ===
        {
            "name": "Ref0",
            "filepath": fs.MRMS_REF_0C_DIR,
            "key": "Ref0",
            "method": "max"
        },
        {
            "name": "Ref5",
            "filepath": fs.MRMS_REFM5C_DIR,
            "key": "Ref5",
            "method": "max"
        },
        {
            "name": "Ref15",
            "filepath": fs.MRMS_REFM15C_DIR,
            "key": "Ref15",
        },
        # === Lightning ===
        {
            "name": "NLDN",
            "filepath": fs.MRMS_NLDN_DIR,
            "key": "maxCGFlashDensity",
            "method": "max"
        },
        # === Echo Tops ===
        # ET 18
        {
            "name": "EchoTop18",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "key": "maxEchoTop18",
            "method": "max" 
        },
        {
            "name": "EchoTop18 (95th)",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "key": "p95EchoTop18",
            "method": "percentile",
            "percentile": 95
        },
        {
            "name": "EchoTop18 (90th)",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "key": "p90EchoTop18",
            "method": "percentile",
            "percentile": 90
        },
        # ET 30
        {
            "name": "EchoTop30",
            "filepath": fs.MRMS_ECHOTOP30_DIR,
            "key": "maxEchoTop30",
            "method": "max"
        },
        {
            "name": "EchoTop30 (90th)",
            "filepath": fs.MRMS_ECHOTOP30_DIR,
            "key": "p90EchoTop30",
            "method": "percentile",
            "percentile": 90
        },
        # === VIL and VIL Density ===
        {
            "name": "VIL",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "maxVIL",
            "method": "max"
        },
        {
            "name": "VIL (95th)",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p95VIL",
            "method": "percentile",
            "percentile": 95
        },
        {
            "name": "VIL (90th)",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p90VIL",
            "method": "percentile",
            "percentile": 90
        },
        {
            "name": "VIL (50th)",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p50VIL",
            "method": "percentile",
            "percentile": 50
        },
        {
            "name": "VIL Density",
            "filepath": fs.MRMS_DVIL_DIR,
            "key": "maxVILDensity",
            "method": "max"
        },
        {
            "name": "VIL Density (95th)",
            "filepath": fs.MRMS_DVIL_DIR,
            "key": "p95VILDensity",
            "method": "percentile",
            "percentile": 95
        },
        {
            "name": "VIL Density (90th)",
            "filepath": fs.MRMS_DVIL_DIR,
            "key": "p90VILDensity",
            "method": "percentile",
            "percentile": 90
        },
        {
            "name": "VIL Density (50th)",
            "filepath": fs.MRMS_DVIL_DIR,
            "key": "p50VILDensity",
            "method": "percentile",
            "percentile": 50
        },
        # === AzShear ===
        {
            "name": "AzShear Low",
            "filepath": fs.MRMS_AZSHEARLOW_DIR,
            "key": "maxAzShearLow",
            "method": "max"
        },
        {
            "name": "AzShear Low (95th)",
            "filepath": fs.MRMS_AZSHEARLOW_DIR,
            "key": "p95AzShearLow",
            "method": "percentile",
            "percentile": 95
        },
        {
            "name": "AzShear Mid",
            "filepath": fs.MRMS_AZSHEARMID_DIR,
            "key": "maxAzShearMid",
            "method": "max"
        },
        {
            "name": "AzShear Mid (95th)",
            "filepath": fs.MRMS_AZSHEARMID_DIR,
            "key": "p95AzShearMid",
            "method": "percentile",
            "percentile": 95
        },
        # === Others ===
        {
            "name": "PrecipRate",
            "filepath": fs.MRMS_PRECIPRATE_DIR,
            "key": "maxPrecipRate",
            "method": "max"
        },
        {
            "name": "Reflectivity at Lowest Altitude",
            "filepath": fs.MRMS_RALA_DIR,
            "key": "maxRALA",
            "method": "max"
        },
        {
            "name": "VII",
            "filepath": fs.MRMS_VII_DIR,
            "key": "maxVII",
            "method": "max"
        },
    ]


def get_rap_products():
    """
    Configuration for RAP GRIB2 extraction.
    Each entry specifies filter keys, variable name, and output property key.
    """
    return {
        "products": [
            # === Isobaric Winds ===
            {
                "filter": {"typeOfLevel": "isobaricInhPa"},
                "var": "u",
                "var_aliases": ["u", "UGRD", "u-component_of_wind_isobaric", "wind_u"],
                "levels": [
                    1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 725, 700, 
                    675, 650, 625, 600, 575, 550, 525, 500, 475, 450, 425, 400, 375, 
                    350, 325, 300, 275, 250, 225, 200, 175, 150, 125, 100
                ],
                "key_template": "wind_field.u{level}"
            },
            {
                "filter": {"typeOfLevel": "isobaricInhPa"},
                "var": "v",
                "var_aliases": ["v", "VGRD", "v-component_of_wind_isobaric", "wind_v"],
                "levels": [
                    1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 725, 700, 
                    675, 650, 625, 600, 575, 550, 525, 500, 475, 450, 425, 400, 375, 
                    350, 325, 300, 275, 250, 225, 200, 175, 150, 125, 100
                ],
                "key_template": "wind_field.v{level}"
            },
            # === Surface 10m Winds ===
            {
                "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
                "var": "u10",
                "var_aliases": ["u10", "10u", "u"],
                "key": "u10m"
            },
            {
                "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
                "var": "v10",
                "var_aliases": ["v10", "10v", "v"],
                "key": "v10m"
            },
            # === Surface 2m ===
            {
                "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
                "var": "t2m",
                "var_aliases": ["t2m", "2t", "t"],
                "key": "temp_2m",
                "transform": "kelvin_to_celsius"
            },
            {
                "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
                "var": "d2m",
                "var_aliases": ["d2m", "2d", "dpt"],
                "key": "dewpoint_2m",
                "transform": "kelvin_to_celsius"
            },
            # === Freezing Level ===
            {
                "filter": {"typeOfLevel": "isothermZero"},
                "var": "gh",
                "key": "freezing_level_m"
            },
        ],
        "derived": [
            {
                "formula": "temp_2m - dewpoint_2m",
                "key": "dewpoint_depression"
            },
            {
                "formula": "freezing_level_m / 1000",
                "key": "freezing_level_height"
            }
        ]
    }
