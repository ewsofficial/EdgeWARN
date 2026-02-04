import util.file as fs

def get_datasets_config():
    return [
        # === Lightning ===
        {
            "name": "NLDN",
            "filepath": fs.MRMS_NLDN_DIR,
            "key": "p100CGFlashDensity",
            "method": "max"
        },
        # === Echo Tops ===
        # ET 18
        {
            "name": "EchoTop18",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "key": "p100EchoTop18",
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
            "key": "p100EchoTop30",
            "method": "max"
        },
        # === VIL Density ===
        {
            "name": "VIL Density",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p100VILDensity",
            "method": "max"
        },
        {
            "name": "VIL Density (95th)",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p95VILDensity",
            "method": "percentile",
            "percentile": 95
        },
        {
            "name": "VIL Density (90th)",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p90VILDensity",
            "method": "percentile",
            "percentile": 90
        },
        {
            "name": "VIL Density (50th)",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "p50VILDensity",
            "method": "percentile",
            "percentile": 50
        },
        # === AzShear ===
        {
            "name": "AzShear Low",
            "filepath": fs.MRMS_AZSHEARLOW_DIR,
            "key": "p100AzShearLow",
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
            "key": "p100AzShearMid",
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
            "key": "p100PrecipRate",
            "method": "max"
        },
        {
            "name": "Reflectivity at Lowest Altitude",
            "filepath": fs.MRMS_RALA_DIR,
            "key": "p100RALA",
            "method": "max"
        },
        {
            "name": "VII",
            "filepath": fs.MRMS_VII_DIR,
            "key": "p100VII",
            "method": "max"
        }
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
                "levels": [850, 700, 500, 250],
                "key_template": "u{level}"
            },
            {
                "filter": {"typeOfLevel": "isobaricInhPa"},
                "var": "v",
                "levels": [850, 700, 500, 250],
                "key_template": "v{level}"
            },
            # === Surface 2m ===
            {
                "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
                "var": "t2m",
                "key": "temp_2m",
                "transform": "kelvin_to_celsius"
            },
            {
                "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
                "var": "d2m",
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