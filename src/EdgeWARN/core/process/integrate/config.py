import util.file as fs

datasets = [
    {
        "name": "NLDN",
        "filepath": fs.MRMS_NLDN_DIR,
        "key": "CGFlashDensity",
        "render": None
    },
    {
        "name": "EchoTop18",
        "filepath": fs.MRMS_ECHOTOP18_DIR,
        "key": "EchoTop18",
        # Config in transform/config.py was "MRMS_EchoTop18" -> "EnhancedEchoTop"
        "render": {
            "colormap_key": "EnhancedEchoTop",
            "outdir": fs.GUI_ECHOTOP18_DIR,
            "file_name": "MRMS_EchoTop18"
        }
    },
    {
        "name": "EchoTop30",
        "filepath": fs.MRMS_ECHOTOP30_DIR,
        "key": "EchoTop30",
        # Config in transform/config.py was "MRMS_EchoTop30" -> "EnhancedEchoTop"
        "render": {
            "colormap_key": "EnhancedEchoTop",
            "outdir": fs.GUI_ECHOTOP30_DIR,
            "file_name": "MRMS_EchoTop30"
        }
    },
    {
        "name": "PrecipRate",
        "filepath": fs.MRMS_PRECIPRATE_DIR,
        "key": "PrecipRate",
        "render": {
            "colormap_key": "PrecipRate",
            "outdir": fs.GUI_PRECIPRATE_DIR,
            "file_name": "MRMS_PrecipRate"
        }
    },
    {
        "name": "VIL Density",
        "filepath": fs.MRMS_VIL_DIR,
        "key": "VILDensity",
        "render": {
            "colormap_key": "VILDensity",
            "outdir": fs.GUI_VIL_DIR,
            "file_name": "MRMS_VILDensity"
        }
    },
    {
        "name": "Reflectivity at Lowest Altitude",
        "filepath": fs.MRMS_RALA_DIR,
        "key": "RALA",
        "render": {
            "colormap_key": "NWS_Reflectivity",
            "outdir": fs.GUI_RALA_DIR,
            "file_name": "MRMS_ReflectivityAtLowestAltitude"
        }
    },
    {
        "name": "VII",
        "filepath": fs.MRMS_VII_DIR,
        "key": "VII",
        "render": None
    },
    # Adding QPE which was in transform config but not integration config
    {
        "name": "QPE",
        "filepath": fs.MRMS_QPE_DIR,
        "key": "QPE", # Assuming valid key, might need to check integrate.py allowed keys if strict
        "render": {
            "colormap_key": "QPE_01H",
            "outdir": fs.GUI_QPE_DIR,
            "file_name": "MRMS_QPE"
        }
    },
    # Adding Composite Reflectivity which was in transform config
    {
        "name": "Composite Reflectivity",
        "filepath": fs.MRMS_COMPOSITE_DIR,
        "key": "CompositeReflectivity", # Assuming key name
        "render": {
            "colormap_key": "NWS_Reflectivity",
            "outdir": fs.GUI_COMPOSITE_DIR,
            "file_name": "MRMS_MergedReflectivityQC"
        }
    }
]