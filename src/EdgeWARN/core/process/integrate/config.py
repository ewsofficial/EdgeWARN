import util.file as fs

def get_datasets_config():
    return [
        {
            "name": "NLDN",
            "filepath": fs.MRMS_NLDN_DIR,
            "key": "CGFlashDensity",
            "method": "max"
        },
        {
            "name": "EchoTop18",
            "filepath": fs.MRMS_ECHOTOP18_DIR,
            "key": "EchoTop18",
            "method": "max" 
        },
        {
            "name": "EchoTop30",
            "filepath": fs.MRMS_ECHOTOP30_DIR,
            "key": "EchoTop30",
            "method": "max"
        },
        {
            "name": "PrecipRate",
            "filepath": fs.MRMS_PRECIPRATE_DIR,
            "key": "PrecipRate",
            "method": "max"
        },
        {
            "name": "VIL Density",
            "filepath": fs.MRMS_VIL_DIR,
            "key": "VILDensity",
            "method": "max"
        },
        {
            "name": "Reflectivity at Lowest Altitude",
            "filepath": fs.MRMS_RALA_DIR,
            "key": "RALA",
            "method": "max"
        },
        {
            "name": "VII",
            "filepath": fs.MRMS_VII_DIR,
            "key": "VII",
            "method": "max"
        }
    ]