**/nexrad/** - List active sites
 - Example response:

```json
["KOKX", "KOHX", "KTLX"]
```

**/nexrad/<SITE>/** - List valid elevations of a radar site and their timestamps in YYYYMMDD-HHMMSS format
 - **SITE**: Four-letter identifier of radar site (e.g. KOKX, KBOX)
 - Example response:

```json
{
    "0.5": [
        "20260512-004753",
        "20260512-004336"
    ],
    "0.9": [
        "20260512-004753",
        "20260512-004336"
    ],
    "1.3": [
        "20260512-004753",
        "20260512-004336"
    ],
    "1.8": [
        "20260512-004753",
        "20260512-004336"
    ]
}
```

**/nexrad/<SITE>/<TIMESTAMP>/<ELEVATION>?product=PRODUCT** - Download the bin.gz data file for a specific product
 - **SITE**: Four letter identifier of radar site
 - **TIMESTAMP**: Timestamp of scan in YYYYMMDD-HHMMSS format
 - **ELEVATION**: Sweep elevation (e.g. 0.5, 0.9)
 - **PRODUCT**: Radar product to download (DBZH, VRADH, WRADH, PHIDP, CCORH, RHOHV, ZDR ONLY)
