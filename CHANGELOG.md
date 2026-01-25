# Changelog for Version ``1.3.1``

## Additions


## Changes
- **MRMS HTTPS Fallback**
    - Implemented fallback to `mrms.ncep.noaa.gov` if S3 bucket is inaccessible.
    - Added retry logic to scheduler: triggers fallback after 3 consecutive S3 failures.
    - New module `https_client.py` for direct NCEP access.

## Fixes