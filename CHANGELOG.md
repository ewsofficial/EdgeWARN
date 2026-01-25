# Changelog for Version ``1.3.1``

## Additions


## Changes
- **MRMS HTTPS Fallback**
    - Implemented fallback to `mrms.ncep.noaa.gov` if S3 bucket is inaccessible.
    - Integrated aggressive fallback in scheduler: immediately checks HTTPS if S3 timestamp is stale or unavailable.
    - New module `https_client.py` for direct NCEP access.

## Fixes