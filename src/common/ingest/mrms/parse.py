"""S3 key prefixes for MRMS and GOES lookups.

Both functions are thin wrappers now: the grammars live in `ingest.yaml` under
`mrms.path_patterns` and `goes.bucket_path_pattern`, so an upstream bucket
reorganization is an operator edit. The wrappers stay because two other modules
import them by name, and because the argument order is the useful part of the API.
"""

from common.ingest.mrms.config import goes_bucket_path, mrms_s3_prefix


def parse_mrms_bucket_path(dt, region, modifier):
    """Return the MRMS S3 key prefix ``region/[modifier/]YYYYMMDD/``.

    Args:
        dt (datetime): timestamp selecting the day segment
        region (str): region name, e.g. ``CONUS`` or ``ProbSevere``
        modifier (str | None): product/folder segment; ``None`` omits it entirely
    """
    return mrms_s3_prefix(dt, region, modifier)


def parse_goes_bucket_path(dt, product, hour_offset=0):
    """Return the GOES S3 key prefix ``product/YYYY/DDD/HH/``.

    Args:
        dt (datetime): timestamp selecting the hour segment
        product (str): GOES product name, e.g. ``GLM-L2-LCFA``
        hour_offset (int): hours subtracted from ``dt`` before formatting
    """
    return goes_bucket_path(dt, product, hour_offset=hour_offset)
