import os


RAP_BUCKET = "noaa-rap-pds"
RAP_FILE_PATTERN = "rap.t{hour:02d}z.awp130pgrbf00.grib2"
RAP_DIR_PATTERN = "rap.{date}"
RAP_MAX_AGE_MINUTES = 180
RAP_MAX_FILES = 3
RAP_MAX_AGE_ENV = "EDGEWARN_RAP_MAX_AGE_MINUTES"


def get_rap_max_age_minutes() -> int:
    """Return the configured maximum RAP analysis age."""
    raw_value = os.environ.get(RAP_MAX_AGE_ENV)
    if raw_value is None:
        return RAP_MAX_AGE_MINUTES

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{RAP_MAX_AGE_ENV} must be a non-negative integer, got {raw_value!r}"
        ) from exc

    if value < 0:
        raise ValueError(
            f"{RAP_MAX_AGE_ENV} must be a non-negative integer, got {raw_value!r}"
        )
    return value
