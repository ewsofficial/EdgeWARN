from datetime import datetime, timezone, timedelta


def parse_mrms_bucket_path(dt, region, modifier):
    """
    Parse bucket path from region, modifier, and datetime.
    
    Args:
        dt (datetime): The datetime object to use for the path
        region (str): The region name
        modifier (str): The modifier/folder name to include in the path
        
    Returns:
        str: Complete bucket path in format: region/modifier/YYYYMMDD/
    """
    # Extract date components from datetime object
    date_str = dt.strftime('%Y%m%d')

    # Handle if modifier is none
    if modifier is None:
        path = f"{region}/{date_str}/"
        return path
    
    # Construct the full path
    path = f"{region}/{modifier}/{date_str}/"
    
    return path


def parse_goes_bucket_path(dt, product, hour_offset=0):
    """
    Parse GOES bucket path from product and datetime.
    GOES format: product/YYYY/DDD/HH/
    
    Args:
        dt (datetime): The datetime object to use for the path
        product (str): GOES product name (e.g., "GLM-L2-LCFA", "ABI-L2-ACHAC")
        hour_offset (int): Hours to subtract from dt (for looking back in time)
        
    Returns:
        str: Complete bucket path in GOES format: product/YYYY/DDD/HH/
    """
    
    # Apply hour offset
    adjusted_dt = dt - timedelta(hours=hour_offset)
    
    # Extract date components
    year = adjusted_dt.strftime("%Y")
    day_of_year = adjusted_dt.strftime("%j")  # Julian day (001-366)
    hour = adjusted_dt.strftime("%H")
    
    # Construct the path
    path = f"{product}/{year}/{day_of_year}/{hour}/"
    
    return path

