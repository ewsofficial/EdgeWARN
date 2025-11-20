from datetime import datetime, timezone


class MRMSBucketParser:
    def __init__(self, dt):
        self.dt = dt
    
    def parse_bucket_path(self, region, modifier):
        """
        Parse bucket path from region, modifier, and datetime.
        
        Args:
            modifier (str): The modifier/folder name to include in the path
            
        Returns:
            str: Complete bucket path in format: region/modifier/YYYYMMDD/
        """
        # Extract date components from datetime object
        date_str = self.dt.strftime('%Y%m%d')

        # Handle if modifier is none
        if modifier is None:
            path = f"{region}/{date_str}/"
            return path
        
        # Construct the full path
        path = f"{region}/{modifier}/{date_str}/"
        
        return path

class GOESBucketParser:
    def __init__(self, dt):
        self.dt = dt
    
    def parse_bucket_path(self, product, hour_offset=0):
        """
        Parse GOES bucket path from product and datetime.
        GOES format: product/YYYY/DDD/HH/
        
        Args:
            product (str): GOES product name (e.g., "GLM-L2-LCFA", "ABI-L2-ACHAC")
            hour_offset (int): Hours to subtract from dt (for looking back in time)
            
        Returns:
            str: Complete bucket path in GOES format: product/YYYY/DDD/HH/
        """
        from datetime import timedelta
        
        # Apply hour offset
        adjusted_dt = self.dt - timedelta(hours=hour_offset)
        
        # Extract date components
        year = adjusted_dt.strftime("%Y")
        day_of_year = adjusted_dt.strftime("%j")  # Julian day (001-366)
        hour = adjusted_dt.strftime("%H")
        
        # Construct the path
        path = f"{product}/{year}/{day_of_year}/{hour}/"
        
        return path

