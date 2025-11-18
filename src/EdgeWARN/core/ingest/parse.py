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
