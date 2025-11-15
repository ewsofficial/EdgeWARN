from datetime import datetime, timezone


class MRMSBucketParser:
    def __init__(self, dt, region):
        self.dt = dt
        self.region = region
    
    def parse_bucket_path(self, modifier):
        """
        Parse bucket path from region, modifier, and datetime.
        
        Args:
            modifier (str): The modifier/folder name to include in the path
            
        Returns:
            str: Complete bucket path in format: region/modifier/YYYYMMDD/
        """
        # Extract date components from datetime object
        date_str = self.dt.strftime('%Y%m%d')
        
        # Construct the full path
        path = f"{self.region}/{modifier}/{date_str}/"
        
        return path
