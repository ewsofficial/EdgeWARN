import re
from datetime import datetime
from pathlib import Path as PathLibPath

_TIMESTAMP_PATTERNS = [
    re.compile(r"(\d{8}[_\.-]\d{6})"),
    re.compile(r"(\d{8}[_\.-]\d{4})"),
    re.compile(r"(\d{8}-\d{2})"),
    re.compile(r"(\d{8})"),
    re.compile(r"(\d{10,})"),
]


def extract_timestamp_from_filepath(filepath, io_manager, dataset=None):
    filename = PathLibPath(filepath).name

    for pattern in _TIMESTAMP_PATTERNS:
        match = pattern.search(filename)
        if match:
            timestamp_str = match.group(1)

            try:
                if len(timestamp_str) == 15 and ("_" in timestamp_str or "." in timestamp_str or "-" in timestamp_str):
                    date_part, time_part = re.split(r"[_\.-]", timestamp_str)
                    if len(time_part) == 6:
                        return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")
                    if len(time_part) == 4:
                        return datetime.strptime(f"{date_part}{time_part}00", "%Y%m%d%H%M%S")

                elif len(timestamp_str) == 14:
                    return datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

                elif len(timestamp_str) == 11 and "-" in timestamp_str:
                    return datetime.strptime(timestamp_str + "0000", "%Y%m%d-%H%M%S")

                elif len(timestamp_str) == 8:
                    return datetime.strptime(timestamp_str + "000000", "%Y%m%d%H%M%S")

                elif len(timestamp_str) >= 10:
                    return datetime.fromtimestamp(int(timestamp_str[:10]))

            except (ValueError, TypeError) as e:
                io_manager.write_error(f"Could not parse timestamp '{timestamp_str}' from {filename}: {e}")
                continue

    if dataset is not None:
        try:
            time_coords = ["time", "valid_time", "forecast_time", "reference_time"]
            for coord in time_coords:
                if coord in dataset.coords:
                    time_data = dataset[coord].values
                    if len(time_data) > 0:
                        if hasattr(time_data[0], "item"):
                            return datetime.utcfromtimestamp(time_data[0].item() / 1e9)
                        return datetime.utcfromtimestamp(time_data[0] / 1e9)
        except Exception as e:
            io_manager.write_error(f"Could not extract time from dataset: {e}")

    io_manager.write_error(f"Could not find timestamp in filename: {filename}")
    return None
