from common.ingest.nexrad.config import ANGLE_DEDUP_TOLERANCE_DEG, HIGH_MAX_ANGLE_DEG, LOW_BINS, LOW_MAX_ANGLE_DEG
from common.ingest.nexrad.models import SweepInfo


def _closest_bin(angle: float, bins, tolerance=ANGLE_DEDUP_TOLERANCE_DEG):
    candidates = [target for target in bins if abs(angle - target) <= tolerance]
    if not candidates:
        return None
    return min(candidates, key=lambda target: abs(angle - target))


def classify_sweeps(sweeps: list[SweepInfo], *, dynamic_scan_type=None):
    seen_low_bins = set()
    seen_high_tilt = False
    dynamic_scan_type = (dynamic_scan_type or "").upper()
    repeated_low_is_supplemental = any(token in dynamic_scan_type for token in ("SAILS", "MRLE", "MESO"))
    classified = []

    for sweep in sorted(sweeps, key=lambda item: item.index):
        low_bin = _closest_bin(sweep.fixed_angle, LOW_BINS)
        bucket = "excluded"
        supplemental = False

        if low_bin is not None:
            if low_bin not in seen_low_bins:
                bucket = "low"
                seen_low_bins.add(low_bin)
            elif seen_high_tilt or repeated_low_is_supplemental or sweep.fixed_angle <= LOW_MAX_ANGLE_DEG:
                bucket = "excluded"
                supplemental = True
        elif LOW_MAX_ANGLE_DEG < sweep.fixed_angle <= HIGH_MAX_ANGLE_DEG:
            bucket = "high"
            seen_high_tilt = True
        else:
            bucket = "excluded"

        classified.append(
            SweepInfo(
                index=sweep.index,
                group_name=sweep.group_name,
                fixed_angle=sweep.fixed_angle,
                waveform=sweep.waveform,
                azimuth_count=sweep.azimuth_count,
                complete=sweep.complete,
                supplemental=supplemental,
                bucket=bucket,
            )
        )
    return classified


def canonical_angle_matches(angle: float, target: float, tolerance=ANGLE_DEDUP_TOLERANCE_DEG):
    return abs(angle - target) <= tolerance
