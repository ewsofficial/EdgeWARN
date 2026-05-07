from common.ingest.nexrad.models import SweepInfo
from common.ingest.nexrad.sweep_classifier import classify_sweeps


def _sweep(index, angle, waveform="surveillance"):
    return SweepInfo(index, f"/sweep_{index:02d}", angle, waveform, 720, True, False, "excluded")


def test_classify_sweeps_for_vcp_212_style_bins():
    sweeps = [_sweep(0, 0.5), _sweep(1, 0.87), _sweep(2, 1.3), _sweep(3, 1.8), _sweep(4, 2.4), _sweep(5, 3.1), _sweep(6, 4.0)]
    classified = classify_sweeps(sweeps, dynamic_scan_type="standard")

    assert [sweep.bucket for sweep in classified[:2]] == ["low", "low"]
    assert all(sweep.bucket == "high" for sweep in classified[2:])


def test_classify_sweeps_excludes_sails_low_repeat_after_higher_tilts():
    sweeps = [_sweep(0, 0.48), _sweep(1, 0.88), _sweep(2, 1.2), _sweep(3, 1.8), _sweep(4, 0.5)]
    classified = classify_sweeps(sweeps, dynamic_scan_type="SAILS x 1")

    assert classified[0].bucket == "low"
    assert classified[1].bucket == "low"
    assert classified[4].bucket == "excluded"
    assert classified[4].supplemental is True


def test_classify_sweeps_excludes_mrle_style_repeated_low_bins():
    sweeps = [_sweep(0, 0.5), _sweep(1, 0.9), _sweep(2, 1.2), _sweep(3, 0.52), _sweep(4, 0.88)]
    classified = classify_sweeps(sweeps, dynamic_scan_type="MRLE")

    assert [sweep.bucket for sweep in classified] == ["low", "low", "high", "excluded", "excluded"]
    assert classified[3].supplemental is True
    assert classified[4].supplemental is True


def test_classify_sweeps_deduplicates_elevations_within_tolerance():
    sweeps = [_sweep(0, 0.49), _sweep(1, 0.95), _sweep(2, 0.54), _sweep(3, 1.25)]
    classified = classify_sweeps(sweeps, dynamic_scan_type="standard")

    assert [sweep.bucket for sweep in classified] == ["low", "low", "excluded", "high"]
