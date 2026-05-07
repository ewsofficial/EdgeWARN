from common.ingest.nexrad.models import SweepInfo
from common.ingest.nexrad.sweep_classifier import classify_sweeps


def _sweep(index, angle, waveform="surveillance"):
    return SweepInfo(index, f"/sweep_{index:02d}", angle, waveform, 720, True, False, "excluded")


def test_classify_sweeps_for_vcp_212_style_bins():
    sweeps = [
        _sweep(0, 0.5, "contiguous_surveillance"),
        _sweep(1, 0.5, "contiguous_doppler"),
        _sweep(2, 0.87, "contiguous_surveillance"),
        _sweep(3, 0.87, "contiguous_doppler"),
        _sweep(4, 1.3),
        _sweep(5, 1.8),
        _sweep(6, 2.4),
        _sweep(7, 3.1),
        _sweep(8, 4.0),
    ]
    classified = classify_sweeps(sweeps, dynamic_scan_type="standard")

    assert [sweep.bucket for sweep in classified[:4]] == ["low", "low", "low", "low"]
    assert all(sweep.bucket == "high" for sweep in classified[4:])


def test_classify_sweeps_keeps_paired_low_waveforms_but_excludes_true_repeats():
    sweeps = [
        _sweep(0, 0.5, "contiguous_surveillance"),
        _sweep(1, 0.5, "contiguous_doppler"),
        _sweep(2, 0.52, "contiguous_surveillance"),
        _sweep(3, 0.52, "contiguous_doppler"),
    ]
    classified = classify_sweeps(sweeps, dynamic_scan_type="standard")

    assert [sweep.bucket for sweep in classified] == ["low", "low", "excluded", "excluded"]


def test_classify_sweeps_excludes_sails_low_repeat_after_higher_tilts():
    sweeps = [
        _sweep(0, 0.48, "contiguous_surveillance"),
        _sweep(1, 0.48, "contiguous_doppler"),
        _sweep(2, 0.88, "contiguous_surveillance"),
        _sweep(3, 0.88, "contiguous_doppler"),
        _sweep(4, 1.2),
        _sweep(5, 1.8),
        _sweep(6, 0.5, "contiguous_surveillance"),
        _sweep(7, 0.5, "contiguous_doppler"),
    ]
    classified = classify_sweeps(sweeps, dynamic_scan_type="SAILS x 1")

    assert classified[0].bucket == "low"
    assert classified[1].bucket == "low"
    assert classified[6].bucket == "excluded"
    assert classified[7].bucket == "excluded"
    assert classified[6].supplemental is True
    assert classified[7].supplemental is True


def test_classify_sweeps_excludes_mrle_style_repeated_low_bins():
    sweeps = [
        _sweep(0, 0.5, "contiguous_surveillance"),
        _sweep(1, 0.5, "contiguous_doppler"),
        _sweep(2, 0.9, "contiguous_surveillance"),
        _sweep(3, 0.9, "contiguous_doppler"),
        _sweep(4, 1.2),
        _sweep(5, 0.52, "contiguous_surveillance"),
        _sweep(6, 0.88, "contiguous_doppler"),
    ]
    classified = classify_sweeps(sweeps, dynamic_scan_type="MRLE")

    assert [sweep.bucket for sweep in classified] == ["low", "low", "low", "low", "high", "excluded", "excluded"]
    assert classified[5].supplemental is True
    assert classified[6].supplemental is True


def test_classify_sweeps_deduplicates_elevations_within_tolerance():
    sweeps = [
        _sweep(0, 0.49, "contiguous_surveillance"),
        _sweep(1, 0.49, "contiguous_doppler"),
        _sweep(2, 0.54, "contiguous_surveillance"),
        _sweep(3, 1.25),
    ]
    classified = classify_sweeps(sweeps, dynamic_scan_type="standard")

    assert [sweep.bucket for sweep in classified] == ["low", "low", "excluded", "high"]
