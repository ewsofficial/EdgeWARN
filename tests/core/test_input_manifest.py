from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from common.ingest.manifest import (
    CycleInputManifest,
    StagedInput,
    staged_input_from_path,
)
from common.ingest.mrms.downloader import DownloadBatchResult
from EdgeWARN import pipeline as edgewarn_pipeline
from EdgeWARN.process.integrate import pipeline as integration_pipeline


def _record(
    tmp_path,
    product,
    timestamp,
    *,
    family="mrms",
    role="current",
    directory=None,
):
    root = directory or (tmp_path / product)
    root.mkdir(parents=True, exist_ok=True)
    if family == "goes":
        path = root / (
            "OR_ABI-L1b-RadC-M6C01_G19_"
            f"s{timestamp:%Y}{timestamp.timetuple().tm_yday:03d}{timestamp:%H%M%S}0.nc"
        )
    else:
        path = root / f"MRMS_{product}_{timestamp:%Y%m%d-%H%M%S}.grib2"
    path.write_bytes(b"data")
    return staged_input_from_path(
        product,
        path,
        source="test",
        family=family,
        role=role,
    )


def test_cycle_input_manifest_round_trips_and_is_immutable(tmp_path):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    record = _record(tmp_path, "Composite", cycle_time)
    manifest = CycleInputManifest(cycle_time=cycle_time, inputs=(record,))

    restored = CycleInputManifest.from_dict(manifest.as_dict())

    assert restored == manifest
    assert restored.validate_alignment() == ()
    with pytest.raises(AttributeError):
        restored.inputs = ()


def test_download_batch_requires_structured_staged_records():
    with pytest.raises(TypeError, match="StagedInput"):
        DownloadBatchResult(
            attempted=("Composite",),
            downloaded=("Composite",),
            failed=(),
        )


def test_cycle_input_manifest_rejects_cross_timestamp_current_input(tmp_path):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    record = _record(
        tmp_path,
        "Composite",
        cycle_time + timedelta(minutes=3),
    )
    manifest = CycleInputManifest(cycle_time=cycle_time, inputs=(record,))

    assert "Composite" in manifest.validate_alignment()[0]


def test_cycle_input_manifest_allows_normal_mrms_publication_lag(tmp_path):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    record = _record(
        tmp_path,
        "MergedRhoHV_00.50",
        cycle_time - timedelta(seconds=200),
    )

    assert (
        CycleInputManifest(cycle_time=cycle_time, inputs=(record,)).validate_alignment()
        == ()
    )


def test_rap_validation_ceiling_follows_the_configured_staging_budget(
    tmp_path, monkeypatch
):
    """Widening the RAP age budget must not turn into a validation failure.

    The downloader walks back through analysis hours until the budget is spent,
    so a raised budget stages an older analysis deliberately. If the manifest
    kept its own ceiling, that file would be staged and then rejected.
    """
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    record = _record(
        tmp_path,
        "RAP",
        cycle_time - timedelta(minutes=200),
        family="rap",
    )

    assert CycleInputManifest(
        cycle_time=cycle_time, inputs=(record,)
    ).validate_alignment() != ()

    monkeypatch.setenv("EDGEWARN_RAP_MAX_AGE_MINUTES", "240")

    assert CycleInputManifest(
        cycle_time=cycle_time, inputs=(record,)
    ).validate_alignment() == ()


def test_detection_uses_pinned_frames_after_newer_file_arrives(tmp_path):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    products = (
        "MergedReflectivityQCComposite_00.50",
        "ProbSevere",
        "PrecipFlag_00.00",
    )
    records = []
    for product in products:
        directory = tmp_path / product
        records.extend(
            (
                _record(
                    tmp_path,
                    product,
                    cycle_time - timedelta(minutes=2),
                    role="previous",
                    directory=directory,
                ),
                _record(
                    tmp_path,
                    product,
                    cycle_time,
                    directory=directory,
                ),
            )
        )
        _record(
            tmp_path,
            product,
            cycle_time + timedelta(minutes=2),
            directory=directory,
        )

    manifest = CycleInputManifest(cycle_time=cycle_time, inputs=tuple(records))
    selected = edgewarn_pipeline._prepare_realtime_detection_inputs(
        lambda _message: None,
        manifest,
    )

    assert all("180200" not in str(path) for path in selected)
    assert [Path(path).name for path in selected[:2]] == [
        "MRMS_MergedReflectivityQCComposite_00.50_20260726-175800.grib2",
        "MRMS_MergedReflectivityQCComposite_00.50_20260726-180000.grib2",
    ]
    assert all(path is None or isinstance(path, str) for path in selected)


def test_integration_uses_manifest_path_instead_of_newer_directory_file(tmp_path):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    directory = tmp_path / "RAP"
    pinned = _record(
        tmp_path,
        "RAP",
        cycle_time,
        directory=directory,
    )
    _record(
        tmp_path,
        "RAP",
        cycle_time + timedelta(minutes=2),
        directory=directory,
    )
    manifest = CycleInputManifest(cycle_time=cycle_time, inputs=(pinned,))

    selected = integration_pipeline._selected_input_path(directory, manifest)

    assert selected == pinned.local_path


def test_integration_converts_pinned_path_at_legacy_stats_boundary(tmp_path, monkeypatch):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    directory = tmp_path / "Ref0"
    pinned = _record(tmp_path, "Ref0", cycle_time, directory=directory)
    manifest = CycleInputManifest(cycle_time=cycle_time, inputs=(pinned,))
    integrator = MagicMock()
    integrator.integrate_multi_stats.side_effect = lambda _path, cells, *_args, **_kwargs: cells
    monkeypatch.setattr(
        integration_pipeline,
        "get_datasets_config",
        lambda: [{"filepath": directory, "name": "Ref0", "key": "Ref0"}],
    )

    integration_pipeline._integrate_dataset_groups(integrator, [{"id": 1}], manifest)

    assert integrator.integrate_multi_stats.call_args.args[0] == str(pinned.local_path)


def test_manifest_selects_exact_goes_path_by_directory(tmp_path):
    cycle_time = datetime(2026, 7, 26, 18, 0, tzinfo=timezone.utc)
    directory = tmp_path / "C01"
    record = _record(
        tmp_path,
        "visible_blue",
        cycle_time,
        family="goes",
        directory=directory,
    )
    manifest = CycleInputManifest(cycle_time=cycle_time, inputs=(record,))

    assert manifest.latest_for_directory(directory) == record
    assert isinstance(manifest.inputs[0], StagedInput)
