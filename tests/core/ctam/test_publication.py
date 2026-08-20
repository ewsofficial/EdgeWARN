from __future__ import annotations

import json
import os

import pytest

from EdgeWARN.ctam.publication import CTAMPublicationCoordinator


def test_publication_replaces_all_payloads_before_indexes(tmp_path):
    snapshot, history, journal = tmp_path / "stormcells.json", tmp_path / "7.json", tmp_path / "journals"
    snapshot.write_text('{"old":true}'); history.write_text('[]')
    seen = []
    path = CTAMPublicationCoordinator(journal).publish({snapshot: {"features": [1]}, history: [{"id": 7}]}, publish_indexes=lambda: seen.append((json.loads(snapshot.read_text()), json.loads(history.read_text()))), transaction_id="cycle")
    assert seen == [({"features": [1]}, [{"id": 7}])]
    assert json.loads(path.read_text())["state"] == "committed"


def test_recovery_rolls_forward_after_fault_between_replacements(tmp_path):
    first, second, journal = tmp_path / "first.json", tmp_path / "second.json", tmp_path / "journals"
    calls = 0
    def fail_second(source, destination):
        nonlocal calls; calls += 1
        if calls == 2: raise OSError("injected process death")
        os.replace(source, destination)
    coordinator = CTAMPublicationCoordinator(journal, replace=fail_second)
    with pytest.raises(OSError): coordinator.publish({first: {"v": 1}, second: {"v": 2}}, transaction_id="fault")
    assert json.loads(first.read_text()) == {"v": 1}
    CTAMPublicationCoordinator(journal).recover()
    assert json.loads(second.read_text()) == {"v": 2}
    assert json.loads((journal / "fault.json").read_text())["state"] == "committed"


def test_recovery_quarantines_journal_when_remaining_part_is_corrupt(tmp_path):
    target, journal = tmp_path / "target.json", tmp_path / "journals"
    coordinator = CTAMPublicationCoordinator(journal, replace=lambda _source, _target: (_ for _ in ()).throw(OSError("stop")))
    with pytest.raises(OSError): coordinator.publish({target: {"v": 1}}, transaction_id="bad")
    part = next(tmp_path.glob(".target.json.bad.ctam-part")); part.write_text("not json")
    assert CTAMPublicationCoordinator(journal).recover() == []
    assert (journal / "quarantine" / "bad.json").exists()
