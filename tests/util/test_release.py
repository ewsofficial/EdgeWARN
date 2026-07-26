import json
from pathlib import Path

from util.release import get_release_version


def test_get_release_version_matches_package_json():
    package_json_path = Path(__file__).resolve().parents[2] / "package.json"
    expected = json.loads(package_json_path.read_text())["version"]
    assert get_release_version() == expected
