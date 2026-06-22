from util.release import get_release_version


def test_get_release_version_matches_package_json():
    assert get_release_version() == "2.6.4"
