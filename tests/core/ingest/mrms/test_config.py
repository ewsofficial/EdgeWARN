from common.ingest.mrms.config import (
    ABI_RADC_PRODUCT,
    DEFAULT_ABI_RADC_CHANNEL_IDS,
    GoesIngestSpec,
    get_abi_radc_channel_specs,
    get_goes_modifiers,
    normalize_goes_modifier,
)


def test_get_abi_radc_channel_specs_includes_all_channels():
    specs = get_abi_radc_channel_specs(channel_ids=None)

    assert [spec.channel_id for spec in specs] == [f"C{i:02d}" for i in range(1, 17)]
    assert all(spec.product == ABI_RADC_PRODUCT for spec in specs)
    assert all(spec.filename_matcher == rf"(?:_|-)M\d{spec.channel_id}_" for spec in specs)


def test_get_goes_modifiers_defaults_to_glm_plus_all_abi_channels():
    specs = get_goes_modifiers()

    assert specs[0] == GoesIngestSpec("GLM-L2-LCFA", specs[0].outdir)
    assert [spec.channel_id for spec in specs[1:]] == list(DEFAULT_ABI_RADC_CHANNEL_IDS)


def test_normalize_goes_modifier_supports_legacy_tuple(tmp_path):
    spec = normalize_goes_modifier(("GLM-L2-LCFA", tmp_path / "GLM"))

    assert spec == GoesIngestSpec("GLM-L2-LCFA", tmp_path / "GLM")
