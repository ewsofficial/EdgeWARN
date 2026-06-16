from dataclasses import dataclass
from pathlib import Path

import util.file as fs

bucket = "noaa-mrms-pds"
goes_bucket = "noaa-goes19"


@dataclass(frozen=True)
class GoesIngestSpec:
    product: str
    outdir: Path
    channel_id: str | None = None
    channel_name: str | None = None
    filename_matcher: str | None = None
    max_files: int = 2

    @property
    def label(self) -> str:
        return self.channel_name or self.channel_id or self.product

    @property
    def is_glm(self) -> bool:
        return self.channel_id is None and "GLM" in self.product


ABI_RADC_PRODUCT = "ABI-L1b-RadC"

_ABI_CHANNEL_DEFINITIONS = [
    ("C01", "visible_blue", "GOES_ABI_VISIBLE_BLUE_DIR"),
    ("C02", "visible_red", "GOES_ABI_VISIBLE_RED_DIR"),
    ("C03", "veggie", "GOES_ABI_VEGGIE_DIR"),
    ("C04", "cirrus", "GOES_ABI_CIRRUS_DIR"),
    ("C05", "snow_ice", "GOES_ABI_SNOW_ICE_DIR"),
    ("C06", "particle_size", "GOES_ABI_PARTICLE_SIZE_DIR"),
    ("C07", "shortwave_ir", "GOES_ABI_SHORTWAVE_IR_DIR"),
    ("C08", "upper_level_wv", "GOES_ABI_UPPER_LEVEL_WV_DIR"),
    ("C09", "mid_level_wv", "GOES_ABI_MID_LEVEL_WV_DIR"),
    ("C10", "lower_level_wv", "GOES_ABI_LOWER_LEVEL_WV_DIR"),
    ("C11", "cld_top_phase", "GOES_ABI_CLD_TOP_PHASE_DIR"),
    ("C12", "ozone", "GOES_ABI_OZONE_DIR"),
    ("C13", "clean_lwir", "GOES_ABI_CLEAN_LWIR_DIR"),
    ("C14", "longwave_ir", "GOES_ABI_LONGWAVE_IR_DIR"),
    ("C15", "dirty_lwir", "GOES_ABI_DIRTY_LWIR_DIR"),
    ("C16", "co2_lwir", "GOES_ABI_CO2_LWIR_DIR"),
]

DEFAULT_ABI_RADC_CHANNEL_IDS = tuple(channel_id for channel_id, _, _ in _ABI_CHANNEL_DEFINITIONS)

def get_mrms_modifiers():
    return [
        ("CONUS", "EchoTop_18_00.50", fs.MRMS_ECHOTOP18_DIR), # Region / Product / Outdir
        ("CONUS", "EchoTop_30_00.50", fs.MRMS_ECHOTOP30_DIR),
        ("CONUS", "EchoTop_50_00.50", fs.MRMS_ECHOTOP50_DIR),
        ("CONUS", "FLASH_CREST_MAXUNITSTREAMFLOW_00.00", fs.MRMS_FLASH_CREST_MAXUNIT_DIR),
        ("CONUS", "FLASH_QPE_ARIMAX_00.00", fs.MRMS_FLASH_ARIMAX_DIR),
        ("CONUS", "FLASH_QPE_ARI30M_00.00", fs.MRMS_FLASH_ARI30M_DIR),
        ("CONUS", "FLASH_QPE_ARI01H_00.00", fs.MRMS_FLASH_ARI01H_DIR),
        ("CONUS", "FLASH_HP_MAXUNITSTREAMFLOW_00.00", fs.MRMS_FLASH_HP_MAXUNIT_DIR),
        ("CONUS", "FLASH_SAC_MAXSOILSAT_00.00", fs.MRMS_FLASH_SAC_MAXSOIL_DIR),
        ("CONUS", "FLASH_QPE_FFGMAX_00.00", fs.MRMS_FLASH_FFGMAX_DIR),
        ("CONUS", "RadarQualityIndex_00.00", fs.MRMS_RQI_DIR),
        ("CONUS", "MESH_00.50", fs.MRMS_MESH_DIR),
        ("CONUS", "NLDN_CG_005min_AvgDensity_00.00", fs.MRMS_NLDN_DIR),
        ("CONUS", "PrecipRate_00.00", fs.MRMS_PRECIPRATE_DIR),
        ("CONUS", "RadarOnly_QPE_01H_00.00", fs.MRMS_QPE_DIR),
        ("CONUS", "MergedAzShear_0-2kmAGL_00.50", fs.MRMS_AZSHEARLOW_DIR),
        ("CONUS", "MergedAzShear_3-6kmAGL_00.50", fs.MRMS_AZSHEARMID_DIR),
        ("CONUS", "VIL_Density_00.50", fs.MRMS_DVIL_DIR),
        ("ProbSevere", None, fs.MRMS_PROBSEVERE_DIR),
        ("CONUS", "MergedRhoHV_00.50", fs.MRMS_RHOHV_DIR),
        ("CONUS", "PrecipFlag_00.00", fs.MRMS_PRECIPTYP_DIR),
        ("CONUS", "MergedReflectivityAtLowestAltitude_00.50", fs.MRMS_RALA_DIR),
        ("CONUS", "MergedReflectivityQCComposite_00.50", fs.MRMS_COMPOSITE_DIR),
        ("CONUS", "VII_00.50", fs.MRMS_VII_DIR),
        ("CONUS", "VIL_00.50", fs.MRMS_VIL_DIR),
        ("CONUS", "Reflectivity_0C_00.50", fs.MRMS_REF_0C_DIR),
        ("CONUS", "Reflectivity_-5C_00.50", fs.MRMS_REFM5C_DIR),
        ("CONUS", "Reflectivity_-15C_00.50", fs.MRMS_REFM15C_DIR)
    ]

def get_check_modifiers():
    return [
        ("CONUS", "EchoTop_18_00.50", fs.MRMS_ECHOTOP18_DIR), # Region / Product / Outdir
        ("CONUS", "EchoTop_30_00.50", fs.MRMS_ECHOTOP30_DIR),
        ("CONUS", "EchoTop_50_00.50", fs.MRMS_ECHOTOP50_DIR),
        ("CONUS", "PrecipRate_00.00", fs.MRMS_PRECIPRATE_DIR),
        ("CONUS", "MergedAzShear_0-2kmAGL_00.50", fs.MRMS_AZSHEARLOW_DIR),
        ("CONUS", "MergedAzShear_3-6kmAGL_00.50", fs.MRMS_AZSHEARMID_DIR),
        ("CONUS", "VIL_Density_00.50", fs.MRMS_DVIL_DIR),
        ("ProbSevere", None, fs.MRMS_PROBSEVERE_DIR),
        ("CONUS", "PrecipFlag_00.00", fs.MRMS_PRECIPTYP_DIR),
        ("CONUS", "MergedReflectivityAtLowestAltitude_00.50", fs.MRMS_RALA_DIR),
        ("CONUS", "MergedReflectivityQCComposite_00.50", fs.MRMS_COMPOSITE_DIR),
        ("CONUS", "VII_00.50", fs.MRMS_VII_DIR)
    ]


def get_goes_modifiers():
    return [
        GoesIngestSpec("GLM-L2-LCFA", fs.GOES_GLM_DIR),
        *get_abi_radc_channel_specs(),
    ]


def get_abi_radc_channel_specs(channel_ids=DEFAULT_ABI_RADC_CHANNEL_IDS):
    selected_channel_ids = set(channel_ids) if channel_ids is not None else None
    specs = []

    for channel_id, channel_name, outdir_attr in _ABI_CHANNEL_DEFINITIONS:
        if selected_channel_ids is not None and channel_id not in selected_channel_ids:
            continue

        outdir = getattr(fs, outdir_attr)

        specs.append(
            GoesIngestSpec(
                product=ABI_RADC_PRODUCT,
                outdir=outdir,
                channel_id=channel_id,
                channel_name=channel_name,
                filename_matcher=rf"(?:_|-)M\d{channel_id}_",
            )
        )

    return specs


def normalize_goes_modifier(spec):
    if isinstance(spec, GoesIngestSpec):
        return spec

    if isinstance(spec, tuple) and len(spec) == 2:
        product, outdir = spec
        return GoesIngestSpec(product=product, outdir=outdir)

    raise TypeError(f"Unsupported GOES modifier specification: {spec!r}")
