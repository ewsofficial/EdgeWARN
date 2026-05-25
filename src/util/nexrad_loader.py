from __future__ import annotations

from pathlib import Path

import util.file as fs


def open_nexrad_level2_datatree(path: str | Path):
    from common.ingest.nexrad.parser import open_partial_volume

    return open_partial_volume(path)


def open_nexrad_artifact_datatree(*, artifact_path: str | Path, site: str, volume_id: str):
    artifact_path = Path(artifact_path)
    try:
        return open_nexrad_level2_datatree(artifact_path)
    except Exception:
        runtime_path = (
            Path(fs.NEXRAD_LEVEL2_DIR)
            / ".runtime"
            / str(site).upper()
            / f"{str(site).upper()}_{volume_id}.ar2v"
        )
        if not runtime_path.exists():
            raise
        return open_nexrad_level2_datatree(runtime_path)
