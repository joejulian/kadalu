"""CSI volume lookup behavior tests."""

import importlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _load_volumeutils(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    sys.modules.pop("volumeutils", None)
    return importlib.import_module("volumeutils")


def test_search_volume_checks_every_hosting_pool(monkeypatch, tmp_path):
    volumeutils = _load_volumeutils(monkeypatch)
    volume_id = "pvc-bellagio-vault"
    hosting_volumes = [
        {"name": "mirage-pool", "type": "Replica1"},
        {"name": "bellagio-pool", "type": "Replica3"},
    ]
    mounted = []

    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(tmp_path))
    monkeypatch.setattr(
        volumeutils,
        "get_pv_hosting_volumes",
        lambda _filters: hosting_volumes,
    )
    monkeypatch.setattr(
        volumeutils,
        "mount_glusterfs",
        lambda volume, mountpoint: mounted.append((volume["name"], mountpoint)),
    )
    monkeypatch.setattr(
        volumeutils,
        "retry_errors",
        lambda function, args, _errors: function(*args),
    )

    volume_hash = volumeutils.get_volname_hash(volume_id)
    volume_path = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volume_hash,
        volume_id,
    )
    first_pool = tmp_path / "mirage-pool"
    second_pool = tmp_path / "bellagio-pool"
    first_pool.mkdir()
    metadata = second_pool / "info" / f"{volume_path}.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(
        json.dumps({"size": 10485760, "single_pv_per_pool": True}),
        encoding="utf-8",
    )

    found = volumeutils.search_volume(volume_id)

    assert found is not None
    assert found.volname == volume_id
    assert found.hostvol == "bellagio-pool"
    assert found.voltype == volumeutils.PV_TYPE_SUBVOL
    assert found.single_pv_per_pool is True
    assert mounted == [
        ("mirage-pool", str(first_pool)),
        ("bellagio-pool", str(second_pool)),
    ]
