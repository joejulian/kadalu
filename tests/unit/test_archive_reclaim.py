"""Crash-safe archive reclaim behavior for the CSI volume store."""

import errno
import importlib
import json
import sys
import threading
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
POOL_NAME = "bellagio-archive-pool"
VOLUME_ID = "pvc-linus-caldwell"
VOLUME_SIZE = 11 * 1024 * 1024


def _load_volumeutils(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    sys.modules.pop("volumeutils", None)
    return importlib.import_module("volumeutils")


def _load_cleanup_module():
    """Load the CSI cleanup script without shadowing the CLI module."""
    module_name = "kadalu_csi_remove_archived_pv"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(
        module_name,
        ROOT / "csi" / "remove_archived_pv.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_active_volume(env, payload_text="The Bellagio vault is fake"):
    payload = env["pool_root"] / env["volpath"]
    payload.mkdir(parents=True)
    (payload / "crew-ledger.txt").write_text(payload_text, encoding="utf-8")
    metadata_path = env["pool_root"] / "info" / f"{env['volpath']}.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps({
            "size": VOLUME_SIZE,
            "path_prefix": str(Path(env["volpath"]).parent),
        }),
        encoding="utf-8",
    )
    env["accounting"][VOLUME_ID] = VOLUME_SIZE
    return payload, metadata_path


def _configure_archive_pool(monkeypatch, tmp_path, policy="archive"):
    volumeutils = _load_volumeutils(monkeypatch)
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / POOL_NAME
    pool_root.mkdir(parents=True)
    volinfo_dir = tmp_path / "volinfo"
    volinfo_dir.mkdir()
    (volinfo_dir / f"{POOL_NAME}.info").write_text(
        json.dumps({
            "volname": POOL_NAME,
            "type": "Replica1",
            "pvReclaimPolicy": policy,
        }),
        encoding="utf-8",
    )
    pool = {
        "name": POOL_NAME,
        "type": "Replica1",
        "single_pv_per_pool": False,
    }
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(volumeutils, "VOLINFO_DIR", str(volinfo_dir))
    monkeypatch.setattr(
        volumeutils,
        "get_pv_hosting_volumes",
        lambda *_args, **_kwargs: [pool.copy()],
    )
    monkeypatch.setattr(
        volumeutils,
        "mount_glusterfs",
        lambda _volume, mountpoint, *_args: mountpoint,
    )
    monkeypatch.setattr(
        volumeutils,
        "retry_errors",
        lambda function, args, _errors: function(*args),
    )
    accounting = {}
    real_archive_reservation = volumeutils.archive_pv_reservation

    def archive_reservation(_pool, old_name, archived_name, size):
        accounting.pop(old_name, None)
        accounting[archived_name] = size

    monkeypatch.setattr(
        volumeutils,
        "archive_pv_reservation",
        archive_reservation,
    )
    volhash = volumeutils.get_volname_hash(VOLUME_ID)
    volpath = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volhash,
        VOLUME_ID,
    )
    env = {
        "volumeutils": volumeutils,
        "pool_root": pool_root,
        "volpath": volpath,
        "accounting": accounting,
        "real_archive_reservation": real_archive_reservation,
    }
    _write_active_volume(env)
    return env


def _archive_records(env):
    archive_info = env["pool_root"] / "info" / "archive"
    return sorted(archive_info.glob("archived-*.json"))


def _write_hashed_metadata(
        volumeutils, pool_root, stored_name, *, hashed_for=None, **values):
    """Write one metadata record under the hash layout used by old CSI."""
    hashed_for = stored_name if hashed_for is None else hashed_for
    volume_hash = volumeutils.get_volname_hash(hashed_for)
    prefix = Path(volumeutils.PV_TYPE_SUBVOL) / volume_hash[:2] / volume_hash[2:4]
    metadata_path = pool_root / "info" / prefix / f"{stored_name}.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps({
            "path_prefix": str(prefix),
            "size": VOLUME_SIZE,
            **values,
        }),
        encoding="utf-8",
    )
    return metadata_path


def test_archive_uses_unique_identity_and_is_not_listed_as_active(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]

    volumeutils.delete_volume(VOLUME_ID)

    records = _archive_records(env)
    assert len(records) == 1
    archived_name = records[0].stem
    archived = json.loads(records[0].read_text(encoding="utf-8"))
    assert archived_name.startswith(volumeutils.ARCHIVE_PREFIX)
    assert archived == {
        "archive_name": archived_name,
        "original_volume_id": VOLUME_ID,
        "path_prefix": "archive",
        "size": VOLUME_SIZE,
        "state": "archived",
    }
    assert (env["pool_root"] / "archive" / archived_name
            / "crew-ledger.txt").is_file()
    assert env["accounting"] == {archived_name: VOLUME_SIZE}
    assert list(volumeutils.yield_pvc_from_hostvol()) == []
    cleanup_records = [
        record for record in volumeutils.yield_pvc_from_mntdir(
            str(env["pool_root"] / "info")
        )
        if record is not None
    ]
    assert [record["name"] for record in cleanup_records] == [archived_name]


def test_archive_metadata_accepts_an_opaque_csi_volume_id(
        monkeypatch, tmp_path):
    volumeutils = _load_volumeutils(monkeypatch)
    pool_root = tmp_path / "bellagio-opaque-archive"
    archived_name = "archived-benedicts-opaque-score"
    original_volume_id = "../bellagio\ncounting-room"
    metadata_path = (
        pool_root / "info" / "archive" / f"{archived_name}.json"
    )
    metadata_path.parent.mkdir(parents=True)
    metadata = {
        "archive_name": archived_name,
        "original_volume_id": original_volume_id,
        "path_prefix": "archive",
        "size": VOLUME_SIZE,
        "state": "archived",
    }
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    details = volumeutils.archive_metadata_details(
        str(pool_root),
        str(metadata_path),
        metadata,
    )

    assert details["original_volume_id"] == original_volume_id
    assert details["payload_path"] == str(
        pool_root / "archive" / archived_name
    )


def test_recreated_volume_gets_new_archive_without_overwrite(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    volumeutils.delete_volume(VOLUME_ID)
    first_record = _archive_records(env)[0]
    first_name = first_record.stem
    first_payload = env["pool_root"] / "archive" / first_name

    _write_active_volume(env, "The second Bellagio vault is also fake")
    volumeutils.delete_volume(VOLUME_ID)

    records = _archive_records(env)
    assert len(records) == 2
    assert first_record in records
    assert (first_payload / "crew-ledger.txt").read_text(
        encoding="utf-8",
    ) == "The Bellagio vault is fake"
    names = {record.stem for record in records}
    assert len(names) == 2
    assert env["accounting"] == {
        name: VOLUME_SIZE for name in names
    }


def test_archive_retry_reuses_identity_after_accounting_failure(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    successful_reservation = volumeutils.archive_pv_reservation
    attempts = []

    def fail_first_reservation(pool, old_name, archived_name, size):
        attempts.append(archived_name)
        if len(attempts) == 1:
            raise OSError("Rusty dropped the fake reservation ledger")
        successful_reservation(pool, old_name, archived_name, size)

    monkeypatch.setattr(
        volumeutils,
        "archive_pv_reservation",
        fail_first_reservation,
    )

    with pytest.raises(OSError):
        volumeutils.delete_volume(VOLUME_ID)
    active_metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"
    prepared = json.loads(active_metadata.read_text(encoding="utf-8"))
    archived_name = prepared["archive_name"]
    assert (env["pool_root"] / "archive" / archived_name).is_dir()

    volumeutils.delete_volume(VOLUME_ID)

    assert attempts == [archived_name, archived_name]
    assert not active_metadata.exists()
    assert [record.stem for record in _archive_records(env)] == [archived_name]
    assert env["accounting"] == {archived_name: VOLUME_SIZE}


def test_prepared_archive_metadata_repairs_post_accounting_crash(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    monkeypatch.setattr(
        volumeutils,
        "archive_pv_reservation",
        env["real_archive_reservation"],
    )
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        accounting.update_summary(10 * VOLUME_SIZE)
        accounting.update_pv_record(VOLUME_ID, VOLUME_SIZE)

    active_metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"
    successful_rename = volumeutils._rename_for_archive

    def interrupt_metadata_commit(source, destination):
        if Path(source) == active_metadata:
            raise OSError("The fake archive ledger did not commit")
        return successful_rename(source, destination)

    monkeypatch.setattr(
        volumeutils,
        "_rename_for_archive",
        interrupt_metadata_commit,
    )

    with pytest.raises(OSError, match="did not commit"):
        volumeutils.delete_volume(VOLUME_ID)

    prepared = json.loads(active_metadata.read_text(encoding="utf-8"))
    archived_name = prepared["archive_name"]
    assert (env["pool_root"] / "archive" / archived_name).is_dir()
    assert _archive_records(env) == []

    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        volumeutils._rebuild_committed_capacity(
            accounting,
            str(env["pool_root"]),
        )
        stats = accounting.get_stats()
        assert accounting.get_pv_size(VOLUME_ID) == 0
        assert accounting.get_pv_size(archived_name) == VOLUME_SIZE
    assert stats["number_of_pvs"] == 1
    assert stats["used_size_bytes"] == VOLUME_SIZE

    monkeypatch.setattr(
        volumeutils,
        "_rename_for_archive",
        successful_rename,
    )
    volumeutils.delete_volume(VOLUME_ID)

    assert not active_metadata.exists()
    assert [record.stem for record in _archive_records(env)] == [archived_name]


def test_prepared_and_committed_archive_metadata_fails_closed_as_duplicate(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    monkeypatch.setattr(
        volumeutils,
        "archive_pv_reservation",
        env["real_archive_reservation"],
    )
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        accounting.update_summary(10 * VOLUME_SIZE)
        accounting.update_pv_record(VOLUME_ID, VOLUME_SIZE)

    volumeutils.delete_volume(VOLUME_ID)

    archived_metadata = _archive_records(env)[0]
    archived = json.loads(archived_metadata.read_text(encoding="utf-8"))
    archived_name = archived["archive_name"]
    active_metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"
    active_metadata.parent.mkdir(parents=True, exist_ok=True)
    prepared = archived.copy()
    prepared.update({
        "path_prefix": str(Path(env["volpath"]).parent),
        "state": "archiving",
    })
    active_metadata.write_text(json.dumps(prepared), encoding="utf-8")

    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        with pytest.raises(ValueError, match="Duplicate PV metadata"):
            volumeutils._rebuild_committed_capacity(
                accounting,
                str(env["pool_root"]),
            )
        assert accounting.get_pv_size(VOLUME_ID) == 0
        assert accounting.get_pv_size(archived_name) == VOLUME_SIZE

    assert active_metadata.exists()
    assert (env["pool_root"] / "archive" / archived_name).exists()


def test_archive_collision_fails_without_overwriting_prior_payload(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    archived_name = "archived-benedicts-existing-score"
    metadata_path = env["pool_root"] / "info" / f"{env['volpath']}.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update({
        "archive_name": archived_name,
        "original_volume_id": VOLUME_ID,
        "state": "archiving",
    })
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    collision = env["pool_root"] / "archive" / archived_name
    collision.mkdir(parents=True)
    sentinel = collision / "benedicts-ledger.txt"
    sentinel.write_text("Do not overwrite this fake score", encoding="utf-8")

    with pytest.raises(FileExistsError):
        volumeutils.delete_volume(VOLUME_ID)

    assert sentinel.read_text(encoding="utf-8") == (
        "Do not overwrite this fake score"
    )
    assert (env["pool_root"] / env["volpath"]).is_dir()
    assert env["accounting"] == {VOLUME_ID: VOLUME_SIZE}


def test_unknown_reclaim_policy_fails_before_touching_data(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(
        monkeypatch,
        tmp_path,
        policy="benedict-keeps-everything",
    )
    volumeutils = env["volumeutils"]
    payload = env["pool_root"] / env["volpath"]
    metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"

    with pytest.raises(volumeutils.UnsupportedReclaimPolicyError):
        volumeutils.delete_volume(VOLUME_ID)

    assert payload.is_dir()
    assert metadata.is_file()
    assert env["accounting"] == {VOLUME_ID: VOLUME_SIZE}


def test_archive_cleanup_deletes_only_selected_identity(monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    volumeutils.delete_volume(VOLUME_ID)
    _write_active_volume(env, "Another entirely fake casino ledger")
    volumeutils.delete_volume(VOLUME_ID)
    records = _archive_records(env)
    selected = records[0]
    sibling = records[1]
    released = []
    cleanup = _load_cleanup_module()
    monkeypatch.setattr(
        cleanup,
        "release_archived_pv_reservation",
        lambda pool, name, original: released.append((pool, name, original)),
    )

    cleanup.delete_archived_pvs(
        POOL_NAME,
        cleanup.get_archived_pvs(POOL_NAME, selected.stem),
    )

    assert released == [(POOL_NAME, selected.stem, VOLUME_ID)]
    assert not selected.exists()
    assert not (env["pool_root"] / "archive" / selected.stem).exists()
    assert sibling.exists()
    assert (env["pool_root"] / "archive" / sibling.stem).is_dir()


def test_active_volume_id_with_archive_prefix_is_not_a_legacy_archive(
        monkeypatch, tmp_path):
    volumeutils = _load_volumeutils(monkeypatch)
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / POOL_NAME
    active_volume_id = "archived-pvc-danny-ocean"
    _write_hashed_metadata(
        volumeutils,
        pool_root,
        active_volume_id,
    )
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))

    reservations, aliases = volumeutils._committed_capacity_records(
        str(pool_root),
    )
    visible = [
        record["name"]
        for record in volumeutils.yield_pvc_from_mntdir(
            str(pool_root / "info"),
            include_archived=False,
        )
        if record is not None
    ]
    cleanup = _load_cleanup_module()

    assert reservations == {active_volume_id: VOLUME_SIZE}
    assert aliases == {}
    assert visible == [active_volume_id]
    assert cleanup.get_archived_pvs(POOL_NAME, None) == {}


@pytest.mark.parametrize(
    ("hashed_for", "expected_aliases"),
    [
        (VOLUME_ID, {f"archived-{VOLUME_ID}": {VOLUME_ID}}),
        (f"archived-{VOLUME_ID}", {}),
    ],
    ids=["canonical-old-hash", "active-prefixed-id-hash"],
)
def test_legacy_archive_detection_requires_the_original_volume_hash_layout(
        monkeypatch, tmp_path, hashed_for, expected_aliases):
    volumeutils = _load_volumeutils(monkeypatch)
    pool_root = tmp_path / "bellagio-pool"
    archived_name = f"archived-{VOLUME_ID}"
    _write_hashed_metadata(
        volumeutils,
        pool_root,
        archived_name,
        hashed_for=hashed_for,
    )

    reservations, aliases = volumeutils._committed_capacity_records(
        str(pool_root),
    )

    assert reservations == {archived_name: VOLUME_SIZE}
    assert aliases == expected_aliases


def test_duplicate_committed_archive_identity_fails_closed(
        monkeypatch, tmp_path):
    volumeutils = _load_volumeutils(monkeypatch)
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / POOL_NAME
    archived_name = "archived-benedicts-duplicate-score"
    metadata = {
        "archive_name": archived_name,
        "original_volume_id": VOLUME_ID,
        "path_prefix": "archive",
        "size": VOLUME_SIZE,
        "state": "archived",
    }
    for crew in ("danny-ocean", "rusty-ryan"):
        metadata_path = (
            pool_root / "info" / "archive" / crew / f"{archived_name}.json"
        )
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    cleanup = _load_cleanup_module()

    with pytest.raises(ValueError, match="(?i)(duplicate|conflict)"):
        volumeutils._committed_capacity_records(str(pool_root))
    with pytest.raises(ValueError, match="(?i)(duplicate|conflict)"):
        cleanup.get_archived_pvs(POOL_NAME, None)


def test_archive_cleanup_tombstones_before_removal_and_retry_resumes(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    volumeutils.delete_volume(VOLUME_ID)
    archived_metadata = _archive_records(env)[0]
    archived_name = archived_metadata.stem
    archived_payload = env["pool_root"] / "archive" / archived_name
    cleanup = _load_cleanup_module()
    archived = cleanup.get_archived_pvs(POOL_NAME, archived_name)
    assert list(archived) == [archived_name]

    real_rmtree = cleanup.shutil.rmtree

    def interrupted_payload_delete(path):
        assert Path(path) == archived_payload
        assert json.loads(archived_metadata.read_text(encoding="utf-8"))[
            "state"
        ] == "reclaiming"
        raise OSError("The fake archive disposal truck broke down")

    monkeypatch.setattr(cleanup.shutil, "rmtree", interrupted_payload_delete)
    monkeypatch.setattr(
        cleanup,
        "release_archived_pv_reservation",
        lambda *_args: pytest.fail("capacity released before payload removal"),
    )

    with pytest.raises(OSError, match="disposal truck"):
        cleanup.delete_archived_pvs(POOL_NAME, archived)

    reclaiming = json.loads(archived_metadata.read_text(encoding="utf-8"))
    assert reclaiming["state"] == "reclaiming"
    assert archived_payload.exists()
    reservations, _aliases, tombstones = (
        volumeutils._capacity_metadata_records(str(env["pool_root"]))
    )
    assert reservations == {archived_name: VOLUME_SIZE}
    assert archived_name not in tombstones
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        accounting.update_summary(10 * VOLUME_SIZE)
        accounting.update_pv_record(archived_name, VOLUME_SIZE)
        volumeutils._rebuild_committed_capacity(
            accounting,
            str(env["pool_root"]),
        )
        assert accounting.get_pv_size(archived_name) == VOLUME_SIZE

    # If cleanup dies after removing the payload, the durable reclaim tombstone
    # authorizes the next rebuild to release its reservation.
    real_rmtree(archived_payload)
    reservations, _aliases, tombstones = (
        volumeutils._capacity_metadata_records(str(env["pool_root"]))
    )
    assert archived_name not in reservations
    assert archived_name in tombstones
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        volumeutils._rebuild_committed_capacity(
            accounting,
            str(env["pool_root"]),
        )
        assert accounting.get_pv_size(archived_name) == 0

    retry_records = cleanup.get_archived_pvs(POOL_NAME, archived_name)
    release_events = []

    def release_after_payload(pool, name, original):
        assert not archived_payload.exists()
        assert json.loads(archived_metadata.read_text(encoding="utf-8"))[
            "state"
        ] == "reclaiming"
        release_events.append((pool, name, original))

    monkeypatch.setattr(cleanup.shutil, "rmtree", real_rmtree)
    monkeypatch.setattr(
        cleanup,
        "release_archived_pv_reservation",
        release_after_payload,
    )

    cleanup.delete_archived_pvs(POOL_NAME, retry_records)

    assert release_events == [(POOL_NAME, archived_name, VOLUME_ID)]
    assert not archived_payload.exists()
    assert not archived_metadata.exists()


def test_managed_delete_tombstone_hides_volume_and_retry_resumes(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path, policy="delete")
    volumeutils = env["volumeutils"]
    payload = env["pool_root"] / env["volpath"]
    metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        accounting.update_summary(10 * VOLUME_SIZE)
        accounting.update_pv_record(VOLUME_ID, VOLUME_SIZE)
    real_rmtree = volumeutils.shutil.rmtree

    def interrupted_payload_delete(path):
        assert Path(path) == payload
        assert json.loads(metadata.read_text(encoding="utf-8"))["state"] == (
            "deleting"
        )
        raise OSError("The fake Bellagio demolition crew stopped")

    monkeypatch.setattr(
        volumeutils.shutil,
        "rmtree",
        interrupted_payload_delete,
    )

    with pytest.raises(OSError, match="demolition crew"):
        volumeutils.delete_volume(VOLUME_ID)

    assert json.loads(metadata.read_text(encoding="utf-8"))["state"] == "deleting"
    transitional = volumeutils.search_volume(VOLUME_ID)
    assert transitional is not None
    assert transitional.extra["state"] == "deleting"
    assert list(volumeutils.yield_pvc_from_hostvol()) == []
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        volumeutils._rebuild_committed_capacity(
            accounting,
            str(env["pool_root"]),
        )
        assert accounting.get_pv_size(VOLUME_ID) == VOLUME_SIZE

    # Payload removal is the release point. A crash before the explicit
    # accounting update is repaired from the still-present tombstone.
    real_rmtree(payload)
    reservations, _aliases, tombstones = (
        volumeutils._capacity_metadata_records(str(env["pool_root"]))
    )
    assert VOLUME_ID not in reservations
    assert VOLUME_ID in tombstones
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        volumeutils._rebuild_committed_capacity(
            accounting,
            str(env["pool_root"]),
        )
        assert accounting.get_pv_size(VOLUME_ID) == 0

    monkeypatch.setattr(volumeutils.shutil, "rmtree", real_rmtree)
    volumeutils.delete_volume(VOLUME_ID)

    assert not payload.exists()
    assert not metadata.exists()
    assert volumeutils.search_volume(VOLUME_ID) is None


def test_capacity_scan_rejects_noncanonical_delete_tombstone(
        monkeypatch, tmp_path):
    volumeutils = _load_volumeutils(monkeypatch)
    pool_root = tmp_path / "bellagio-noncanonical-delete"
    metadata_path = (
        pool_root / "info" / "subvol" / "00" / "00" / "impostor.json"
    )
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(
        json.dumps({
            "path_prefix": "subvol/00/00",
            "size": VOLUME_SIZE,
            "state": "deleting",
            "volume_id": VOLUME_ID,
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="canonical path"):
        volumeutils._capacity_metadata_records(str(pool_root))


def test_capacity_scan_propagates_payload_storage_error(monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path, policy="delete")
    volumeutils = env["volumeutils"]
    payload = env["pool_root"] / env["volpath"]
    metadata_path = env["pool_root"] / "info" / f"{env['volpath']}.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata.update({"state": "deleting", "volume_id": VOLUME_ID})
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    real_lstat = volumeutils.os.lstat

    def fail_payload_lstat(path, *args, **kwargs):
        if str(path) == str(payload):
            raise OSError(errno.EIO, "The Bellagio storage link failed")
        return real_lstat(path, *args, **kwargs)

    monkeypatch.setattr(volumeutils.os, "lstat", fail_payload_lstat)

    with pytest.raises(OSError) as raised:
        volumeutils._capacity_metadata_records(str(env["pool_root"]))
    assert raised.value.errno == errno.EIO


@pytest.mark.parametrize("recreated_active", [False, True])
def test_legacy_archive_accounting_migrates_and_cleanup_is_safe(
        monkeypatch, tmp_path, recreated_active):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    legacy_name = f"{volumeutils.ARCHIVE_PREFIX}{VOLUME_ID}"
    active_payload = env["pool_root"] / env["volpath"]
    active_metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"
    legacy_payload = active_payload.with_name(legacy_name)
    legacy_metadata = active_metadata.with_name(f"{legacy_name}.json")
    active_payload.rename(legacy_payload)
    active_metadata.rename(legacy_metadata)

    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        accounting.update_summary(10 * VOLUME_SIZE)
        # Old releases kept archived capacity under the original PVC ID.
        accounting.update_pv_record(VOLUME_ID, VOLUME_SIZE)

    if recreated_active:
        _write_active_volume(env, "A new fake Bellagio vault ledger")

    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        volumeutils._rebuild_committed_capacity(
            accounting,
            str(env["pool_root"]),
        )
        expected_count = 2 if recreated_active else 1
        assert accounting.get_pv_size(legacy_name) == VOLUME_SIZE
        assert accounting.get_pv_size(VOLUME_ID) == (
            VOLUME_SIZE if recreated_active else 0
        )
        assert accounting.get_stats()["number_of_pvs"] == expected_count

    cleanup = _load_cleanup_module()
    archived = cleanup.get_archived_pvs(POOL_NAME, legacy_name)
    assert list(archived) == [legacy_name]
    cleanup.delete_archived_pvs(POOL_NAME, archived)

    assert not legacy_payload.exists()
    assert not legacy_metadata.exists()
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        assert accounting.get_pv_size(legacy_name) == 0
        assert accounting.get_pv_size(VOLUME_ID) == (
            VOLUME_SIZE if recreated_active else 0
        )
    assert active_payload.exists() is recreated_active
    assert active_metadata.exists() is recreated_active


def test_legacy_release_orders_a_create_at_the_metadata_scan_boundary(
        monkeypatch, tmp_path):
    env = _configure_archive_pool(monkeypatch, tmp_path)
    volumeutils = env["volumeutils"]
    legacy_name = f"{volumeutils.ARCHIVE_PREFIX}{VOLUME_ID}"
    active_payload = env["pool_root"] / env["volpath"]
    active_metadata = env["pool_root"] / "info" / f"{env['volpath']}.json"
    legacy_payload = active_payload.with_name(legacy_name)
    legacy_metadata = active_metadata.with_name(f"{legacy_name}.json")
    active_payload.rename(legacy_payload)
    active_metadata.rename(legacy_metadata)
    reclaiming = json.loads(legacy_metadata.read_text(encoding="utf-8"))
    reclaiming.update({
        "legacy_archive": True,
        "original_volume_id": VOLUME_ID,
        "state": "reclaiming",
    })
    volumeutils._atomic_write_json(str(legacy_metadata), reclaiming)
    volumeutils.shutil.rmtree(legacy_payload)

    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        accounting.update_summary(10 * VOLUME_SIZE)
        accounting.update_pv_record(VOLUME_ID, VOLUME_SIZE)

    metadata_written = threading.Event()
    accounting_write_started = threading.Event()
    writer_finished = threading.Event()
    writer_errors = []

    def recreate_volume():
        try:
            active_payload.mkdir(parents=True)
            volumeutils.save_pv_metadata(
                str(env["pool_root"]),
                env["volpath"],
                VOLUME_SIZE,
            )
            metadata_written.set()
            with volumeutils.SizeAccounting(
                    POOL_NAME, str(env["pool_root"])) as accounting:
                accounting.conn.set_trace_callback(
                    lambda statement: accounting_write_started.set()
                    if statement.lstrip().startswith("INSERT OR REPLACE INTO pv_stats")
                    else None
                )
                accounting.update_pv_record(VOLUME_ID, VOLUME_SIZE)
        except BaseException as err:  # Preserve worker failures for the test.
            writer_errors.append(err)
        finally:
            writer_finished.set()

    real_capacity_scan = volumeutils._capacity_metadata_records
    creator = threading.Thread(target=recreate_volume)
    scan_count = 0

    def stale_snapshot_then_create(pool_root):
        nonlocal scan_count
        snapshot = real_capacity_scan(pool_root)
        scan_count += 1
        creator.start()
        assert metadata_written.wait(1)
        assert accounting_write_started.wait(1)
        # BEGIN IMMEDIATE must keep this accounting write behind the release.
        assert not writer_finished.wait(0.05)
        return snapshot

    monkeypatch.setattr(
        volumeutils,
        "_capacity_metadata_records",
        stale_snapshot_then_create,
    )

    volumeutils.release_archived_pv_reservation(
        POOL_NAME,
        legacy_name,
        VOLUME_ID,
    )
    creator.join(timeout=2)

    assert not creator.is_alive()
    assert not writer_errors
    assert scan_count == 1
    with volumeutils.SizeAccounting(
            POOL_NAME, str(env["pool_root"])) as accounting:
        assert accounting.get_pv_size(legacy_name) == 0
        assert accounting.get_pv_size(VOLUME_ID) == VOLUME_SIZE
