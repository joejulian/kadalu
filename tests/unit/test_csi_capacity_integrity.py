"""Capacity integrity and crash-recovery contracts for the CSI controller."""

import errno
import importlib
import json
import os
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[2]
GIB = 1024 * 1024 * 1024


class FakeContext:
    """Capture gRPC status and expose an optional shrinking deadline."""

    def __init__(self, timeout=None):
        self.code = None
        self.details = None
        self._deadline = (
            None if timeout is None else time.monotonic() + timeout
        )

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details

    def time_remaining(self):
        if self._deadline is None:
            return None
        return max(self._deadline - time.monotonic(), 0)

    def is_active(self):
        remaining = self.time_remaining()
        return remaining is None or remaining > 0

    def abort(self, code, details):
        self.code = code
        self.details = details
        raise RpcAborted


class RpcAborted(Exception):
    """Stand in for grpc.ServicerContext.abort terminating an RPC."""


def _load_csi(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    for name in ("controllerserver", "volumeutils"):
        sys.modules.pop(name, None)
    volumeutils = importlib.import_module("volumeutils")
    controllerserver = importlib.import_module("controllerserver")
    monkeypatch.setattr(
        controllerserver,
        "volume_operation_lock",
        _passthrough_volume_operation_lock,
    )
    monkeypatch.setattr(
        controllerserver,
        "volume_creation_identity_lock",
        _passthrough_volume_operation_lock,
    )
    csi_pb2 = importlib.import_module("csi_pb2")
    return volumeutils, controllerserver, csi_pb2


@contextmanager
def _passthrough_volume_operation_lock(*_args, **_kwargs):
    """Keep non-locking controller unit tests focused on their own contract."""
    yield


def _mount_capability():
    return {
        "mount": {},
        "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
    }


def _existing_volume(volumeutils, size=20 * 1024 * 1024):
    name = "pvc-bellagio-vault"
    return volumeutils.Volume(
        volname=name,
        voltype=volumeutils.PV_TYPE_SUBVOL,
        volhash=volumeutils.get_volname_hash(name),
        hostvol="bellagio-pool",
        size=size,
        hostvoltype="Replica1",
    )


def _fail_if_called(*_args, **_kwargs):
    raise AssertionError("storage must not be mutated for this request")


def _configure_native_create(
        monkeypatch, tmp_path, volumeutils, controllerserver):
    """Configure a native pool and return a list of requested create sizes."""
    pool = "bellagio-pool"
    volinfo = tmp_path / "volinfo"
    mount_root = tmp_path / "mnt"
    volinfo.mkdir()
    (mount_root / pool).mkdir(parents=True)
    (volinfo / "uid").write_text("ocean-eleven\n", encoding="utf-8")
    (volinfo / f"{pool}.info").write_text(
        json.dumps({"type": "Replica1"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(controllerserver, "VOLINFO_DIR", str(volinfo))
    monkeypatch.setattr(controllerserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: None)
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        lambda _filters: [{
            "name": pool,
            "type": "Replica1",
            "single_pv_per_pool": False,
        }],
    )
    monkeypatch.setattr(
        controllerserver,
        "mount_and_select_hosting_volume",
        lambda _volumes, _size: pool,
    )
    monkeypatch.setattr(controllerserver.random, "shuffle", lambda _items: None)
    monkeypatch.setattr(
        controllerserver,
        "verify_creation_reservation",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        controllerserver,
        "send_analytics_tracker",
        lambda *_args: None,
    )
    monkeypatch.setattr(controllerserver, "update_free_size", lambda *_args: None)

    requested_sizes = []

    def create_subdir(_mount, name, size, _use_gluster_quota):
        requested_sizes.append(size)
        return volumeutils.Volume(
            volname=name,
            voltype=volumeutils.PV_TYPE_SUBVOL,
            volhash=volumeutils.get_volname_hash(name),
            hostvol=pool,
            size=size,
            volpath=volumeutils.get_volume_path(
                volumeutils.PV_TYPE_SUBVOL,
                volumeutils.get_volname_hash(name),
                name,
            ),
        )

    monkeypatch.setattr(
        controllerserver,
        "create_subdir_volume",
        create_subdir,
    )
    return requested_sizes


@pytest.mark.parametrize(
    "capacity_range",
    [
        {"required_bytes": -1},
        {"limit_bytes": -1},
        {"required_bytes": 21, "limit_bytes": 20},
        {},
    ],
    ids=["negative-required", "negative-limit", "required-over-limit", "empty"],
)
def test_create_rejects_invalid_capacity_ranges_before_storage_mutation(
        monkeypatch, capacity_range):
    _volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    monkeypatch.setattr(controllerserver, "search_volume", _fail_if_called)

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-benedict-casino",
            capacity_range=capacity_range,
            volume_capabilities=[_mount_capability()],
        ),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_create_accepts_limit_only_capacity_range(monkeypatch, tmp_path):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    requested_sizes = _configure_native_create(
        monkeypatch, tmp_path, volumeutils, controllerserver)

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-mgm-grand-score",
            capacity_range={"limit_bytes": 64 * 1024 * 1024},
            volume_capabilities=[_mount_capability()],
        ),
        context,
    )

    assert context.code is None
    assert response.volume.capacity_bytes == 64 * 1024 * 1024
    assert requested_sizes == [64 * 1024 * 1024]


def test_create_uses_one_gib_default_when_capacity_range_is_omitted(
        monkeypatch, tmp_path):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    requested_sizes = _configure_native_create(
        monkeypatch, tmp_path, volumeutils, controllerserver)

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-oceans-reserve",
            volume_capabilities=[_mount_capability()],
        ),
        context,
    )

    assert context.code is None
    assert response.volume.capacity_bytes == GIB
    assert requested_sizes == [GIB]


@pytest.mark.parametrize("return_code", [1, 124], ids=["command-error", "timeout"])
@pytest.mark.parametrize(
    "rpc_name",
    ["create", "delete", "validate", "expand"],
)
def test_controller_storage_command_failures_are_retryable(
        monkeypatch, rpc_name, return_code):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    command_error = volumeutils.CommandException(
        return_code,
        "",
        (
            "command timed out after 120 seconds"
            if return_code == 124
            else "the Bellagio storage command failed"
        ),
    )

    def fail_storage(*_args, **_kwargs):
        raise command_error

    server = controllerserver.ControllerServer()
    if rpc_name == "create":
        monkeypatch.setattr(controllerserver, "search_volume", fail_storage)
        handler = server.CreateVolume
        request = csi_pb2.CreateVolumeRequest(
            name="pvc-bellagio-command-failure",
            capacity_range={"required_bytes": 20 * 1024 * 1024},
            volume_capabilities=[_mount_capability()],
        )
        response_type = csi_pb2.CreateVolumeResponse
    elif rpc_name == "delete":
        monkeypatch.setattr(controllerserver, "delete_volume", fail_storage)
        handler = server.DeleteVolume
        request = csi_pb2.DeleteVolumeRequest(
            volume_id="pvc-bellagio-command-failure",
        )
        response_type = csi_pb2.DeleteVolumeResponse
    elif rpc_name == "validate":
        monkeypatch.setattr(controllerserver, "search_volume", fail_storage)
        handler = server.ValidateVolumeCapabilities
        request = csi_pb2.ValidateVolumeCapabilitiesRequest(
            volume_id="pvc-bellagio-command-failure",
            volume_capabilities=[_mount_capability()],
        )
        response_type = csi_pb2.ValidateVolumeCapabilitiesResponse
    else:
        monkeypatch.setattr(controllerserver, "search_volume", fail_storage)
        handler = server.ControllerExpandVolume
        request = csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-command-failure",
            capacity_range={"required_bytes": 40 * 1024 * 1024},
        )
        response_type = csi_pb2.ControllerExpandVolumeResponse

    context = FakeContext()
    response = handler(request, context)

    assert isinstance(response, response_type)
    assert context.code in {
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNAVAILABLE,
    }
    assert "Storage operation failed" in context.details


@pytest.mark.parametrize("state", ["archiving", "deleting", "reclaiming"])
def test_create_rejects_a_volume_with_transitional_metadata(
        monkeypatch, state):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    existing = _existing_volume(volumeutils)
    existing.extra["state"] = state
    monkeypatch.setattr(
        controllerserver,
        "search_volume",
        lambda _volume_id: existing,
    )
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        _fail_if_called,
    )
    context = FakeContext()

    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name=existing.volname,
            capacity_range={"required_bytes": existing.size},
            volume_capabilities=[_mount_capability()],
        ),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION


def test_expand_rejects_an_omitted_capacity_range_before_lookup(monkeypatch):
    _volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    monkeypatch.setattr(controllerserver, "search_volume", _fail_if_called)

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
        ),
        context,
    )

    assert response.capacity_bytes == 0
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_stale_expand_rejects_limit_below_committed_capacity_without_mutation(
        monkeypatch):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    existing = _existing_volume(volumeutils)
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        _fail_if_called,
    )
    monkeypatch.setattr(controllerserver, "update_free_size", _fail_if_called)
    monkeypatch.setattr(controllerserver, "apply_subdir_quota", _fail_if_called)

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=existing.volname,
            capacity_range={
                "required_bytes": existing.size // 2,
                "limit_bytes": existing.size - 1,
            },
        ),
        context,
    )

    assert response.capacity_bytes == existing.size
    assert response.node_expansion_required is False
    assert context.code == grpc.StatusCode.OUT_OF_RANGE


@pytest.mark.parametrize(
    ("table", "bad_value"),
    [
        ("summary", -1),
        ("summary", "the-spanish-prisoner"),
        ("pv_stats", -1),
        ("pv_stats", "inside-man-ledger"),
    ],
)
def test_size_accounting_rejects_corrupt_rows(
        monkeypatch, tmp_path, table, bad_value):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)

    with volumeutils.SizeAccounting(
            "bellagio-pool", str(tmp_path)) as accounting:
        accounting.update_summary(1000)
        accounting.update_pv_record("pvc-danny-ocean", 100)
        accounting.cursor.execute(
            f"UPDATE {table} SET size = ?",  # noqa: S608 - fixed table fixture
            (bad_value,),
        )
        accounting.conn.commit()

        with pytest.raises(ValueError, match="(?i)(invalid|corrupt|size)"):
            accounting.get_stats()


@pytest.mark.parametrize("bad_value", [-1, "the-italian-job"])
def test_size_accounting_rejects_a_corrupt_pv_lookup(
        monkeypatch, tmp_path, bad_value):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)

    with volumeutils.SizeAccounting(
            "bellagio-pool", str(tmp_path)) as accounting:
        accounting.update_summary(1000)
        accounting.update_pv_record("pvc-stella-bridger", 100)
        accounting.cursor.execute(
            "UPDATE pv_stats SET size = ? WHERE pvname = ?",
            (bad_value, "pvc-stella-bridger"),
        )
        accounting.conn.commit()

        with pytest.raises(ValueError, match="(?i)(invalid|corrupt|size)"):
            accounting.get_pv_size("pvc-stella-bridger")


@pytest.mark.parametrize("pvtype", ["rawblock", "virtblock"])
def test_interrupted_block_create_with_matching_intent_repairs_metadata(
        monkeypatch, tmp_path, pvtype):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-charlie-croker"
    size = 8 * 1024 * 1024
    volhash = volumeutils.get_volname_hash(name)
    volpath = volumeutils.get_volume_path(pvtype, volhash, name)
    block_path = tmp_path / volpath
    block_path.parent.mkdir(parents=True)
    block_path.write_bytes(b"")
    os.truncate(block_path, size)
    intent_path = Path(volumeutils._block_creation_intent_path(
        str(tmp_path),
        volpath,
    ))
    intent_path.parent.mkdir(parents=True)
    intent_path.write_text(
        json.dumps(volumeutils._block_creation_intent(
            name,
            pvtype,
            volpath,
            size,
        )),
        encoding="utf-8",
    )
    executed = []
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *args: executed.append(args),
    )

    volume = volumeutils.create_block_volume(
        pvtype,
        str(tmp_path),
        name,
        size,
    )

    metadata_path = tmp_path / "info" / f"{volpath}.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["size"] == size
    assert metadata["path_prefix"] == os.path.dirname(volpath)
    assert str(uuid.UUID(metadata["incarnation"])) == metadata["incarnation"]
    assert block_path.stat().st_size == size
    assert volume.volpath == volpath
    assert not intent_path.exists()
    assert executed == (
        [(volumeutils.MKFS_XFS_CMD, "-f", str(block_path))]
        if pvtype == volumeutils.PV_TYPE_VIRTBLOCK
        else []
    )


@pytest.mark.parametrize("pvtype", ["rawblock", "virtblock"])
def test_block_create_never_modifies_unowned_existing_backing(
        monkeypatch, tmp_path, pvtype):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-the-thomas-crown-affair"
    size = 8 * 1024 * 1024
    volpath = volumeutils.get_volume_path(
        pvtype,
        volumeutils.get_volname_hash(name),
        name,
    )
    block_path = tmp_path / volpath
    block_path.parent.mkdir(parents=True)
    original = b"This fake backing belongs to a prior museum caper"
    block_path.write_bytes(original)
    monkeypatch.setattr(volumeutils, "execute", _fail_if_called)

    with pytest.raises(FileExistsError, match="creation intent"):
        volumeutils.create_block_volume(
            pvtype,
            str(tmp_path),
            name,
            size,
        )

    assert block_path.read_bytes() == original
    assert not (tmp_path / "info" / f"{volpath}.json").exists()
    assert not Path(volumeutils._block_creation_intent_path(
        str(tmp_path),
        volpath,
    )).exists()


@pytest.mark.parametrize("pvtype", ["rawblock", "virtblock"])
def test_block_retry_rejects_metadata_from_another_generation(
        monkeypatch, tmp_path, pvtype):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-baby-driver-generation-swap"
    size = 8 * 1024 * 1024
    volpath = volumeutils.get_volume_path(
        pvtype,
        volumeutils.get_volname_hash(name),
        name,
    )
    payload = tmp_path / volpath
    payload.parent.mkdir(parents=True)
    payload.write_bytes(b"")
    os.truncate(payload, size)
    intent = volumeutils.prepare_volume_creation(
        str(tmp_path),
        name,
        pvtype,
        volpath,
        size,
    )
    metadata_path = tmp_path / "info" / f"{volpath}.json"
    replacement = {
        "incarnation": "22222222-3333-4444-8555-666666666666",
        "path_prefix": os.path.dirname(volpath),
        "size": size,
    }
    metadata_path.write_text(json.dumps(replacement), encoding="utf-8")
    monkeypatch.setattr(volumeutils, "execute", _fail_if_called)

    with pytest.raises(ValueError, match="different volume generation"):
        volumeutils.create_block_volume(
            pvtype,
            str(tmp_path),
            name,
            size,
        )

    assert json.loads(metadata_path.read_text(encoding="utf-8")) == replacement
    assert volumeutils.read_volume_creation_intent(
        str(tmp_path),
        name,
        pvtype,
        volpath,
    ) == intent


@pytest.mark.parametrize("pvtype", ["rawblock", "virtblock"])
def test_failed_block_metadata_commit_leaves_resumable_creation_intent(
        monkeypatch, tmp_path, pvtype):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-the-great-muppet-caper"
    size = 8 * 1024 * 1024
    volpath = volumeutils.get_volume_path(
        pvtype,
        volumeutils.get_volname_hash(name),
        name,
    )
    intent_path = Path(volumeutils._block_creation_intent_path(
        str(tmp_path),
        volpath,
    ))
    expected_intent = volumeutils._block_creation_intent(
        name,
        pvtype,
        volpath,
        size,
    )
    successful_save = volumeutils.save_pv_metadata
    format_commands = []
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *command: format_commands.append(command),
    )
    monkeypatch.setattr(
        volumeutils,
        "save_pv_metadata",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("The fake jewel ledger did not commit")
        ),
    )

    with pytest.raises(OSError, match="did not commit"):
        volumeutils.create_block_volume(
            pvtype,
            str(tmp_path),
            name,
            size,
        )

    saved_intent = json.loads(intent_path.read_text(encoding="utf-8"))
    assert {
        key: saved_intent[key]
        for key in expected_intent
    } == expected_intent
    assert str(uuid.UUID(saved_intent["incarnation"])) == (
        saved_intent["incarnation"]
    )
    assert (tmp_path / volpath).stat().st_size == size
    assert not (tmp_path / "info" / f"{volpath}.json").exists()

    monkeypatch.setattr(volumeutils, "save_pv_metadata", successful_save)
    volume = volumeutils.create_block_volume(
        pvtype,
        str(tmp_path),
        name,
        size,
    )

    assert volume.volpath == volpath
    assert not intent_path.exists()
    metadata = json.loads(
        (tmp_path / "info" / f"{volpath}.json").read_text(encoding="utf-8")
    )
    assert metadata["path_prefix"] == os.path.dirname(volpath)
    assert metadata["size"] == size
    assert str(uuid.UUID(metadata["incarnation"])) == metadata["incarnation"]
    assert len(format_commands) == (
        2 if pvtype == volumeutils.PV_TYPE_VIRTBLOCK else 0
    )
    if pvtype == volumeutils.PV_TYPE_VIRTBLOCK:
        assert format_commands == [
            (volumeutils.MKFS_XFS_CMD, "-f", str(tmp_path / volpath)),
            (volumeutils.MKFS_XFS_CMD, "-f", str(tmp_path / volpath)),
        ]


def test_failed_atomic_metadata_save_leaves_no_authoritative_json(
        monkeypatch, tmp_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-the-score"
    volpath = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volumeutils.get_volname_hash(name),
        name,
    )
    metadata_path = tmp_path / "info" / f"{volpath}.json"

    def fail_replace(_source, _destination):
        raise OSError("the Bellagio metadata handoff was interrupted")

    monkeypatch.setattr(volumeutils.os, "replace", fail_replace)

    with pytest.raises(OSError):
        volumeutils.save_pv_metadata(str(tmp_path), volpath, 4096)

    assert not metadata_path.exists()


def test_failed_atomic_metadata_update_preserves_prior_json(
        monkeypatch, tmp_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-bank-job"
    volpath = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volumeutils.get_volname_hash(name),
        name,
    )
    metadata_path = tmp_path / "info" / f"{volpath}.json"
    metadata_path.parent.mkdir(parents=True)
    original = json.dumps({
        "size": 4096,
        "path_prefix": os.path.dirname(volpath),
    })
    metadata_path.write_text(original, encoding="utf-8")

    def fail_replace(_source, _destination):
        raise OSError("the vault switch failed before the new ledger landed")

    monkeypatch.setattr(volumeutils.os, "replace", fail_replace)

    with pytest.raises(OSError):
        volumeutils.update_pv_metadata(str(tmp_path), volpath, 8192)

    assert metadata_path.read_text(encoding="utf-8") == original


def _write_expandable_volume(volumeutils, pool_root, name, size):
    """Write one committed subvolume and return its canonical relative path."""
    volpath = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volumeutils.get_volname_hash(name),
        name,
    )
    payload = pool_root / volpath
    payload.mkdir(parents=True)
    volumeutils.save_pv_metadata(str(pool_root), volpath, size)
    return volpath


def test_pending_expansion_target_is_capacity_authoritative(
        monkeypatch, tmp_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    pool_root = tmp_path / "bellagio-expansion-pool"
    pool_root.mkdir()
    name = "pvc-rusty-expands-the-vault"
    from_size = 20 * 1024 * 1024
    to_size = 60 * 1024 * 1024
    volpath = _write_expandable_volume(
        volumeutils,
        pool_root,
        name,
        from_size,
    )
    volumeutils.prepare_volume_expansion(
        str(pool_root),
        name,
        volumeutils.PV_TYPE_SUBVOL,
        volpath,
        from_size,
        to_size,
    )

    with volumeutils.SizeAccounting(
            "bellagio-expansion-pool", str(pool_root)) as accounting:
        accounting.update_summary(100 * 1024 * 1024)
        accounting.update_pv_record(name, from_size)
        volumeutils._rebuild_committed_capacity(accounting, str(pool_root))

        assert accounting.get_pv_size(name) == to_size


def test_expansion_retry_rejects_a_different_target(monkeypatch, tmp_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    pool_root = tmp_path / "bellagio-exact-expansion"
    pool_root.mkdir()
    name = "pvc-linus-expands-the-vault"
    from_size = 20 * 1024 * 1024
    first_target = 40 * 1024 * 1024
    volpath = _write_expandable_volume(
        volumeutils,
        pool_root,
        name,
        from_size,
    )
    expected = volumeutils.prepare_volume_expansion(
        str(pool_root),
        name,
        volumeutils.PV_TYPE_SUBVOL,
        volpath,
        from_size,
        first_target,
    )

    with pytest.raises(ValueError, match="does not match"):
        volumeutils.prepare_volume_expansion(
            str(pool_root),
            name,
            volumeutils.PV_TYPE_SUBVOL,
            volpath,
            from_size,
            50 * 1024 * 1024,
        )

    assert volumeutils.read_volume_expansion_intent(
        str(pool_root),
        name,
        volumeutils.PV_TYPE_SUBVOL,
        volpath,
    ) == expected


def test_simple_quota_failure_leaves_target_reserved_before_growth(
        monkeypatch, tmp_path):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    pool = "bellagio-simple-quota-pool"
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / pool
    pool_root.mkdir(parents=True)
    name = "pvc-danny-expands-the-vault"
    from_size = 20 * 1024 * 1024
    to_size = 40 * 1024 * 1024
    volpath = _write_expandable_volume(
        volumeutils,
        pool_root,
        name,
        from_size,
    )
    existing = volumeutils.Volume(
        volname=name,
        voltype=volumeutils.PV_TYPE_SUBVOL,
        volhash=volumeutils.get_volname_hash(name),
        hostvol=pool,
        size=from_size,
        volpath=volpath,
        hostvoltype="Replica1",
    )
    accounted_size = from_size

    monkeypatch.setattr(controllerserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=100 * 1024 * 1024,
            f_bsize=1,
            f_frsize=1,
        ),
    )
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _name: accounted_size,
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, _size: True,
    )

    def update_accounting(_hostvol, _name, size_change):
        nonlocal accounted_size
        accounted_size = -size_change

    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        update_accounting,
    )

    def fail_quota(*_args, **_kwargs):
        intent = volumeutils.read_volume_expansion_intent(
            str(pool_root),
            name,
            volumeutils.PV_TYPE_SUBVOL,
            volpath,
        )
        assert intent["to_size"] == to_size
        with volumeutils.SizeAccounting(pool, str(pool_root)) as accounting:
            assert accounting.get_pv_size(name) == to_size
        raise OSError(errno.EIO, "The fake simple quota service stopped")

    monkeypatch.setattr(controllerserver, "update_subdir_volume", fail_quota)
    context = FakeContext()

    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=name,
            capacity_range={"required_bytes": to_size},
        ),
        context,
    )

    assert response.capacity_bytes == 0
    assert context.code == grpc.StatusCode.UNAVAILABLE
    assert json.loads(
        (pool_root / "info" / f"{volpath}.json").read_text(encoding="utf-8")
    )["size"] == from_size
    with volumeutils.SizeAccounting(pool, str(pool_root)) as accounting:
        accounting.update_summary(100 * 1024 * 1024)
        accounting.update_pv_record(name, from_size)
        volumeutils._rebuild_committed_capacity(accounting, str(pool_root))
        assert accounting.get_pv_size(name) == to_size


@pytest.mark.parametrize("admission_path", ["native-selection", "external-check"])
def test_admission_rebuilds_missing_reservation_from_committed_metadata(
        monkeypatch, tmp_path, admission_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    pool = "bellagio-pool"
    mount_root = tmp_path / "mnt"
    pool_mount = mount_root / pool
    pool_mount.mkdir(parents=True)
    name = "pvc-danny-ocean"
    volpath = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volumeutils.get_volname_hash(name),
        name,
    )
    metadata_path = pool_mount / "info" / f"{volpath}.json"
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(
        json.dumps({"size": 800, "path_prefix": os.path.dirname(volpath)}),
        encoding="utf-8",
    )

    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        volumeutils,
        "mount_glusterfs",
        lambda _volume, mountpoint: mountpoint,
    )
    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=1000,
            f_bsize=1,
            f_frsize=1,
        ),
    )

    if admission_path == "native-selection":
        admitted = volumeutils.mount_and_select_hosting_volume(
            [{"name": pool, "type": "Replica1"}],
            190,
        )
        assert admitted is None
    else:
        admitted = volumeutils.is_hosting_volume_free(pool, 190)
        assert admitted is False
    with volumeutils.SizeAccounting(pool, str(pool_mount)) as accounting:
        assert accounting.get_pv_size(name) == 800


@pytest.mark.parametrize("pvtype", ["subvol", "rawblock"])
def test_interrupted_create_admission_prevents_cross_name_overcommit_after_restart(
        monkeypatch, tmp_path, pvtype):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    pool = "bellagio-create-reservation-pool"
    pool_mount = tmp_path / "mnt" / pool
    pool_mount.mkdir(parents=True)
    first_name = "pvc-danny-ocean-admission"
    other_name = "pvc-rusty-ryan-overcommit"
    hosting_volumes = [{"name": pool, "type": "Replica1"}]
    monkeypatch.setattr(
        volumeutils,
        "HOSTVOL_MOUNTDIR",
        str(tmp_path / "mnt"),
    )
    monkeypatch.setattr(
        volumeutils,
        "mount_glusterfs",
        lambda _volume, mountpoint: mountpoint,
    )
    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=1000,
            f_bsize=1,
            f_frsize=1,
        ),
    )

    assert volumeutils.mount_and_select_hosting_volume(
        hosting_volumes,
        600,
        volume_id=first_name,
        pvtype=pvtype,
    ) == pool
    volpath = volumeutils.get_volume_path(
        pvtype,
        volumeutils.get_volname_hash(first_name),
        first_name,
    )
    intent_path = Path(volumeutils._creation_intent_path(
        str(pool_mount),
        volpath,
    ))
    saved_intent = json.loads(intent_path.read_text(encoding="utf-8"))
    assert {
        key: saved_intent[key]
        for key in ("path", "pvtype", "size", "version", "volume_id")
    } == {
        "path": volpath,
        "pvtype": pvtype,
        "size": 600,
        "version": 1,
        "volume_id": first_name,
    }
    assert str(uuid.UUID(saved_intent["incarnation"])) == (
        saved_intent["incarnation"]
    )
    assert not (pool_mount / volpath).exists()

    # Losing the rebuildable SQLite index models a controller restart at the
    # exact crash point between admission and payload/quota mutation.
    (pool_mount / "stat.db").unlink()
    assert volumeutils.mount_and_select_hosting_volume(
        hosting_volumes,
        400,
        volume_id=other_name,
        pvtype=pvtype,
    ) is None
    assert volumeutils.mount_and_select_hosting_volume(
        hosting_volumes,
        600,
        volume_id=first_name,
        pvtype=pvtype,
    ) == pool
    with pytest.raises(ValueError, match="Pending create does not match"):
        volumeutils.mount_and_select_hosting_volume(
            hosting_volumes,
            601,
            volume_id=first_name,
            pvtype=pvtype,
        )
    with volumeutils.SizeAccounting(pool, str(pool_mount)) as accounting:
        assert accounting.get_pv_size(first_name) == 600
        assert accounting.get_pv_size(other_name) == 0


def test_creation_intent_rebuild_fails_closed_for_malformed_or_duplicate_records(
        monkeypatch, tmp_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    name = "pvc-linus-caldwell-intent"
    subvol_path = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volumeutils.get_volname_hash(name),
        name,
    )
    subvol_intent_path = Path(volumeutils._creation_intent_path(
        str(tmp_path),
        subvol_path,
    ))
    subvol_intent_path.parent.mkdir(parents=True)
    malformed = volumeutils._volume_creation_intent(
        name,
        volumeutils.PV_TYPE_SUBVOL,
        subvol_path,
        400,
    )
    malformed["path"] = "subvol/nightfox/escape"
    subvol_intent_path.write_text(json.dumps(malformed), encoding="utf-8")

    with pytest.raises(ValueError, match="canonical"):
        volumeutils._capacity_metadata_records(str(tmp_path))

    subvol_intent_path.write_text(
        json.dumps(volumeutils._volume_creation_intent(
            name,
            volumeutils.PV_TYPE_SUBVOL,
            subvol_path,
            400,
        )),
        encoding="utf-8",
    )
    block_path = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_RAWBLOCK,
        volumeutils.get_volname_hash(name),
        name,
    )
    block_intent_path = Path(volumeutils._creation_intent_path(
        str(tmp_path),
        block_path,
    ))
    block_intent_path.parent.mkdir(parents=True)
    block_intent_path.write_text(
        json.dumps(volumeutils._volume_creation_intent(
            name,
            volumeutils.PV_TYPE_RAWBLOCK,
            block_path,
            400,
        )),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate creation intent"):
        volumeutils._capacity_metadata_records(str(tmp_path))


def test_expansion_reservation_serializes_cross_process_admission(
        monkeypatch, tmp_path):
    volumeutils, _controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    pool = "bellagio-expansion-race-pool"
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / pool
    pool_root.mkdir(parents=True)
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(f_blocks=1000, f_bsize=1, f_frsize=1),
    )

    volumes = []
    for name in ("pvc-danny-ocean-expands", "pvc-rusty-ryan-expands"):
        volpath = _write_expandable_volume(
            volumeutils,
            pool_root,
            name,
            300,
        )
        volumes.append((name, volpath))

    first_name, first_path = volumes[0]
    first_intent = volumeutils.reserve_volume_expansion(
        pool,
        first_name,
        (volumeutils.PV_TYPE_SUBVOL, first_path, 300, 500),
    )
    assert first_intent["to_size"] == 500

    # Losing SQLite models a second controller process starting from the
    # durable metadata and intent rather than sharing in-memory state.
    (pool_root / "stat.db").unlink()
    second_name, second_path = volumes[1]
    assert volumeutils.reserve_volume_expansion(
        pool,
        second_name,
        (volumeutils.PV_TYPE_SUBVOL, second_path, 300, 500),
    ) is None
    assert volumeutils.reserve_volume_expansion(
        pool,
        first_name,
        (volumeutils.PV_TYPE_SUBVOL, first_path, 300, 500),
    ) == first_intent

    with volumeutils.SizeAccounting(pool, str(pool_root)) as accounting:
        assert accounting.get_pv_size(first_name) == 500
        assert accounting.get_pv_size(second_name) == 300


@pytest.mark.parametrize("rpc_name", ["validate", "expand"])
def test_pending_create_is_not_exposed_as_a_committed_volume(
        monkeypatch, rpc_name):
    volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    pending = _existing_volume(volumeutils)
    pending.extra["state"] = "creating"
    monkeypatch.setattr(
        controllerserver,
        "search_volume",
        lambda _volume_id: pending,
    )
    context = FakeContext()

    if rpc_name == "validate":
        response = controllerserver.ControllerServer().ValidateVolumeCapabilities(
            csi_pb2.ValidateVolumeCapabilitiesRequest(
                volume_id=pending.volname,
                volume_capabilities=[_mount_capability()],
            ),
            context,
        )
        assert not response.HasField("confirmed")
        assert context.code == grpc.StatusCode.NOT_FOUND
    else:
        response = controllerserver.ControllerServer().ControllerExpandVolume(
            csi_pb2.ControllerExpandVolumeRequest(
                volume_id=pending.volname,
                capacity_range={"required_bytes": pending.size * 2},
            ),
            context,
        )
        assert response.capacity_bytes == 0
        assert context.code == grpc.StatusCode.FAILED_PRECONDITION


def _configure_single_pool_create(
        monkeypatch, tmp_path, controllerserver, actual_capacity):
    pool = "the-bank-job-pool"
    volinfo = tmp_path / "volinfo"
    mount_root = tmp_path / "mnt"
    volinfo.mkdir()
    (mount_root / pool).mkdir(parents=True)
    (volinfo / "uid").write_text("crew-red-circus\n", encoding="utf-8")
    (volinfo / f"{pool}.info").write_text(
        json.dumps({"type": "Replica1"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(controllerserver, "VOLINFO_DIR", str(volinfo))
    monkeypatch.setattr(controllerserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: None)
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        lambda _filters: [{
            "name": pool,
            "type": "Replica1",
            "single_pv_per_pool": True,
        }],
    )
    def mount_single_pool(_volumes, required_size, limit_size):
        if (
                actual_capacity < required_size
                or (limit_size and actual_capacity > limit_size)):
            raise controllerserver.UnsupportedCapacityRangeError(
                "dedicated pool capacity is outside the requested range"
            )
        return pool, actual_capacity

    monkeypatch.setattr(
        controllerserver,
        "mount_single_pv_hosting_volume",
        mount_single_pool,
    )
    monkeypatch.setattr(controllerserver.random, "shuffle", lambda _items: None)
    claims = []
    monkeypatch.setattr(
        controllerserver,
        "claim_single_pv_volume",
        lambda mount, volume_id, size: claims.append((mount, volume_id, size)),
    )
    return pool, claims


def test_single_pv_create_reports_and_claims_the_pool_actual_capacity(
        monkeypatch, tmp_path):
    _volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    actual_capacity = 100 * 1024 * 1024
    pool, claims = _configure_single_pool_create(
        monkeypatch, tmp_path, controllerserver, actual_capacity)

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-red-circus",
            capacity_range={"required_bytes": 20 * 1024 * 1024},
            volume_capabilities=[_mount_capability()],
            parameters={
                "storage_name": pool,
                "single_pv_per_pool": "true",
            },
        ),
        context,
    )

    assert context.code is None
    assert response.volume.capacity_bytes == actual_capacity
    assert claims == [(
        str(tmp_path / "mnt" / pool),
        "pvc-red-circus",
        actual_capacity,
    )]


@pytest.mark.parametrize(
    ("actual_capacity", "capacity_range"),
    [
        (
            100 * 1024 * 1024,
            {
                "required_bytes": 20 * 1024 * 1024,
                "limit_bytes": 50 * 1024 * 1024,
            },
        ),
        (
            10 * 1024 * 1024,
            {"required_bytes": 20 * 1024 * 1024},
        ),
    ],
    ids=["actual-above-limit", "actual-below-required"],
)
def test_single_pv_create_rejects_actual_capacity_outside_range(
        monkeypatch, tmp_path, actual_capacity, capacity_range):
    _volumeutils, controllerserver, csi_pb2 = _load_csi(monkeypatch)
    pool, claims = _configure_single_pool_create(
        monkeypatch, tmp_path, controllerserver, actual_capacity)

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-sneakers-black-box",
            capacity_range=capacity_range,
            volume_capabilities=[_mount_capability()],
            parameters={
                "storage_name": pool,
                "single_pv_per_pool": "true",
            },
        ),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.OUT_OF_RANGE
    assert not claims


def test_capacity_lock_respects_rpc_deadline_and_aborts_pending_operation(
        monkeypatch):
    _volumeutils, controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    lock = threading.Lock()
    lock.acquire()
    monkeypatch.setattr(controllerserver, "CAPACITY_OPERATION_LOCK", lock)
    executed = []

    @controllerserver.serialized_capacity_operation
    def operation(_server, _request, _context):
        executed.append(True)

    release = threading.Timer(0.2, lock.release)
    release.start()
    context = FakeContext(timeout=0.03)
    started = time.monotonic()
    try:
        try:
            operation(object(), object(), context)
        except RpcAborted:
            pass
        elapsed = time.monotonic() - started
    finally:
        release.join()

    assert elapsed < 0.15
    assert not executed
    assert context.code == grpc.StatusCode.ABORTED
    assert context.details


def test_capacity_lock_has_a_short_wait_cap_without_an_rpc_deadline(
        monkeypatch):
    _volumeutils, controllerserver, _csi_pb2 = _load_csi(monkeypatch)
    lock = threading.Lock()
    lock.acquire()
    monkeypatch.setattr(controllerserver, "CAPACITY_OPERATION_LOCK", lock)
    monkeypatch.setattr(
        controllerserver,
        "CAPACITY_LOCK_WAIT_TIMEOUT_SECONDS",
        0.03,
    )
    executed = []

    @controllerserver.serialized_capacity_operation
    def operation(_server, _request, _context):
        executed.append(True)

    release = threading.Timer(0.2, lock.release)
    release.start()
    context = FakeContext()
    started = time.monotonic()
    try:
        with pytest.raises(RpcAborted):
            operation(object(), object(), context)
        elapsed = time.monotonic() - started
    finally:
        release.join()

    assert elapsed < 0.15
    assert not executed
    assert context.code == grpc.StatusCode.ABORTED
