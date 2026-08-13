"""Safety and idempotency tests for CSI lifecycle operations."""

import importlib
import errno
import json
import os
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[2]
KUBELET_TARGET = (
    "/var/lib/kubelet/pods/oceans-eleven/volumes/"
    "kubernetes.io~csi/bellagio-vault/mount"
)


class FakeContext:
    """Capture the status set by a CSI handler."""

    def __init__(self):
        self.code = None
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


def _load_csi_module(monkeypatch, name):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    sys.modules.pop(name, None)
    module = importlib.import_module(name)
    if name == "controllerserver":
        module._real_volume_creation_identity_lock = (  # noqa: SLF001
            module.volume_creation_identity_lock
        )
        monkeypatch.setattr(
            module,
            "volume_operation_lock",
            _passthrough_volume_operation_lock,
        )
        monkeypatch.setattr(
            module,
            "volume_creation_identity_lock",
            _passthrough_volume_operation_lock,
        )
    return module


@contextmanager
def _passthrough_volume_operation_lock(*_args, **_kwargs):
    """Keep non-locking controller unit tests focused on their own contract."""
    yield


def _existing_volume(
        volumeutils, *, single_pv_per_pool=False, voltype=None):
    volname = "pvc-bellagio-vault"
    return volumeutils.Volume(
        volname=volname,
        voltype=voltype or volumeutils.PV_TYPE_SUBVOL,
        volhash=volumeutils.get_volname_hash(volname),
        hostvol="bellagio-pool",
        size=20 * 1024 * 1024,
        single_pv_per_pool=single_pv_per_pool,
        hostvoltype="Replica1",
    )


def _write_expansion_metadata(
        monkeypatch, tmp_path, volumeutils, controllerserver, volume):
    """Create the durable metadata that a real search_volume result implies."""
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / volume.hostvol
    metadata_path = pool_root / "info" / f"{volume.volpath}.json"
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(
        json.dumps({
            "path_prefix": os.path.dirname(volume.volpath),
            "size": volume.size,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(controllerserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    return pool_root, metadata_path


def _fail_if_called(*_args, **_kwargs):
    raise AssertionError("a non-expanding request must not mutate the vault")


def _node_publish_request(csi_pb2, volume_context):
    return csi_pb2.NodePublishVolumeRequest(
        volume_id="pvc-bellagio-vault",
        target_path=KUBELET_TARGET,
        volume_capability={
            "mount": {},
            "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
        },
        volume_context=volume_context,
    )


def _generated_node_path(nodeserver, volume_id="pvc-bellagio-vault",
                         pvtype="subvol"):
    return nodeserver.get_volume_path(
        pvtype,
        nodeserver.get_volname_hash(volume_id),
        volume_id,
    )


def _write_node_pool_record(
        monkeypatch, nodeserver, tmp_path, *, hostvol="bellagio-pool",
        voltype="Replica1", single_pv_per_pool=False):
    """Create the operator-owned pool record required by NodePublish."""
    info_root = tmp_path / "info"
    info_root.mkdir(exist_ok=True)
    (info_root / f"{hostvol}.info").write_text(
        json.dumps({
            "bricks": [],
            "gluster_hosts": "bellagio.example.invalid",
            "gluster_options": "",
            "gluster_volname": hostvol,
            "single_pv_per_pool": single_pv_per_pool,
            "type": voltype,
            "volname": hostvol,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(info_root))


def test_expand_retry_never_shrinks_an_existing_volume(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: existing.size,
    )
    monkeypatch.setattr(controllerserver, "is_hosting_volume_free", _fail_if_called)
    quota_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_subdir_volume",
        lambda _mount, _hostvoltype, _pvname, size, update_metadata=True:
            quota_updates.append((size, update_metadata)),
    )
    accounting_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": 10 * 1024 * 1024},
        ),
        context,
    )

    assert isinstance(response, csi_pb2.ControllerExpandVolumeResponse)
    assert response.capacity_bytes == existing.size
    assert response.node_expansion_required is False
    assert quota_updates == [(existing.size, False)]
    assert accounting_updates == []
    assert context.code is None


@pytest.mark.parametrize("state", ["deleting", "archiving"])
def test_expand_rejects_a_volume_in_a_delete_transition(monkeypatch, state):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    existing.extra["state"] = state
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "reserve_volume_expansion",
        _fail_if_called,
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=existing.volname,
            capacity_range={"required_bytes": existing.size * 2},
        ),
        context,
    )

    assert response.capacity_bytes == 0
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert state in context.details


def test_volume_lifecycle_lock_contends_across_processes(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume_id = "pvc-the-sting-shared-lock"
    pool_root = tmp_path / "bellagio-pool"
    pool_root.mkdir()

    # Create the stable lock inode before the independent controller process
    # opens it. Lock files are deliberately retained between operations.
    with volumeutils.volume_operation_lock(str(pool_root), volume_id):
        pass
    lock_path = next((pool_root / "info" / ".locks").iterdir())
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import fcntl,sys; "
                "lock_file=open(sys.argv[1], 'r+'); "
                "fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX); "
                "print('ready', flush=True); sys.stdin.readline()"
            ),
            str(lock_path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(volumeutils.VolumeOperationLockTimeoutError):
            with volumeutils.volume_operation_lock(
                    str(pool_root), volume_id, timeout=0.05):
                pytest.fail("a second controller acquired the same volume")
    finally:
        holder.communicate("\n", timeout=2)

    with volumeutils.volume_operation_lock(
            str(pool_root), volume_id, timeout=0.05):
        pass


def test_create_identity_lock_contends_across_controller_processes(
        monkeypatch, tmp_path):
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    volume_id = "pvc-logan-lucky-identity-lock"
    lock_root = tmp_path / "controller-locks"
    monkeypatch.setattr(
        controllerserver,
        "CREATE_IDENTITY_LOCK_DIR",
        str(lock_root),
    )
    lock = controllerserver._real_volume_creation_identity_lock  # noqa: SLF001
    context = FakeContext()
    context.time_remaining = lambda: 0.05

    with lock(volume_id, context):
        pass
    lock_path = next(lock_root.iterdir())
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import fcntl,sys; "
                "lock_file=open(sys.argv[1], 'r+'); "
                "fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX); "
                "print('ready', flush=True); sys.stdin.readline()"
            ),
            str(lock_path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(
                controllerserver.VolumeOperationLockTimeoutError):
            with lock(volume_id, context):
                pytest.fail("a second controller acquired the same CSI name")
    finally:
        holder.communicate("\n", timeout=2)

    with lock(volume_id, context):
        pass


def test_expansion_holds_lifecycle_lock_until_delete_can_recheck(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    target_size = existing.size * 2
    pool_root, _metadata_path = _write_expansion_metadata(
        monkeypatch,
        tmp_path,
        volumeutils,
        controllerserver,
        existing,
    )
    volinfo_root = tmp_path / "volinfo"
    volinfo_root.mkdir()
    (volinfo_root / f"{existing.hostvol}.info").write_text(
        json.dumps({"pvReclaimPolicy": "delete"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(volumeutils, "VOLINFO_DIR", str(volinfo_root))
    monkeypatch.setattr(
        controllerserver,
        "volume_operation_lock",
        volumeutils.volume_operation_lock,
    )
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(volumeutils, "search_volume", lambda _name: existing)

    quota_started = threading.Event()
    release_quota = threading.Event()

    def apply_quota(_volume, size, _context, update_metadata):
        quota_started.set()
        assert release_quota.wait(2)
        assert update_metadata is True
        volumeutils.update_pv_metadata(
            str(pool_root),
            existing.volpath,
            size,
        )
        existing.size = size
        return True

    monkeypatch.setattr(controllerserver, "apply_subdir_quota", apply_quota)
    delete_started = threading.Event()
    delete_errors = []
    monkeypatch.setattr(
        volumeutils,
        "_delete_managed_volume",
        lambda *_args: delete_started.set(),
    )
    expand_result = {}

    def expand():
        context = FakeContext()
        expand_result["response"] = (
            controllerserver.ControllerServer().ControllerExpandVolume(
                csi_pb2.ControllerExpandVolumeRequest(
                    volume_id=existing.volname,
                    capacity_range={"required_bytes": target_size},
                ),
                context,
            )
        )
        expand_result["context"] = context

    expand_thread = threading.Thread(target=expand)
    expand_thread.start()
    assert quota_started.wait(2)

    def delete():
        try:
            volumeutils.delete_volume(existing.volname)
        except volumeutils.VolumeOperationConflictError as err:
            delete_errors.append(err)

    delete_thread = threading.Thread(target=delete)
    delete_thread.start()

    assert not delete_started.wait(0.1)
    release_quota.set()
    expand_thread.join(2)
    delete_thread.join(2)

    assert not expand_thread.is_alive()
    assert not delete_thread.is_alive()
    # Expansion replaces metadata atomically. The stale delete must not act on
    # that now-different observation; CSI will retry and delete the new view.
    assert not delete_started.is_set()
    assert len(delete_errors) == 1
    assert expand_result["response"].capacity_bytes == target_size
    assert expand_result["context"].code is None


def test_idempotent_create_holds_lifecycle_lock_until_delete_can_recheck(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / existing.hostvol
    pool_root.mkdir(parents=True)
    volinfo_root = tmp_path / "volinfo"
    volinfo_root.mkdir()
    (volinfo_root / f"{existing.hostvol}.info").write_text(
        json.dumps({"pvReclaimPolicy": "delete"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(controllerserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(volumeutils, "VOLINFO_DIR", str(volinfo_root))
    monkeypatch.setattr(
        controllerserver,
        "volume_operation_lock",
        volumeutils.volume_operation_lock,
    )
    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(volumeutils, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "finish_committed_creation",
        lambda _volume: None,
    )

    reconcile_started = threading.Event()
    release_reconcile = threading.Event()

    def reconcile(*_args):
        reconcile_started.set()
        assert release_reconcile.wait(2)

    monkeypatch.setattr(
        controllerserver,
        "reconcile_committed_capacity",
        reconcile,
    )
    delete_started = threading.Event()
    monkeypatch.setattr(
        volumeutils,
        "_delete_managed_volume",
        lambda *_args: delete_started.set(),
    )
    create_result = {}

    def create():
        context = FakeContext()
        create_result["response"] = (
            controllerserver.ControllerServer().CreateVolume(
                csi_pb2.CreateVolumeRequest(
                    name=existing.volname,
                    capacity_range={"required_bytes": existing.size},
                    volume_capabilities=[{
                        "mount": {},
                        "access_mode": {
                            "mode": "MULTI_NODE_MULTI_WRITER",
                        },
                    }],
                    parameters={
                        "storage_name": existing.hostvol,
                        "single_pv_per_pool": "false",
                    },
                ),
                context,
            )
        )
        create_result["context"] = context

    create_thread = threading.Thread(target=create)
    create_thread.start()
    assert reconcile_started.wait(2)
    delete_thread = threading.Thread(
        target=volumeutils.delete_volume,
        args=(existing.volname,),
    )
    delete_thread.start()

    assert not delete_started.wait(0.1)
    release_reconcile.set()
    create_thread.join(2)
    delete_thread.join(2)

    assert not create_thread.is_alive()
    assert not delete_thread.is_alive()
    assert delete_started.is_set()
    assert create_result["response"].volume.volume_id == existing.volname
    assert create_result["context"].code is None


def test_stale_delete_refuses_a_recreated_same_id_volume(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume_id = "pvc-inside-man-recreated-vault"
    hostvol = "bellagio-pool"
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / hostvol
    pool_root.mkdir(parents=True)
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    volinfo_root = tmp_path / "volinfo"
    volinfo_root.mkdir()
    (volinfo_root / f"{hostvol}.info").write_text(
        json.dumps({"pvReclaimPolicy": "delete"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(volumeutils, "VOLINFO_DIR", str(volinfo_root))
    monkeypatch.setattr(
        volumeutils,
        "_set_simple_quota",
        lambda *_args, **_kwargs: None,
    )

    old_volume = volumeutils.create_subdir_volume(
        str(pool_root),
        volume_id,
        20 * 1024 * 1024,
        use_gluster_quota=False,
    )
    old_volume.hostvol = hostvol
    old_volume.extra["hostvoltype"] = "Replica1"
    current_volume = [old_volume]
    monkeypatch.setattr(
        volumeutils,
        "search_volume",
        lambda _name: current_volume[0],
    )
    monkeypatch.setattr(volumeutils, "update_free_size", lambda *_args: None)

    observed = threading.Event()
    release_search = threading.Event()
    original_incarnation = volumeutils.volume_incarnation
    incarnation_calls = 0

    def pause_after_observation(volume):
        nonlocal incarnation_calls
        result = original_incarnation(volume)
        incarnation_calls += 1
        if incarnation_calls == 1:
            observed.set()
            assert release_search.wait(2)
        return result

    monkeypatch.setattr(
        volumeutils,
        "volume_incarnation",
        pause_after_observation,
    )
    delete_errors = []

    def stale_delete():
        try:
            volumeutils.delete_volume(volume_id)
        except volumeutils.VolumeOperationConflictError as err:
            delete_errors.append(err)

    delete_thread = threading.Thread(target=stale_delete)
    delete_thread.start()
    assert observed.wait(2)

    metadata_path = pool_root / "info" / f"{old_volume.volpath}.json"
    old_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    replacement = dict(old_metadata)
    replacement["incarnation"] = "22222222-3333-4444-8555-666666666666"
    metadata_path.write_text(json.dumps(replacement), encoding="utf-8")
    recreated = volumeutils.Volume(
        volname=volume_id,
        voltype=volumeutils.PV_TYPE_SUBVOL,
        volhash=volumeutils.get_volname_hash(volume_id),
        hostvol=hostvol,
        size=old_volume.size,
        volpath=old_volume.volpath,
        hostvoltype="Replica1",
    )
    current_volume[0] = recreated
    release_search.set()
    delete_thread.join(2)

    assert not delete_thread.is_alive()
    assert len(delete_errors) == 1
    assert metadata_path.exists()
    assert json.loads(metadata_path.read_text(encoding="utf-8")) == replacement


def test_native_create_retry_repairs_capacity_accounting(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        _fail_if_called,
    )
    monkeypatch.setattr(
        controllerserver,
        "mount_and_select_hosting_volume",
        _fail_if_called,
    )
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: 0,
    )
    monkeypatch.setattr(controllerserver, "is_hosting_volume_free", _fail_if_called)
    accounting_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-bellagio-vault",
            capacity_range={"required_bytes": existing.size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={
                "storage_name": "bellagio-pool",
                "single_pv_per_pool": "false",
            },
        ),
        context,
    )

    assert response.volume.volume_id == "pvc-bellagio-vault"
    assert response.volume.capacity_bytes == existing.size
    assert response.volume.volume_context == {
        "type": "Replica1",
        "hostvol": "bellagio-pool",
        "pvtype": "subvol",
        "path": existing.volpath,
        "fstype": "xfs",
        "single_pv_per_pool": "False",
    }
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]
    assert context.code is None


def test_create_retry_repairs_missing_reservation_when_pool_is_full(
        monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: 0,
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        _fail_if_called,
    )
    accounting_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name=existing.volname,
            capacity_range={"required_bytes": existing.size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={
                "storage_name": existing.hostvol,
                "single_pv_per_pool": "false",
            },
        ),
        context,
    )

    assert response.volume.volume_id == existing.volname
    assert response.volume.capacity_bytes == existing.size
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]
    assert context.code is None


def test_create_retry_rejects_incompatible_pool(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-bellagio-vault",
            capacity_range={"required_bytes": existing.size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={"storage_name": "mirage-pool"},
        ),
        context,
    )

    assert isinstance(response, csi_pb2.CreateVolumeResponse)
    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.ALREADY_EXISTS


def test_external_create_retry_rejects_different_gluster_target(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    existing.extra.update({
        "hostvoltype": "External",
        "ghost": "bellagio.example.invalid",
        "gvolname": "bellagio-vault",
        "goptions": "log-level=WARNING",
    })

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-bellagio-vault",
            capacity_range={"required_bytes": existing.size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={
                "hostvol_type": "External",
                "gluster_hosts": "mirage.example.invalid",
                "gluster_volname": "bellagio-vault",
                "gluster_options": "log-level=WARNING",
                "single_pv_per_pool": "false",
            },
        ),
        context,
    )

    assert isinstance(response, csi_pb2.CreateVolumeResponse)
    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.ALREADY_EXISTS


def test_external_create_retry_repairs_capacity_accounting(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    existing.extra.update({
        "hostvoltype": "External",
        "ghost": "bellagio.example.invalid",
        "gvolname": "bellagio-vault",
        "goptions": "log-level=WARNING",
    })
    accounting_updates = []

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: 0,
    )
    monkeypatch.setattr(controllerserver, "is_hosting_volume_free", _fail_if_called)
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name=existing.volname,
            capacity_range={"required_bytes": existing.size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={
                "hostvol_type": "External",
                "gluster_hosts": "bellagio.example.invalid",
                "gluster_volname": "bellagio-vault",
                "gluster_options": "log-level=WARNING",
                "single_pv_per_pool": "false",
            },
        ),
        FakeContext(),
    )

    assert response.volume.volume_id == existing.volname
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]


def test_external_kadalu_retry_repairs_interrupted_capacity_accounting(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    pvsize = 20 * 1024 * 1024
    existing = None
    accounting_attempts = 0
    accounted_sizes = {}

    monkeypatch.setattr(controllerserver, "VOLINFO_DIR", str(tmp_path))
    (tmp_path / "uid").write_text("the-bellagio-crew", encoding="utf-8")
    monkeypatch.setattr(
        controllerserver,
        "search_volume",
        lambda _name: existing,
    )
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        lambda _filters: [{
            "name": "bellagio-pool",
            "type": "External",
            "g_volname": "bellagio-vault",
            "g_host": "bellagio.example.invalid",
            "g_options": "log-level=WARNING",
        }],
    )
    monkeypatch.setattr(
        controllerserver,
        "check_external_volume",
        lambda _request, _volumes: {
            "name": "bellagio-pool",
            "g_volname": "bellagio-vault",
            "g_host": "bellagio.example.invalid",
            "g_options": "log-level=WARNING",
        },
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, _size: True,
    )
    monkeypatch.setattr(
        controllerserver,
        "verify_creation_reservation",
        lambda *_args: None,
    )
    monkeypatch.setattr(controllerserver.os.path, "isfile", lambda _path: False)

    def fake_create_subdir(_mntdir, volname, size, _use_quota,
                           save_metadata=True):
        nonlocal existing
        assert save_metadata
        existing = volumeutils.Volume(
            volname=volname,
            voltype=volumeutils.PV_TYPE_SUBVOL,
            volhash=volumeutils.get_volname_hash(volname),
            hostvol="bellagio-pool",
            size=size,
            hostvoltype="External",
            ghost="bellagio.example.invalid",
            gvolname="bellagio-vault",
            goptions="log-level=WARNING",
        )
        return existing

    monkeypatch.setattr(
        controllerserver,
        "create_subdir_volume",
        fake_create_subdir,
    )

    def fake_update_free_size(hostvol, pvname, sizechange):
        nonlocal accounting_attempts
        accounting_attempts += 1
        if accounting_attempts == 1:
            raise OSError("the accounting ledger is temporarily unavailable")
        accounted_sizes[(hostvol, pvname)] = -sizechange

    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        fake_update_free_size,
    )
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda hostvol, pvname: accounted_sizes.get((hostvol, pvname), 0),
    )
    monkeypatch.setattr(
        controllerserver,
        "send_analytics_tracker",
        lambda *_args: None,
    )

    request = csi_pb2.CreateVolumeRequest(
        name="pvc-bellagio-vault",
        capacity_range={"required_bytes": pvsize},
        volume_capabilities=[{
            "mount": {},
            "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
        }],
        parameters={
            "hostvol_type": "External",
            "gluster_hosts": "bellagio.example.invalid",
            "gluster_volname": "bellagio-vault",
            "gluster_options": "log-level=WARNING",
            "single_pv_per_pool": "false",
        },
    )

    first_context = FakeContext()
    first_response = controllerserver.ControllerServer().CreateVolume(
        request,
        first_context,
    )
    assert not first_response.HasField("volume")
    assert first_context.code == grpc.StatusCode.INTERNAL

    second_response = controllerserver.ControllerServer().CreateVolume(
        request,
        FakeContext(),
    )
    third_response = controllerserver.ControllerServer().CreateVolume(
        request,
        FakeContext(),
    )

    assert second_response.volume.volume_id == request.name
    assert third_response.volume.volume_id == request.name
    assert accounting_attempts == 2
    assert accounted_sizes == {
        ("bellagio-pool", request.name): pvsize,
    }


def test_capacity_reconciliation_replaces_the_existing_pv_record(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    pvsize = 20 * 1024 * 1024
    hostvol = "bellagio-pool"
    pvname = "pvc-bellagio-vault"
    mountdir = tmp_path / hostvol
    mountdir.mkdir()
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(tmp_path))

    volumeutils.update_free_size(hostvol, pvname, -pvsize)
    volumeutils.update_free_size(hostvol, pvname, -pvsize)

    with volumeutils.SizeAccounting(hostvol, str(mountdir)) as accounting:
        accounting.update_summary(10 * pvsize)
        stats = accounting.get_stats()
        recorded_size = accounting.get_pv_size(pvname)
        missing_size = accounting.get_pv_size("pvc-basher-tarr")

    assert stats["number_of_pvs"] == 1
    assert stats["used_size_bytes"] == pvsize
    assert recorded_size == pvsize
    assert missing_size == 0


def test_external_simple_quota_is_applied_before_expansion_metadata(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    pvname = "pvc-bellagio-vault"
    expanded_size = 30 * 1024 * 1024
    events = []

    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=expanded_size,
            f_bsize=1,
            f_frsize=1,
        ),
    )
    monkeypatch.setattr(
        volumeutils.os,
        "setxattr",
        lambda _path, name, value: events.append(("quota", name, value)),
    )
    monkeypatch.setattr(
        volumeutils,
        "update_pv_metadata",
        lambda _mount, _path, size: events.append(("metadata", size)),
    )

    volume = volumeutils.update_subdir_volume(
        str(tmp_path),
        "External",
        pvname,
        expanded_size,
    )

    assert volume.size == expanded_size
    assert events == [
        (
            "quota",
            "trusted.gfs.squota.limit",
            str(expanded_size).encode(),
        ),
        ("metadata", expanded_size),
    ]


def test_simple_quota_is_applied_before_create_metadata(monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    pvsize = 20 * 1024 * 1024
    events = []

    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=pvsize,
            f_bsize=1,
            f_frsize=1,
        ),
    )
    monkeypatch.setattr(
        volumeutils.os,
        "setxattr",
        lambda _path, name, value: events.append(("quota", name, value)),
    )
    monkeypatch.setattr(
        volumeutils,
        "save_pv_metadata",
        lambda _mount, _path, size, **_kwargs:
            events.append(("metadata", size)),
    )

    volumeutils.create_subdir_volume(
        str(tmp_path),
        "pvc-bellagio-vault",
        pvsize,
        use_gluster_quota=False,
    )

    assert events == [
        ("quota", "trusted.glusterfs.namespace", b"true"),
        ("quota", "trusted.gfs.squota.limit", str(pvsize).encode()),
        ("metadata", pvsize),
    ]


def test_failed_simple_quota_does_not_commit_expansion_metadata(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    expanded_size = 30 * 1024 * 1024

    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=expanded_size,
            f_bsize=1,
            f_frsize=1,
        ),
    )

    def fail_quota(*_args):
        raise OSError("the Bellagio vault rejected the new limit")

    monkeypatch.setattr(volumeutils.os, "setxattr", fail_quota)
    monkeypatch.setattr(volumeutils, "update_pv_metadata", _fail_if_called)

    with pytest.raises(OSError):
        volumeutils.update_subdir_volume(
            str(tmp_path),
            "Replica1",
            "pvc-bellagio-vault",
            expanded_size,
        )


def test_equal_size_expand_retry_repairs_quota_and_accounting(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    expanded_size = 30 * 1024 * 1024
    pool_root, _metadata_path = _write_expansion_metadata(
        monkeypatch,
        tmp_path,
        volumeutils,
        controllerserver,
        existing,
    )
    quota_updates = []

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)

    def fake_update_subdir(
            _mount, _hostvoltype, _pvname, size, update_metadata=True):
        quota_updates.append((size, update_metadata))
        if update_metadata:
            volumeutils.update_pv_metadata(
                str(pool_root),
                existing.volpath,
                size,
            )
            existing.size = size

    monkeypatch.setattr(
        controllerserver,
        "update_subdir_volume",
        fake_update_subdir,
    )

    original_finish = controllerserver.finish_volume_expansion
    finish_attempts = 0

    def interrupted_finish(*args):
        nonlocal finish_attempts
        finish_attempts += 1
        if finish_attempts == 1:
            raise OSError("the casino ledger is temporarily unavailable")
        return original_finish(*args)

    monkeypatch.setattr(
        controllerserver,
        "finish_volume_expansion",
        interrupted_finish,
    )

    request = csi_pb2.ControllerExpandVolumeRequest(
        volume_id=existing.volname,
        capacity_range={"required_bytes": expanded_size},
    )

    first_context = FakeContext()
    first_response = (
        controllerserver.ControllerServer().ControllerExpandVolume(
            request,
            first_context,
        )
    )
    assert first_response.capacity_bytes == 0
    assert first_context.code == grpc.StatusCode.INTERNAL

    response = controllerserver.ControllerServer().ControllerExpandVolume(
        request,
        FakeContext(),
    )

    assert response.capacity_bytes == expanded_size
    assert quota_updates == [
        (expanded_size, True),
        (expanded_size, True),
    ]
    assert finish_attempts == 2
    assert volumeutils.read_volume_expansion_intent(
        str(pool_root),
        existing.volname,
        existing.voltype,
        existing.volpath,
    ) is None
    with volumeutils.SizeAccounting(
            existing.hostvol, str(pool_root)) as accounting:
        assert accounting.get_pv_size(existing.volname) == expanded_size


def test_equal_size_repair_restores_committed_reservation_when_pool_is_full(
        monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    capacity_checks = []

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: 0,
    )

    monkeypatch.setattr(controllerserver, "is_hosting_volume_free", _fail_if_called)
    monkeypatch.setattr(
        controllerserver,
        "apply_subdir_quota",
        lambda *_args, **_kwargs: True,
    )
    accounting_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=existing.volname,
            capacity_range={"required_bytes": existing.size},
        ),
        context,
    )

    assert response.capacity_bytes == existing.size
    assert context.code is None
    assert capacity_checks == []
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]


def test_equal_size_repair_restores_absolute_committed_reservation(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    accounted_size = 8 * 1024 * 1024
    capacity_checks = []

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: accounted_size,
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, size: capacity_checks.append(size) or True,
    )
    monkeypatch.setattr(
        controllerserver,
        "apply_subdir_quota",
        lambda *_args, **_kwargs: True,
    )
    accounting_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=existing.volname,
            capacity_range={"required_bytes": existing.size},
        ),
        FakeContext(),
    )

    assert response.capacity_bytes == existing.size
    assert capacity_checks == []
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]


def test_equal_size_external_ssh_retry_reapplies_remote_quota(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    existing.extra.update({
        "hostvoltype": "External",
        "ghost": "bellagio.example.invalid",
        "gvolname": "bellagio-vault",
    })
    quota_calls = []

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: existing.size,
    )
    monkeypatch.setattr(controllerserver, "is_hosting_volume_free", _fail_if_called)
    monkeypatch.setattr(controllerserver.os.path, "isfile", lambda _path: True)
    monkeypatch.setenv("SECRET_GLUSTERQUOTA_SSH_USERNAME", "danny-ocean")
    monkeypatch.setattr(
        controllerserver,
        "execute_gluster_quota_command",
        lambda *args: quota_calls.append(args),
    )
    monkeypatch.setattr(controllerserver, "update_pv_metadata", _fail_if_called)
    accounting_updates = []
    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        lambda *args: accounting_updates.append(args),
    )

    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=existing.volname,
            capacity_range={"required_bytes": existing.size},
        ),
        FakeContext(),
    )

    assert response.capacity_bytes == existing.size
    assert quota_calls[0][-1] == existing.size
    assert accounting_updates == []


def test_expand_full_pool_returns_the_correct_response_type(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: existing.size,
    )
    monkeypatch.setattr(
        controllerserver,
        "reserve_volume_expansion",
        lambda *_args: None,
    )
    monkeypatch.setattr(controllerserver, "update_subdir_volume", _fail_if_called)

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": 30 * 1024 * 1024},
        ),
        context,
    )

    assert isinstance(response, csi_pb2.ControllerExpandVolumeResponse)
    assert response.capacity_bytes == existing.size
    assert context.code == grpc.StatusCode.RESOURCE_EXHAUSTED


def test_single_pv_pool_rejects_expansion(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils, single_pv_per_pool=True)
    existing.extra["single_pv_claim_state"] = "active"

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "single_pv_operation_lock",
        _passthrough_volume_operation_lock,
    )
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: existing.size,
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        _fail_if_called,
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": 30 * 1024 * 1024},
        ),
        context,
    )

    assert isinstance(response, csi_pb2.ControllerExpandVolumeResponse)
    assert response.capacity_bytes == existing.size
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION


def test_block_volume_growth_is_rejected_before_backing_file_changes(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(
        volumeutils,
        voltype=volumeutils.PV_TYPE_VIRTBLOCK,
    )

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        _fail_if_called,
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": 30 * 1024 * 1024},
        ),
        context,
    )

    assert response.capacity_bytes == existing.size
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION


def test_failed_external_quota_expansion_does_not_commit_metadata(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    existing.extra.update({
        "hostvoltype": "External",
        "ghost": "bellagio.example.invalid",
        "gvolname": "bellagio-vault",
    })
    pool_root, metadata_path = _write_expansion_metadata(
        monkeypatch,
        tmp_path,
        volumeutils,
        controllerserver,
        existing,
    )
    target_size = 30 * 1024 * 1024
    accounted_size = existing.size

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: accounted_size,
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, _size: True,
    )
    monkeypatch.setattr(controllerserver.os.path, "isfile", lambda _path: True)
    monkeypatch.setenv("SECRET_GLUSTERQUOTA_SSH_USERNAME", "danny-ocean")
    def reject_quota(*_args):
        raise controllerserver.CommandException(
            1,
            "gluster quota",
            "The remote vault rejected the crew",
        )

    monkeypatch.setattr(
        controllerserver,
        "execute_gluster_quota_command",
        reject_quota,
    )
    monkeypatch.setattr(controllerserver, "update_pv_metadata", _fail_if_called)
    accounting_updates = []

    def update_accounting(hostvol, pvname, size_change):
        nonlocal accounted_size
        accounting_updates.append((hostvol, pvname, size_change))
        accounted_size = -size_change

    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        update_accounting,
    )

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": target_size},
        ),
        context,
    )

    assert response.capacity_bytes == existing.size
    assert context.code == grpc.StatusCode.UNAVAILABLE
    assert json.loads(metadata_path.read_text(encoding="utf-8"))["size"] == (
        existing.size
    )
    assert volumeutils.read_volume_expansion_intent(
        str(pool_root),
        existing.volname,
        existing.voltype,
        existing.volpath,
    )["to_size"] == target_size
    assert accounting_updates == []
    with volumeutils.SizeAccounting(
            existing.hostvol, str(pool_root)) as accounting:
        assert accounting.get_pv_size(existing.volname) == target_size

    remote_quota_calls = []
    monkeypatch.setattr(
        controllerserver,
        "execute_gluster_quota_command",
        lambda *args: remote_quota_calls.append(args),
    )
    monkeypatch.setattr(
        controllerserver,
        "update_pv_metadata",
        volumeutils.update_pv_metadata,
    )
    retry_context = FakeContext()
    retry_response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": target_size},
        ),
        retry_context,
    )

    assert retry_context.code is None
    assert retry_response.capacity_bytes == target_size
    assert len(remote_quota_calls) == 1
    assert json.loads(metadata_path.read_text(encoding="utf-8"))["size"] == (
        target_size
    )
    assert volumeutils.read_volume_expansion_intent(
        str(pool_root),
        existing.volname,
        existing.voltype,
        existing.volpath,
    ) is None
    assert accounted_size == target_size


def test_concurrent_external_creates_cannot_overcommit_capacity(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    pool_capacity = 100
    requested_size = 60
    used_capacity = 0
    active_checks = 0
    maximum_active_checks = 0
    volumes = {}
    state_lock = threading.Lock()
    start_barrier = threading.Barrier(2)

    monkeypatch.setattr(controllerserver, "VOLINFO_DIR", str(tmp_path))
    (tmp_path / "uid").write_text("the-bellagio-crew", encoding="utf-8")
    monkeypatch.setattr(
        controllerserver,
        "search_volume",
        lambda name: volumes.get(name),
    )
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        lambda _filters: [{
            "name": "bellagio-pool",
            "type": "External",
            "g_volname": "bellagio-vault",
            "g_host": "bellagio.example.invalid",
            "g_options": "",
        }],
    )
    monkeypatch.setattr(
        controllerserver,
        "check_external_volume",
        lambda _request, volumes_found: volumes_found[0],
    )
    monkeypatch.setattr(controllerserver.os.path, "isfile", lambda _path: False)
    monkeypatch.setattr(
        controllerserver,
        "send_analytics_tracker",
        lambda *_args: None,
    )

    def fake_is_free(_hostvol, size):
        nonlocal active_checks, maximum_active_checks
        with state_lock:
            active_checks += 1
            maximum_active_checks = max(maximum_active_checks, active_checks)
            available = used_capacity + size <= pool_capacity
        time.sleep(0.05)
        with state_lock:
            active_checks -= 1
        return available

    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        fake_is_free,
    )
    monkeypatch.setattr(
        controllerserver,
        "verify_creation_reservation",
        lambda *_args: None,
    )

    def fake_create_subdir(_mount, name, size, _use_quota, **_kwargs):
        volume = volumeutils.Volume(
            volname=name,
            voltype=volumeutils.PV_TYPE_SUBVOL,
            volhash=volumeutils.get_volname_hash(name),
            hostvol="bellagio-pool",
            size=size,
            hostvoltype="External",
            ghost="bellagio.example.invalid",
            gvolname="bellagio-vault",
        )
        volumes[name] = volume
        return volume

    monkeypatch.setattr(
        controllerserver,
        "create_subdir_volume",
        fake_create_subdir,
    )

    def fake_update_free_size(_hostvol, _pvname, sizechange):
        nonlocal used_capacity
        with state_lock:
            used_capacity = used_capacity - sizechange

    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        fake_update_free_size,
    )

    def request(name):
        return csi_pb2.CreateVolumeRequest(
            name=name,
            capacity_range={"required_bytes": requested_size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={
                "hostvol_type": "External",
                "gluster_hosts": "bellagio.example.invalid",
                "gluster_volname": "bellagio-vault",
                "single_pv_per_pool": "false",
            },
        )

    def create(name):
        context = FakeContext()
        start_barrier.wait()
        response = controllerserver.ControllerServer().CreateVolume(
            request(name),
            context,
        )
        return response, context

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(
            create,
            ("pvc-danny-ocean", "pvc-rusty-ryan"),
        ))

    successful = [response for response, _context in results
                  if response.HasField("volume")]
    rejected = [context for response, context in results
                if not response.HasField("volume")]
    assert len(successful) == 1
    assert len(rejected) == 1
    assert rejected[0].code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert used_capacity == requested_size
    assert maximum_active_checks == 1


@pytest.mark.parametrize("volume_id", [
    "bellagio\0vault",
    "bellagio\bvault",
    "bellagio\vvault",
    "bellagio\fvault",
    "bellagio\x0evault",
    "bellagio\x7fvault",
    "bellagio\x85vault",
    "b" * 129,
])
def test_controller_rpcs_reject_csi_banned_identifiers_before_lookup(
        monkeypatch, volume_id):
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    monkeypatch.setattr(controllerserver, "search_volume", _fail_if_called)
    monkeypatch.setattr(controllerserver, "delete_volume", _fail_if_called)

    calls = [
        (
            controllerserver.ControllerServer().CreateVolume,
            csi_pb2.CreateVolumeRequest(
                name=volume_id,
                capacity_range={"required_bytes": 20 * 1024 * 1024},
                volume_capabilities=[{
                    "mount": {},
                    "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
                }],
            ),
            csi_pb2.CreateVolumeResponse,
        ),
        (
            controllerserver.ControllerServer().DeleteVolume,
            csi_pb2.DeleteVolumeRequest(volume_id=volume_id),
            csi_pb2.DeleteVolumeResponse,
        ),
        (
            controllerserver.ControllerServer().ValidateVolumeCapabilities,
            csi_pb2.ValidateVolumeCapabilitiesRequest(volume_id=volume_id),
            csi_pb2.ValidateVolumeCapabilitiesResponse,
        ),
        (
            controllerserver.ControllerServer().ControllerExpandVolume,
            csi_pb2.ControllerExpandVolumeRequest(
                volume_id=volume_id,
                capacity_range={"required_bytes": 40 * 1024 * 1024},
            ),
            csi_pb2.ControllerExpandVolumeResponse,
        ),
    ]

    for handler, request, response_type in calls:
        context = FakeContext()
        response = handler(request, context)

        assert isinstance(response, response_type)
        assert context.code == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.parametrize("volume_id", [
    "bellagio\0vault",
    "bellagio\bvault",
    "bellagio\vvault",
    "bellagio\fvault",
    "bellagio\x0evault",
    "bellagio\x7fvault",
    "bellagio\x85vault",
])
def test_shared_volume_path_builder_rejects_csi_banned_identifiers(
        monkeypatch, volume_id):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    sys.modules.pop("kadalulib", None)
    kadalulib = importlib.import_module("kadalulib")

    with pytest.raises(ValueError, match="volume name"):
        kadalulib.get_volume_path("subvol", "deadbeef", volume_id)


@pytest.mark.parametrize("volume_id", [
    ".",
    "..",
    "../../the-mirage-counting-room",
    "the-italian/job",
    "/bellagio-vault",
    "bellagio\tvault",
    "bellagio\nvault",
    "bellagio\rvault",
    ".kadalu~the-sting",
])
def test_shared_volume_path_builder_encodes_and_decodes_opaque_identifiers(
        monkeypatch, volume_id):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    sys.modules.pop("kadalulib", None)
    kadalulib = importlib.import_module("kadalulib")

    volume_path = kadalulib.get_volume_path(
        "subvol",
        kadalulib.get_volname_hash(volume_id),
        volume_id,
    )
    component = os.path.basename(volume_path)

    assert component.startswith(kadalulib.VOLUME_COMPONENT_ENCODING_PREFIX)
    assert component not in (".", "..")
    assert "/" not in component
    assert kadalulib.volume_name_from_component(component) == volume_id


@pytest.mark.parametrize("volume_id", [
    ".",
    "../bellagio-vault",
    "oceans/eleven",
    "/bellagio-vault",
    "bellagio\tvault",
    "bellagio\nvault",
    "bellagio\rvault",
])
def test_controller_create_accepts_valid_opaque_volume_names(
        monkeypatch, volume_id):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    volume = volumeutils.Volume(
        volname=volume_id,
        voltype=volumeutils.PV_TYPE_SUBVOL,
        volhash=volumeutils.get_volname_hash(volume_id),
        hostvol="bellagio-pool",
        size=20 * 1024 * 1024,
        hostvoltype="Replica1",
    )
    lookups = []
    monkeypatch.setattr(
        controllerserver,
        "search_volume",
        lambda name: lookups.append(name) or volume,
    )
    monkeypatch.setattr(
        controllerserver,
        "reconcile_committed_capacity",
        lambda *_args: None,
    )
    context = FakeContext()

    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name=volume_id,
            capacity_range={"required_bytes": volume.size},
            volume_capabilities=[{
                "mount": {},
                "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
            }],
            parameters={
                "storage_name": volume.hostvol,
                "single_pv_per_pool": "false",
            },
        ),
        context,
    )

    assert context.code is None
    assert lookups == [volume_id, volume_id, volume_id]
    assert response.volume.volume_id == volume_id
    assert response.volume.volume_context["path"] == volume.volpath
    assert os.path.basename(volume.volpath).startswith(".kadalu~")


def test_shared_volume_path_builder_enforces_csi_identifier_size(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    sys.modules.pop("kadalulib", None)
    kadalulib = importlib.import_module("kadalulib")

    assert kadalulib.get_volume_path(
        "subvol",
        "deadbeef",
        "b" * 128,
    ).endswith("/" + "b" * 128)
    with pytest.raises(ValueError, match="volume name"):
        kadalulib.get_volume_path("subvol", "deadbeef", "b" * 129)
    assert kadalulib.get_volume_path(
        "subvol",
        "deadbeef",
        "é" * 64,
    ).endswith("/" + "é" * 64)
    with pytest.raises(ValueError, match="volume name"):
        kadalulib.get_volume_path("subvol", "deadbeef", "é" * 65)


@pytest.mark.parametrize("volume_id", [
    "bellagio;$(quoted)-`crew`",
    "café-de-la-banque",
])
def test_shared_volume_path_builder_preserves_safe_legacy_identifiers(
        monkeypatch, volume_id):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    sys.modules.pop("kadalulib", None)
    kadalulib = importlib.import_module("kadalulib")

    assert kadalulib.get_volume_path(
        "subvol", "deadbeef", volume_id
    ).endswith("/" + volume_id)


def test_encoded_volume_metadata_lists_the_original_opaque_identifier(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume_id = "../bellagio\ncounting-room"
    volume_path = volumeutils.get_volume_path(
        volumeutils.PV_TYPE_SUBVOL,
        volumeutils.get_volname_hash(volume_id),
        volume_id,
    )

    volumeutils.save_pv_metadata(
        str(tmp_path),
        volume_path,
        20 * 1024 * 1024,
    )

    metadata_path = tmp_path / "info" / f"{volume_path}.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    listed = [
        record
        for record in volumeutils.yield_pvc_from_mntdir(
            str(tmp_path / "info"),
        )
        if record is not None
    ]
    assert metadata["volume_id"] == volume_id
    assert len(listed) == 1
    assert listed[0]["name"] == volume_id
    assert listed[0]["volume_id"] == volume_id
    assert listed[0]["metadata_path"] == str(metadata_path)


@pytest.mark.parametrize("raw_block", [False, True])
def test_single_pv_pool_rejects_block_volume_before_lookup(
        monkeypatch, raw_block):
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    monkeypatch.setattr(controllerserver, "search_volume", _fail_if_called)
    capability = {
        "access_mode": {"mode": "SINGLE_NODE_WRITER"},
    }
    parameters = {"single_pv_per_pool": "true"}
    if raw_block:
        capability["block"] = {}
    else:
        capability["mount"] = {}
        parameters["pv_type"] = "block"

    context = FakeContext()
    response = controllerserver.ControllerServer().CreateVolume(
        csi_pb2.CreateVolumeRequest(
            name="pvc-bellagio-vault",
            capacity_range={"required_bytes": 20 * 1024 * 1024},
            volume_capabilities=[capability],
            parameters=parameters,
        ),
        context,
    )

    assert isinstance(response, csi_pb2.CreateVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "filesystem volumes only" in context.details


def test_remote_gluster_quota_quotes_every_shell_argument(monkeypatch):
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    commands = []
    monkeypatch.setattr(
        controllerserver,
        "reachable_host",
        lambda _hosts: "bellagio.example.invalid",
    )
    monkeypatch.setattr(
        controllerserver,
        "execute",
        lambda *command: commands.append(command),
    )

    assert controllerserver.execute_gluster_quota_command(
        "/run/secrets/the-italian-job",
        "danny-ocean",
        "bellagio.example.invalid",
        "bellagio; id",
        "subvol/ab/cd/pvc-bellagio-vault",
        20 * 1024 * 1024,
    ) is None

    assert len(commands) == 1
    assert len(commands[0]) == 6
    assert shlex.split(commands[0][-1]) == [
        "sudo",
        "gluster",
        "volume",
        "quota",
        "bellagio; id",
        "limit-usage",
        "/subvol/ab/cd/pvc-bellagio-vault",
        str(20 * 1024 * 1024 * 0.95),
    ]


def test_node_publish_reports_disconnected_fuse_source_as_unavailable(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    monkeypatch.setattr(nodeserver, "mount_glusterfs", lambda *_args: None)
    monkeypatch.setattr(
        nodeserver,
        "_validate_resolved_volume_path",
        lambda *_args: None,
    )

    @contextmanager
    def disconnected_source(*_args):
        raise OSError(errno.ENOTCONN, "the vault connection dropped")
        yield  # pragma: no cover

    monkeypatch.setattr(nodeserver, "_pinned_volume_source", disconnected_source)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": _generated_node_path(nodeserver),
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.UNAVAILABLE


def test_nomad_target_root_is_explicit_and_orchestrator_scoped(
        monkeypatch):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    job = (ROOT / "nomad" / "nodeplugin.nomad").read_text(encoding="utf-8")
    monkeypatch.setattr(nodeserver, "CSI_MOUNT_DIR", "/csi")

    assert 'CSI_MOUNT_DIR  = "/csi"' in job
    assert 'mount_dir = "/csi"' in job
    assert nodeserver._validate_target_path(
        "/csi/per-alloc/oceans-eleven/bellagio-vault",
    ) == "/csi/per-alloc/oceans-eleven/bellagio-vault"
    with pytest.raises(ValueError, match="outside"):
        nodeserver._validate_target_path(KUBELET_TARGET)


@pytest.mark.parametrize("hostvol", [
    "",
    ".",
    "..",
    "../bellagio-pool",
    "bellagio/pool",
    "/bellagio-pool",
    "bellagio\0pool",
])
def test_node_publish_rejects_unsafe_hostvol_before_mount(
        monkeypatch, hostvol):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)
    original_open = open

    def open_without_pool_lookup(path, *args, **kwargs):
        if os.fspath(path).endswith(".info"):
            raise AssertionError("Linus must reject the pool before file lookup")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", open_without_pool_lookup)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": hostvol,
            "pvtype": "subvol",
            "path": "subvol/ab/cd/pvc-bellagio-vault",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.parametrize("pvpath", [
    "/outside/bellagio-vault",
    "../outside/bellagio-vault",
    "subvol/../../outside/bellagio-vault",
    "subvol/../bellagio-vault",
    "subvol/ab/\0/pvc-bellagio-vault",
])
def test_node_publish_rejects_unsafe_pvpath_before_mount(
        monkeypatch, tmp_path, pvpath):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rejects_path_generated_for_another_volume(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": _generated_node_path(
                nodeserver,
                "pvc-mirage-counting-room",
            ),
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_does_not_trust_single_pv_request_flag(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": "",
            "single_pv_per_pool": "True",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rejects_empty_path_without_operator_pool_record(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    info_root = tmp_path / "missing-info"
    info_root.mkdir()
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(info_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "External",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": "",
            "single_pv_per_pool": "True",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rejects_pvpath_symlink_escape_before_bind_mount(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    outside_root = tmp_path / "outside"
    hostvol_root.mkdir(parents=True)
    outside_root.mkdir()
    pvpath = nodeserver.get_volume_path(
        "subvol",
        nodeserver.get_volname_hash("pvc-bellagio-vault"),
        "pvc-bellagio-vault",
    )
    (hostvol_root / "subvol").symlink_to(outside_root, target_is_directory=True)
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    host_mounts = []
    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        lambda *args: host_mounts.append(args),
    )
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert len(host_mounts) == 1
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rechecks_links_exposed_by_host_mount(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    outside_root = tmp_path / "outside"
    hostvol_root.mkdir(parents=True)
    outside_root.mkdir()

    pvpath = nodeserver.get_volume_path(
        "subvol",
        nodeserver.get_volname_hash("pvc-bellagio-vault"),
        "pvc-bellagio-vault",
    )

    def expose_remote_symlink(*_args):
        (hostvol_root / "subvol").symlink_to(
            outside_root,
            target_is_directory=True,
        )

    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", expose_remote_symlink)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rejects_symlink_to_another_path_inside_pool(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    hostvol_root.mkdir(parents=True)
    pvpath = _generated_node_path(nodeserver)
    source = hostvol_root / pvpath
    victim = hostvol_root / "the-mirage-counting-room"
    victim.mkdir()
    source.parent.mkdir(parents=True)
    source.symlink_to(victim, target_is_directory=True)
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", lambda *_args: None)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rejects_missing_controller_source(monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        lambda _volume, mountpoint, _is_client: Path(mountpoint).mkdir(),
    )
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": _generated_node_path(nodeserver),
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.NOT_FOUND
    assert not (mount_root / "bellagio-pool" /
                _generated_node_path(nodeserver)).exists()


def test_node_publish_rejects_source_type_that_does_not_match_pvtype(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    pvpath = _generated_node_path(nodeserver)
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))

    def expose_file_instead_of_subvolume(_volume, mountpoint, _is_client):
        source = Path(mountpoint) / pvpath
        source.parent.mkdir(parents=True)
        source.touch()

    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        expose_file_instead_of_subvolume,
    )
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_mounts_the_pinned_source_after_path_swap(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    pvpath = _generated_node_path(nodeserver)
    source = mount_root / "bellagio-pool" / pvpath
    moved_source = source.with_name("rusty-ryan-pinned-vault")
    decoy = source.with_name("terry-benedict-decoy")
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))

    def expose_source(_volume, _mountpoint, _is_client):
        source.mkdir(parents=True)

    def swap_source_then_mount(pinned_source, *_args, **_kwargs):
        source.rename(moved_source)
        decoy.mkdir()
        source.symlink_to(decoy, target_is_directory=True)
        assert os.path.samefile(pinned_source, moved_source)
        assert not os.path.samefile(pinned_source, decoy)
        return True

    monkeypatch.setattr(nodeserver, "mount_glusterfs", expose_source)
    monkeypatch.setattr(nodeserver, "mount_volume", swap_source_then_mount)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code is None


@pytest.mark.parametrize("pvtype", ["virtblock", "rawblock"])
def test_pinned_block_source_accepts_controller_file(
        monkeypatch, tmp_path, pvtype):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    hostvol_root = tmp_path / "bellagio-pool"
    pvpath = _generated_node_path(nodeserver, pvtype=pvtype)
    source = hostvol_root / pvpath
    source.parent.mkdir(parents=True)
    source.write_bytes(b"the italian job")

    with nodeserver._pinned_volume_source(
            str(hostvol_root), str(source), pvtype) as pinned_source:
        assert os.path.samefile(pinned_source, source)


@pytest.mark.parametrize("pvtype", ["virtblock", "rawblock"])
def test_pinned_block_source_rejects_directory(
        monkeypatch, tmp_path, pvtype):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    hostvol_root = tmp_path / "bellagio-pool"
    pvpath = _generated_node_path(nodeserver, pvtype=pvtype)
    source = hostvol_root / pvpath
    source.mkdir(parents=True)

    with pytest.raises(ValueError, match="wrong source type"):
        with nodeserver._pinned_volume_source(
                str(hostvol_root), str(source), pvtype):
            pass


def test_node_publish_rechecks_hostvol_link_after_mount(monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(monkeypatch, nodeserver, tmp_path)
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    outside_root = tmp_path / "outside"
    hostvol_root.mkdir(parents=True)
    outside_root.mkdir()

    pvpath = nodeserver.get_volume_path(
        "subvol",
        nodeserver.get_volname_hash("pvc-bellagio-vault"),
        "pvc-bellagio-vault",
    )

    def replace_mountpoint_with_symlink(*_args):
        hostvol_root.rmdir()
        hostvol_root.symlink_to(outside_root, target_is_directory=True)

    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        replace_mountpoint_with_symlink,
    )
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_volume_path_validation_does_not_probe_mounted_hosting_target(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    mntdir = mount_root / "bellagio-pool"
    pvpath = _generated_node_path(nodeserver)
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        nodeserver,
        "target_path_is_mounted",
        lambda path: os.fspath(path) == str(mntdir),
    )
    original_realpath = nodeserver.os.path.realpath

    def realpath_without_stale_mount_probe(path):
        if os.fspath(path) == str(mntdir):
            raise AssertionError("Rusty must not resolve the stale FUSE target")
        return original_realpath(path)

    monkeypatch.setattr(
        nodeserver.os.path,
        "realpath",
        realpath_without_stale_mount_probe,
    )

    assert nodeserver._validate_volume_paths(
        "pvc-bellagio-vault",
        "bellagio-pool",
        pvpath,
        "subvol",
        {"type": "Replica1", "single_pv_per_pool": False},
    ) == (str(mntdir), str(mntdir / pvpath))


def test_node_publish_rejects_hostvol_symlink_before_mount(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    outside_root = tmp_path / "outside"
    mount_root.mkdir()
    outside_root.mkdir()
    _write_node_pool_record(
        monkeypatch,
        nodeserver,
        tmp_path,
        single_pv_per_pool=True,
    )
    (mount_root / "bellagio-pool").symlink_to(
        outside_root,
        target_is_directory=True,
    )
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": "",
            "single_pv_per_pool": "True",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_accepts_generated_path_and_legacy_hostvol_name(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    _write_node_pool_record(
        monkeypatch,
        nodeserver,
        tmp_path,
        hostvol="Legacy_Bellagio.Pool",
    )
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    mounted = []
    pvpath = nodeserver.get_volume_path(
        "subvol",
        nodeserver.get_volname_hash("pvc-bellagio-vault"),
        "pvc-bellagio-vault",
    )
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))

    def mount_host(volume, mountpoint, is_client):
        mounted.append((volume, mountpoint, is_client))
        (Path(mountpoint) / pvpath).mkdir(parents=True)

    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        mount_host,
    )

    def mount_pv(source, target, pvtype, **kwargs):
        mounted.append((os.path.samefile(source, expected_source), target,
                        pvtype, kwargs))
        return True

    monkeypatch.setattr(
        nodeserver,
        "mount_volume",
        mount_pv,
    )

    expected_root = mount_root / "Legacy_Bellagio.Pool"
    expected_source = expected_root / pvpath
    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "Legacy_Bellagio.Pool",
            "pvtype": "subvol",
            "path": pvpath,
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert mounted[0][1:] == (str(expected_root), True)
    assert mounted[1] == (
        True,
        KUBELET_TARGET,
        "subvol",
        {
            "allow_legacy_host_mount": False,
            "fstype": None,
            "host_mount_identity": None,
            "host_volume_name": "Legacy_Bellagio.Pool",
            "logical_source_path": str(expected_source),
            "readonly": False,
            "mount_flags": [],
            "volume_path": pvpath,
            "volume_id": "pvc-bellagio-vault",
        },
    )
    assert context.code is None


@pytest.mark.parametrize("voltype", ["External", "Replica1"])
def test_node_publish_accepts_empty_path_for_single_pv_pool(
        monkeypatch, tmp_path, voltype):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    _write_node_pool_record(
        monkeypatch,
        nodeserver,
        tmp_path,
        voltype=voltype,
        single_pv_per_pool=True,
    )
    pv_mounts = []
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        lambda _volume, mountpoint, _is_client: Path(mountpoint).mkdir(),
    )
    adopted = []
    monkeypatch.setattr(
        nodeserver,
        "ensure_single_pv_claim",
        lambda mountpoint, volume_id, legacy_volume_id:
            adopted.append((mountpoint, volume_id, legacy_volume_id)) or True,
    )
    monkeypatch.setattr(
        nodeserver,
        "single_pv_claim_matches",
        lambda _mountpoint, _volume_id: True,
    )

    def mount_pv(source, *_args, **_kwargs):
        pv_mounts.append(os.path.samefile(source, mount_root / "bellagio-pool"))
        return True

    monkeypatch.setattr(
        nodeserver,
        "mount_volume",
        mount_pv,
    )

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": voltype,
            "hostvol": "bellagio-pool",
            "pvtype": "subvol",
            "path": "",
            "single_pv_per_pool": "True",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert pv_mounts == [True]
    assert adopted == [(
        str(mount_root / "bellagio-pool"),
        "pvc-bellagio-vault",
        None,
    )]
    assert context.code is None


@pytest.mark.parametrize("target_path", [
    "/etc/oceans-eleven-plans",
    "/var/lib/kubelet/pods/../secrets/bellagio-vault",
    "var/lib/kubelet/pods/bellagio-vault",
])
def test_node_publish_rejects_target_outside_host_mounts(
        monkeypatch, target_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    monkeypatch.setattr(nodeserver, "mount_glusterfs", _fail_if_called)
    monkeypatch.setattr(nodeserver, "mount_volume", _fail_if_called)
    request = _node_publish_request(csi_pb2, {
        "type": "Replica1",
        "hostvol": "bellagio-pool",
        "pvtype": "subvol",
        "path": _generated_node_path(nodeserver),
    })
    request.target_path = target_path

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(request, context)

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_target_validation_rejects_symlink_inside_kubelet_root(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    kubelet_root = tmp_path / "kubelet"
    pods_root = kubelet_root / "pods"
    outside = tmp_path / "the-mirage-private-vault"
    pods_root.mkdir(parents=True)
    outside.mkdir()
    (pods_root / "oceans-eleven").symlink_to(
        outside,
        target_is_directory=True,
    )
    monkeypatch.setattr(nodeserver, "KUBELET_DIR", str(kubelet_root))

    with pytest.raises(ValueError, match="symbolic links"):
        nodeserver._validate_target_path(
            str(pods_root / "oceans-eleven" / "bellagio-vault"),
        )


def test_target_validation_accepts_raw_block_plugin_root(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    kubelet_root = tmp_path / "kubelet"
    plugin_root = kubelet_root / "plugins" / "kubernetes.io" / "csi"
    plugin_root.mkdir(parents=True)
    target = plugin_root / "volumeDevices" / "publish" / "the-italian-job"
    monkeypatch.setattr(nodeserver, "KUBELET_DIR", str(kubelet_root))

    assert nodeserver._validate_target_path(str(target)) == str(target)


def test_node_unpublish_rejects_target_outside_host_mounts(monkeypatch):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    monkeypatch.setattr(nodeserver, "unmount_volume", _fail_if_called)

    context = FakeContext()
    response = nodeserver.NodeServer().NodeUnpublishVolume(
        csi_pb2.NodeUnpublishVolumeRequest(
            volume_id="pvc-bellagio-vault",
            target_path="/etc/oceans-eleven-plans",
        ),
        context,
    )

    assert isinstance(response, csi_pb2.NodeUnpublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_mount_volume_never_creates_missing_controller_source(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    source = tmp_path / "the-sting-missing-vault"
    target = tmp_path / "bellagio-target"
    monkeypatch.setattr(volumeutils, "execute", _fail_if_called)

    assert not volumeutils.mount_volume(
        str(source),
        str(target),
        volumeutils.PV_TYPE_SUBVOL,
    )
    assert not source.exists()


def test_node_unpublish_retains_shared_hosting_mount(monkeypatch):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    target = "/var/lib/kubelet/pods/oceans-eleven/volumes/bellagio-vault/mount"
    unmounted = []

    monkeypatch.setattr(nodeserver, "unmount_volume", unmounted.append)

    context = FakeContext()
    response = nodeserver.NodeServer().NodeUnpublishVolume(
        csi_pb2.NodeUnpublishVolumeRequest(
            volume_id="pvc-bellagio; steal-the-vault",
            target_path=target,
        ),
        context,
    )

    assert isinstance(response, csi_pb2.NodeUnpublishVolumeResponse)
    assert unmounted == [target]
    assert context.code is None


def test_mount_parser_decodes_proc_escapes(monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    mounts_file = tmp_path / "mounts"
    mounts_file.write_text(
        "kadalu:bellagio-vault /mnt/bellagio\\040vault "
        "fuse.glusterfs rw,relatime 0 0\n"
        "/dev/vault /not-gluster xfs rw 0 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(volumeutils, "MOUNTS_FILE", str(mounts_file))

    assert volumeutils._read_gluster_mounts() == [
        ("kadalu:bellagio-vault", "/mnt/bellagio vault"),
    ]


def test_process_match_treats_names_as_plain_arguments(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT))
    sys.modules.pop("lib.kadalulib", None)
    kadalulib = importlib.import_module("lib.kadalulib")

    args = [
        "/opt/sbin/glusterfs",
        "--volfile-id",
        "bellagio; steal-the-vault",
        "/mnt/bellagio $(steal-the-vault)",
    ]
    assert kadalulib._is_gluster_process(
        args,
        "bellagio; steal-the-vault",
        "/mnt/bellagio $(steal-the-vault)",
    )

    controller_args = [
        *args[:-1],
        "--fs-display-name",
        "kadalu:bellagio; steal-the-vault",
        args[-1],
        "--client-pid",
        "-14",
        "--volfile-server",
        "server-bellagio-vault-0",
    ]
    assert kadalulib._is_gluster_process(
        controller_args,
        "bellagio; steal-the-vault",
        "/mnt/bellagio $(steal-the-vault)",
    )

    restored_args = ["glusterfs", *args[1:]]
    assert kadalulib._is_gluster_process(
        restored_args,
        "bellagio; steal-the-vault",
        "/mnt/bellagio $(steal-the-vault)",
    )
