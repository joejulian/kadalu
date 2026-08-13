"""Durable ownership tests for pools exposed as one whole CSI volume."""

import importlib
import errno
import json
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[2]
POOL_NAME = "bellagio-pool"
VOLUME_ID = "pvc-danny-ocean"
OTHER_VOLUME_ID = "pvc-rusty-ryan"
VOLUME_SIZE = 20 * 1024 * 1024


class FakeContext:
    """Capture status information set by a CSI handler."""

    def __init__(self):
        self.code = None
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


def _fail_if_called(*_args, **_kwargs):
    raise AssertionError("the occupied Bellagio pool must not be reselected")


def _load_csi_stack(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    for module_name in ("controllerserver", "nodeserver", "volumeutils"):
        sys.modules.pop(module_name, None)

    volumeutils = importlib.import_module("volumeutils")
    controllerserver = importlib.import_module("controllerserver")
    nodeserver = importlib.import_module("nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    return volumeutils, controllerserver, nodeserver, csi_pb2


def _configure_pool(monkeypatch, tmp_path, *, pool_type="Replica1",
                    reclaim_policy="delete"):
    volumeutils, controllerserver, nodeserver, csi_pb2 = _load_csi_stack(
        monkeypatch,
    )
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / POOL_NAME
    info_dir = pool_root / "info"
    info_dir.mkdir(parents=True)
    volinfo_dir = tmp_path / "volinfo"
    volinfo_dir.mkdir()

    pool = {
        "name": POOL_NAME,
        "type": pool_type,
        "single_pv_per_pool": True,
        "single_pv_claim_version": 1,
        "node_affinity": None,
    }
    pool_record = {
        "volname": POOL_NAME,
        "type": pool_type,
        "single_pv_per_pool": True,
        "single_pv_claim_version": 1,
        "pvReclaimPolicy": reclaim_policy,
    }
    if pool_type == "External":
        pool.update({
            "g_host": "bellagio.example.invalid",
            "g_volname": "bellagio-vault",
            "g_options": "log-level=WARNING",
        })
        pool_record.update({
            "gluster_hosts": pool["g_host"],
            "gluster_volname": pool["g_volname"],
            "gluster_options": pool["g_options"],
        })

    (volinfo_dir / f"{POOL_NAME}.info").write_text(
        json.dumps(pool_record),
        encoding="utf-8",
    )
    (volinfo_dir / "uid").write_text(
        "the-bellagio-crew",
        encoding="utf-8",
    )

    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(volumeutils, "VOLINFO_DIR", str(volinfo_dir))
    monkeypatch.setattr(
        volumeutils,
        "get_pv_hosting_volumes",
        lambda _filters: [pool.copy()],
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
    pool_stats = SimpleNamespace(
        f_blocks=VOLUME_SIZE // 4096,
        f_bsize=4096,
        f_frsize=4096,
    )
    monkeypatch.setattr(volumeutils.os, "statvfs", lambda _path: pool_stats)
    xattrs = {}

    def getxattr(path, name):
        try:
            return xattrs[(str(path), name)]
        except KeyError as err:
            raise OSError(errno.ENODATA, "No data available") from err

    def setxattr(path, name, value, flags=0):
        key = (str(path), name)
        if flags == volumeutils.os.XATTR_CREATE and key in xattrs:
            raise OSError(errno.EEXIST, "File exists")
        if flags == volumeutils.os.XATTR_REPLACE and key not in xattrs:
            raise OSError(errno.ENODATA, "No data available")
        xattrs[key] = value

    def removexattr(path, name):
        try:
            del xattrs[(str(path), name)]
        except KeyError as err:
            raise OSError(errno.ENODATA, "No data available") from err

    monkeypatch.setattr(volumeutils.os, "getxattr", getxattr)
    monkeypatch.setattr(volumeutils.os, "setxattr", setxattr)
    monkeypatch.setattr(volumeutils.os, "removexattr", removexattr)

    monkeypatch.setattr(controllerserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(controllerserver, "VOLINFO_DIR", str(volinfo_dir))
    monkeypatch.setattr(
        controllerserver,
        "CREATE_IDENTITY_LOCK_DIR",
        str(tmp_path / "controller-locks"),
    )
    monkeypatch.setattr(
        controllerserver,
        "get_pv_hosting_volumes",
        lambda _filters: [pool.copy()],
    )
    monkeypatch.setattr(
        controllerserver,
        "mount_and_select_hosting_volume",
        lambda _volumes, _size: POOL_NAME,
    )
    monkeypatch.setattr(
        controllerserver,
        "send_analytics_tracker",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        controllerserver,
        "unmount_glusterfs",
        lambda *_args: None,
    )
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(volinfo_dir))

    return {
        "volumeutils": volumeutils,
        "controllerserver": controllerserver,
        "nodeserver": nodeserver,
        "csi_pb2": csi_pb2,
        "pool": pool,
        "pool_root": pool_root,
        "xattrs": xattrs,
        "volinfo_dir": volinfo_dir,
    }


def _create_request(csi_pb2, volume_id=VOLUME_ID, *, pool_type="Replica1"):
    parameters = {
        "single_pv_per_pool": "true",
    }
    if pool_type == "External":
        parameters.update({
            "hostvol_type": "External",
            "gluster_hosts": "bellagio.example.invalid",
            "gluster_volname": "bellagio-vault",
            "gluster_options": "log-level=WARNING",
        })
    else:
        parameters["storage_name"] = POOL_NAME

    return csi_pb2.CreateVolumeRequest(
        name=volume_id,
        capacity_range={"required_bytes": VOLUME_SIZE},
        volume_capabilities=[{
            "mount": {},
            "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
        }],
        parameters=parameters,
    )


def _write_claim(env, volume_id=VOLUME_ID):
    claim = {
        "version": 1,
        "volume_id": volume_id,
        "size": VOLUME_SIZE,
        "pvtype": "subvol",
        "single_pv_per_pool": True,
        "state": "active",
    }
    env["xattrs"][(
        str(env["pool_root"]),
        env["volumeutils"].SINGLE_PV_CLAIM_XATTR,
    )] = json.dumps(claim).encode("utf-8")
    return claim


def _read_claim(env):
    return env["volumeutils"].read_single_pv_claim(str(env["pool_root"]))


def _store_claim(env, claim):
    env["xattrs"][(
        str(env["pool_root"]),
        env["volumeutils"].SINGLE_PV_CLAIM_XATTR,
    )] = json.dumps(claim).encode("utf-8")


def _mark_pool_legacy(env, volume_id=None):
    """Rewrite the fixture as a pre-ownership-marker pool."""
    env["pool"].pop("single_pv_claim_version", None)
    info_path = env["volinfo_dir"] / f"{POOL_NAME}.info"
    record = json.loads(info_path.read_text(encoding="utf-8"))
    record.pop("single_pv_claim_version", None)
    if volume_id is not None:
        record["legacy_single_pv_volume_id"] = volume_id
        env["pool"]["legacy_single_pv_volume_id"] = volume_id
    info_path.write_text(json.dumps(record), encoding="utf-8")


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_create_persists_claim_and_search_reads_it(
        monkeypatch, tmp_path, pool_type):
    env = _configure_pool(monkeypatch, tmp_path, pool_type=pool_type)
    context = FakeContext()

    response = env["controllerserver"].ControllerServer().CreateVolume(
        _create_request(env["csi_pb2"], pool_type=pool_type),
        context,
    )

    assert context.code is None
    assert response.volume.volume_id == VOLUME_ID
    assert _read_claim(env) == {
        "version": 1,
        "volume_id": VOLUME_ID,
        "size": VOLUME_SIZE,
        "pvtype": "subvol",
        "single_pv_per_pool": True,
        "state": "active",
    }

    found = env["volumeutils"].search_volume(VOLUME_ID)
    assert found is not None
    assert found.volname == VOLUME_ID
    assert found.hostvol == POOL_NAME
    assert found.volpath == ""
    assert found.size == VOLUME_SIZE
    assert found.voltype == env["volumeutils"].PV_TYPE_SUBVOL
    assert found.single_pv_per_pool is True


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_same_volume_id_create_is_an_idempotent_retry(
        monkeypatch, tmp_path, pool_type):
    env = _configure_pool(monkeypatch, tmp_path, pool_type=pool_type)
    server = env["controllerserver"].ControllerServer()
    request = _create_request(env["csi_pb2"], pool_type=pool_type)

    first = server.CreateVolume(request, FakeContext())
    original_claim = _read_claim(env)
    monkeypatch.setattr(
        env["controllerserver"],
        "mount_and_select_hosting_volume",
        _fail_if_called,
    )
    monkeypatch.setattr(
        env["controllerserver"],
        "check_external_volume",
        _fail_if_called,
    )
    retry_context = FakeContext()

    retry = server.CreateVolume(request, retry_context)

    assert first.volume.volume_id == retry.volume.volume_id == VOLUME_ID
    assert retry.volume.capacity_bytes == VOLUME_SIZE
    assert retry_context.code is None
    assert _read_claim(env) == original_claim


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_different_volume_id_cannot_claim_an_occupied_pool(
        monkeypatch, tmp_path, pool_type):
    env = _configure_pool(monkeypatch, tmp_path, pool_type=pool_type)
    server = env["controllerserver"].ControllerServer()
    first_context = FakeContext()
    first = server.CreateVolume(
        _create_request(env["csi_pb2"], pool_type=pool_type),
        first_context,
    )
    assert first.volume.volume_id == VOLUME_ID
    original_claim = _read_claim(env)

    other_context = FakeContext()
    other = server.CreateVolume(
        _create_request(
            env["csi_pb2"],
            OTHER_VOLUME_ID,
            pool_type=pool_type,
        ),
        other_context,
    )

    assert not other.HasField("volume")
    assert other_context.code in {
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
    assert _read_claim(env) == original_claim


def test_whole_pool_create_refuses_existing_controller_metadata(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    existing_volume_id = "pvc-linus-caldwell"
    volume_hash = env["volumeutils"].get_volname_hash(existing_volume_id)
    existing_path = env["volumeutils"].get_volume_path(
        env["volumeutils"].PV_TYPE_SUBVOL,
        volume_hash,
        existing_volume_id,
    )
    payload = env["pool_root"] / existing_path
    payload.mkdir(parents=True)
    evidence = payload / "bellagio-crew-ledger.txt"
    evidence.write_text("This fake vault is already allocated", encoding="utf-8")
    metadata = env["pool_root"] / "info" / f"{existing_path}.json"
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text(
        json.dumps({
            "path_prefix": str(Path(existing_path).parent),
            "size": VOLUME_SIZE,
        }),
        encoding="utf-8",
    )
    context = FakeContext()

    response = env["controllerserver"].ControllerServer().CreateVolume(
        _create_request(env["csi_pb2"], OTHER_VOLUME_ID),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert _read_claim(env) is None
    assert metadata.is_file()
    assert evidence.read_text(encoding="utf-8") == (
        "This fake vault is already allocated"
    )


def test_whole_pool_create_refuses_unmanaged_existing_payload(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    evidence = env["pool_root"] / "benedicts-unmanaged-vault-ledger.txt"
    evidence.write_text(
        "This fake vault survived a missing ConfigMap",
        encoding="utf-8",
    )
    context = FakeContext()

    response = env["controllerserver"].ControllerServer().CreateVolume(
        _create_request(env["csi_pb2"], OTHER_VOLUME_ID),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert _read_claim(env) is None
    assert evidence.read_text(encoding="utf-8") == (
        "This fake vault survived a missing ConfigMap"
    )


@pytest.mark.parametrize("claim_state", ["deleting", "retired"])
@pytest.mark.parametrize("requested_volume_id", [VOLUME_ID, OTHER_VOLUME_ID])
def test_whole_pool_create_never_reuses_a_non_active_claim(
        monkeypatch, tmp_path, claim_state, requested_volume_id):
    env = _configure_pool(monkeypatch, tmp_path)
    claim = _write_claim(env)
    claim["state"] = claim_state
    _store_claim(env, claim)
    context = FakeContext()

    response = env["controllerserver"].ControllerServer().CreateVolume(
        _create_request(env["csi_pb2"], requested_volume_id),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert _read_claim(env) == claim


def test_corrupt_claim_marker_fails_closed(monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    corrupt_marker = b"{the-bellagio-job-went-wrong"
    env["xattrs"][(
        str(env["pool_root"]),
        env["volumeutils"].SINGLE_PV_CLAIM_XATTR,
    )] = corrupt_marker
    context = FakeContext()

    response = env["controllerserver"].ControllerServer().CreateVolume(
        _create_request(env["csi_pb2"]),
        context,
    )

    assert not response.HasField("volume")
    assert context.code is not None
    assert env["xattrs"][(
        str(env["pool_root"]),
        env["volumeutils"].SINGLE_PV_CLAIM_XATTR,
    )] == corrupt_marker


def test_create_refuses_unowned_legacy_single_pv_pool(monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    _mark_pool_legacy(env)
    context = FakeContext()

    response = env["controllerserver"].ControllerServer().CreateVolume(
        _create_request(env["csi_pb2"]),
        context,
    )

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert _read_claim(env) is None


def test_delete_adopts_operator_identified_legacy_owner_before_purge(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path, reclaim_policy="delete")
    _mark_pool_legacy(env, VOLUME_ID)
    payload = env["pool_root"] / "the-italian-job-gold.txt"
    payload.write_text("obviously fake gold", encoding="utf-8")

    env["volumeutils"].delete_volume(VOLUME_ID)

    assert not payload.exists()
    assert _read_claim(env)["volume_id"] == VOLUME_ID
    assert _read_claim(env)["state"] == "retired"


def test_delete_policy_purges_data_then_tombstones_claim(monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path, reclaim_policy="delete")
    _write_claim(env)
    payload = env["pool_root"] / "bellagio-casino-ledger.txt"
    payload.write_text("Terry Benedict's obviously fake ledger", encoding="utf-8")
    unrelated_info = env["pool_root"] / "info" / "crew-roster.txt"
    unrelated_info.write_text("Danny Ocean\nRusty Ryan\n", encoding="utf-8")

    env["volumeutils"].delete_volume(VOLUME_ID)

    assert _read_claim(env) == {
        "version": 1,
        "volume_id": VOLUME_ID,
        "size": VOLUME_SIZE,
        "pvtype": "subvol",
        "single_pv_per_pool": True,
        "state": "retired",
    }
    assert not payload.exists()
    assert not unrelated_info.exists()
    assert env["pool_root"].is_dir()


def test_interrupted_delete_stays_blocked_and_retry_resumes(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path, reclaim_policy="delete")
    _write_claim(env)
    payload = env["pool_root"] / "casino-ledger-from-the-vault.txt"
    payload.write_text("obviously fake bearer bonds", encoding="utf-8")
    original_purge = env["volumeutils"].purge_single_pv_pool

    def interrupt_purge(_hostvol_mnt):
        assert _read_claim(env)["state"] == "deleting"
        raise OSError(errno.EIO, "the Bellagio drill jammed")

    monkeypatch.setattr(
        env["volumeutils"], "purge_single_pv_pool", interrupt_purge
    )
    with pytest.raises(OSError, match="Bellagio drill jammed"):
        env["volumeutils"].delete_volume(VOLUME_ID)

    assert _read_claim(env)["state"] == "deleting"
    assert payload.exists()

    monkeypatch.setattr(
        env["volumeutils"], "purge_single_pv_pool", original_purge
    )
    env["volumeutils"].delete_volume(VOLUME_ID)

    assert _read_claim(env)["state"] == "retired"
    assert not payload.exists()


def test_single_pv_claim_waits_for_the_full_delete_transaction(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path, reclaim_policy="delete")
    _write_claim(env)
    volumeutils = env["volumeutils"]
    original_purge = volumeutils.purge_single_pv_pool
    delete_entered_purge = threading.Event()
    release_delete = threading.Event()
    claim_finished = threading.Event()
    claim_errors = []

    def blocking_purge(hostvol_mnt):
        delete_entered_purge.set()
        assert release_delete.wait(timeout=2)
        original_purge(hostvol_mnt)

    def claim_other_volume():
        try:
            volumeutils.claim_single_pv_volume(
                str(env["pool_root"]),
                OTHER_VOLUME_ID,
                VOLUME_SIZE,
            )
        except volumeutils.SinglePVPoolRetiredError as err:
            claim_errors.append(err)
        finally:
            claim_finished.set()

    monkeypatch.setattr(volumeutils, "purge_single_pv_pool", blocking_purge)
    delete_thread = threading.Thread(
        target=volumeutils.delete_volume,
        args=(VOLUME_ID,),
    )
    claim_thread = threading.Thread(target=claim_other_volume)

    delete_thread.start()
    assert delete_entered_purge.wait(timeout=2)
    claim_thread.start()
    assert not claim_finished.wait(timeout=0.1)

    release_delete.set()
    delete_thread.join(timeout=2)
    claim_thread.join(timeout=2)

    assert not delete_thread.is_alive()
    assert not claim_thread.is_alive()
    assert len(claim_errors) == 1
    assert isinstance(claim_errors[0], volumeutils.SinglePVPoolRetiredError)
    assert _read_claim(env)["state"] == "retired"


def test_retain_policy_preserves_claim_and_pool_payload(monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path, reclaim_policy="retain")
    claim = _write_claim(env)
    payload = env["pool_root"] / "mirage-vault-inventory.txt"
    payload.write_text("Three very fake Faberge eggs", encoding="utf-8")

    env["volumeutils"].delete_volume(VOLUME_ID)

    assert _read_claim(env) == claim
    assert payload.read_text(encoding="utf-8") == (
        "Three very fake Faberge eggs"
    )


def _node_publish_request(csi_pb2, target_path, volume_id):
    return csi_pb2.NodePublishVolumeRequest(
        volume_id=volume_id,
        target_path=target_path,
        volume_capability={
            "mount": {},
            "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
        },
        volume_context={
            "type": "Replica1",
            "hostvol": POOL_NAME,
            "pvtype": "subvol",
            "path": "",
            "single_pv_per_pool": "True",
        },
    )


def _prepare_node_publish(monkeypatch, tmp_path, env):
    kubelet_root = tmp_path / "kubelet"
    (kubelet_root / "pods").mkdir(parents=True)
    monkeypatch.setattr(env["nodeserver"], "KUBELET_DIR", str(kubelet_root))
    monkeypatch.setattr(env["nodeserver"], "CSI_MOUNT_DIR", None)
    monkeypatch.setattr(
        env["nodeserver"],
        "mount_glusterfs",
        lambda *_args: None,
    )
    publishes = []
    monkeypatch.setattr(
        env["nodeserver"],
        "mount_volume",
        lambda source, target, pvtype, **_kwargs:
            publishes.append((source, target, pvtype)) or True,
    )
    target = str(
        kubelet_root / "pods" / "eleven-crew" / "volumes"
        / "kubernetes.io~csi" / "bellagio-vault" / "mount"
    )
    return publishes, target


def test_node_publish_binds_whole_pool_to_claimed_volume_id(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    _write_claim(env)
    publishes, target = _prepare_node_publish(monkeypatch, tmp_path, env)

    matching_context = FakeContext()
    matching = env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, VOLUME_ID),
        matching_context,
    )

    assert isinstance(matching, env["csi_pb2"].NodePublishVolumeResponse)
    assert matching_context.code is None
    assert len(publishes) == 1

    publishes.clear()
    mismatched_context = FakeContext()
    mismatched = env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, OTHER_VOLUME_ID),
        mismatched_context,
    )

    assert isinstance(mismatched, env["csi_pb2"].NodePublishVolumeResponse)
    assert mismatched_context.code is not None
    assert publishes == []


def test_node_publish_adopts_only_operator_identified_legacy_pool(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    _mark_pool_legacy(env, VOLUME_ID)
    publishes, target = _prepare_node_publish(monkeypatch, tmp_path, env)
    context = FakeContext()

    response = env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, VOLUME_ID),
        context,
    )

    assert isinstance(response, env["csi_pb2"].NodePublishVolumeResponse)
    assert context.code is None
    assert len(publishes) == 1
    assert _read_claim(env) == {
        "version": 1,
        "volume_id": VOLUME_ID,
        "size": VOLUME_SIZE,
        "pvtype": "subvol",
        "single_pv_per_pool": True,
        "state": "active",
    }


def test_node_publish_rejects_unmarked_pool_without_authoritative_owner(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    _mark_pool_legacy(env)
    publishes, target = _prepare_node_publish(monkeypatch, tmp_path, env)
    context = FakeContext()

    env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, VOLUME_ID),
        context,
    )

    assert context.code == grpc.StatusCode.NOT_FOUND
    assert publishes == []
    assert _read_claim(env) is None


def test_node_publish_never_re_adopts_retired_legacy_pool(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    claim = _write_claim(env)
    claim["state"] = "retired"
    env["xattrs"][(
        str(env["pool_root"]),
        env["volumeutils"].SINGLE_PV_CLAIM_XATTR,
    )] = json.dumps(claim).encode("utf-8")
    publishes, target = _prepare_node_publish(monkeypatch, tmp_path, env)
    context = FakeContext()

    response = env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, OTHER_VOLUME_ID),
        context,
    )

    assert isinstance(response, env["csi_pb2"].NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.NOT_FOUND
    assert publishes == []
    assert _read_claim(env) == claim


def test_node_publish_rejects_pool_while_delete_is_in_progress(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    claim = _write_claim(env)
    claim["state"] = "deleting"
    env["xattrs"][(
        str(env["pool_root"]),
        env["volumeutils"].SINGLE_PV_CLAIM_XATTR,
    )] = json.dumps(claim).encode("utf-8")
    publishes, target = _prepare_node_publish(monkeypatch, tmp_path, env)
    context = FakeContext()

    env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, VOLUME_ID),
        context,
    )

    assert context.code == grpc.StatusCode.NOT_FOUND
    assert publishes == []
    assert _read_claim(env) == claim


def test_node_publish_rechecks_whole_pool_claim_immediately_before_mount(
        monkeypatch, tmp_path):
    env = _configure_pool(monkeypatch, tmp_path)
    claim = _write_claim(env)
    publishes, target = _prepare_node_publish(monkeypatch, tmp_path, env)

    @contextmanager
    def delete_begins_after_source_is_pinned(*_args):
        claim["state"] = "deleting"
        _store_claim(env, claim)
        yield str(env["pool_root"])

    monkeypatch.setattr(
        env["nodeserver"],
        "_pinned_volume_source",
        delete_begins_after_source_is_pinned,
    )
    context = FakeContext()

    response = env["nodeserver"].NodeServer().NodePublishVolume(
        _node_publish_request(env["csi_pb2"], target, VOLUME_ID),
        context,
    )

    assert isinstance(response, env["csi_pb2"].NodePublishVolumeResponse)
    assert context.code in {
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.NOT_FOUND,
    }
    assert publishes == []
    assert _read_claim(env)["state"] == "deleting"
