"""Safety and idempotency tests for CSI lifecycle operations."""

import importlib
import sys
from pathlib import Path

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[2]


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
    return importlib.import_module(name)


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


def _fail_if_called(*_args, **_kwargs):
    raise AssertionError("a non-expanding request must not mutate the vault")


def _node_publish_request(csi_pb2, volume_context):
    return csi_pb2.NodePublishVolumeRequest(
        volume_id="pvc-bellagio-vault",
        target_path="/target/bellagio-vault",
        volume_capability={
            "mount": {},
            "access_mode": {"mode": "MULTI_NODE_MULTI_WRITER"},
        },
        volume_context=volume_context,
    )


def test_expand_retry_never_shrinks_an_existing_volume(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        _fail_if_called,
    )
    monkeypatch.setattr(controllerserver, "update_subdir_volume", _fail_if_called)
    monkeypatch.setattr(controllerserver, "update_free_size", _fail_if_called)

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
    assert context.code is None


def test_create_retry_returns_the_original_volume_without_mutating(monkeypatch):
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
    monkeypatch.setattr(controllerserver, "update_free_size", _fail_if_called)

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

    with pytest.raises(OSError):
        controllerserver.ControllerServer().CreateVolume(
            request,
            FakeContext(),
        )

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
    assert accounting_attempts == 3
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

    assert stats["number_of_pvs"] == 1
    assert stats["used_size_bytes"] == pvsize


def test_expand_full_pool_returns_the_correct_response_type(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, _size: False,
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


def test_failed_external_quota_expansion_does_not_commit_metadata(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    existing.extra.update({
        "hostvoltype": "External",
        "ghost": "bellagio.example.invalid",
        "gvolname": "bellagio-vault",
    })

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, _size: True,
    )
    monkeypatch.setattr(controllerserver.os.path, "isfile", lambda _path: True)
    monkeypatch.setenv("SECRET_GLUSTERQUOTA_SSH_USERNAME", "danny-ocean")
    monkeypatch.setattr(
        controllerserver,
        "execute_gluster_quota_command",
        lambda *_args: "The remote vault rejected the crew",
    )
    monkeypatch.setattr(controllerserver, "update_pv_metadata", _fail_if_called)
    monkeypatch.setattr(controllerserver, "update_free_size", _fail_if_called)

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id="pvc-bellagio-vault",
            capacity_range={"required_bytes": 30 * 1024 * 1024},
        ),
        context,
    )

    assert response.capacity_bytes == existing.size
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


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


def test_node_publish_rejects_pvpath_symlink_escape_before_bind_mount(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    outside_root = tmp_path / "outside"
    hostvol_root.mkdir(parents=True)
    outside_root.mkdir()
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
            "path": "subvol/pvc-bellagio-vault",
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
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    outside_root = tmp_path / "outside"
    hostvol_root.mkdir(parents=True)
    outside_root.mkdir()

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
            "path": "subvol/pvc-bellagio-vault",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rechecks_hostvol_link_after_mount(monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    hostvol_root = mount_root / "bellagio-pool"
    outside_root = tmp_path / "outside"
    hostvol_root.mkdir(parents=True)
    outside_root.mkdir()

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
            "path": "subvol/ab/cd/pvc-bellagio-vault",
        }),
        context,
    )

    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT


def test_node_publish_rejects_hostvol_symlink_before_mount(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    outside_root = tmp_path / "outside"
    mount_root.mkdir()
    outside_root.mkdir()
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
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    mounted = []
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        lambda volume, mountpoint, is_client: mounted.append(
            (volume, mountpoint, is_client)
        ),
    )
    monkeypatch.setattr(
        nodeserver,
        "mount_volume",
        lambda *args, **kwargs: mounted.append((args, kwargs)) or True,
    )

    context = FakeContext()
    response = nodeserver.NodeServer().NodePublishVolume(
        _node_publish_request(csi_pb2, {
            "type": "Replica1",
            "hostvol": "Legacy_Bellagio.Pool",
            "pvtype": "subvol",
            "path": "subvol/ab/cd/pvc-bellagio-vault",
        }),
        context,
    )

    expected_root = mount_root / "Legacy_Bellagio.Pool"
    assert isinstance(response, csi_pb2.NodePublishVolumeResponse)
    assert mounted[0][1:] == (str(expected_root), True)
    assert mounted[1] == ((
        str(expected_root / "subvol/ab/cd/pvc-bellagio-vault"),
        "/target/bellagio-vault",
        "subvol",
    ), {"fstype": None})
    assert context.code is None


@pytest.mark.parametrize("voltype", ["External", "Replica1"])
def test_node_publish_accepts_empty_path_for_single_pv_pool(
        monkeypatch, tmp_path, voltype):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    pv_mounts = []
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", lambda *_args: None)
    monkeypatch.setattr(
        nodeserver,
        "mount_volume",
        lambda *args, **_kwargs: pv_mounts.append(args) or True,
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
    assert pv_mounts[0][0] == str(mount_root / "bellagio-pool")
    assert context.code is None


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
