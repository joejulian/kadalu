"""Safety and idempotency tests for CSI lifecycle operations."""

import importlib
import sys
from pathlib import Path

import grpc


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


def test_node_unpublish_never_executes_volume_handle_as_shell(monkeypatch):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    target = "/var/lib/kubelet/pods/oceans-eleven/volumes/bellagio-vault/mount"
    mount_snapshots = iter([
        [
            ("kadalu:bellagio-pool", "/mnt/bellagio-pool"),
            ("kadalu:bellagio-pool", target),
        ],
        [("kadalu:bellagio-pool", "/mnt/bellagio-pool")],
    ])
    unmounted = []

    monkeypatch.setattr(nodeserver, "_read_gluster_mounts", lambda: next(mount_snapshots))
    monkeypatch.setattr(nodeserver, "unmount_volume", unmounted.append)
    monkeypatch.setattr(
        nodeserver,
        "unmount_glusterfs",
        lambda mountpoint, volume: unmounted.append((mountpoint, volume)),
    )

    context = FakeContext()
    response = nodeserver.NodeServer().NodeUnpublishVolume(
        csi_pb2.NodeUnpublishVolumeRequest(
            volume_id="pvc-bellagio; steal-the-vault",
            target_path=target,
        ),
        context,
    )

    assert isinstance(response, csi_pb2.NodeUnpublishVolumeResponse)
    assert unmounted == [
        target,
        ("/mnt/bellagio-pool", "bellagio-pool"),
    ]
    assert context.code is None


def test_mount_parser_decodes_proc_escapes(monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    mounts_file = tmp_path / "mounts"
    mounts_file.write_text(
        "kadalu:bellagio-vault /mnt/bellagio\\040vault "
        "fuse.glusterfs rw,relatime 0 0\n"
        "/dev/vault /not-gluster xfs rw 0 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(nodeserver, "MOUNTS_FILE", str(mounts_file))

    assert nodeserver._read_gluster_mounts() == [
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
