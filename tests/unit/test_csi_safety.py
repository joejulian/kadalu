"""Safety and idempotency tests for CSI lifecycle operations."""

import importlib
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]
    assert context.code is None


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
        lambda _hostvol, _pvname: existing.size,
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


def test_create_retry_rejects_missing_reservation_when_pool_is_full(
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
        lambda _hostvol, size: size != existing.size,
    )
    monkeypatch.setattr(controllerserver, "update_free_size", _fail_if_called)

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

    assert not response.HasField("volume")
    assert context.code == grpc.StatusCode.RESOURCE_EXHAUSTED


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
        lambda _hostvol, _pvname: existing.size,
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
        lambda _path: SimpleNamespace(f_blocks=pvsize, f_bsize=1),
    )
    monkeypatch.setattr(
        volumeutils.os,
        "setxattr",
        lambda _path, name, value: events.append(("quota", name, value)),
    )
    monkeypatch.setattr(
        volumeutils,
        "save_pv_metadata",
        lambda _mount, _path, size: events.append(("metadata", size)),
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


def test_equal_size_expand_retry_repairs_quota_and_accounting(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    controllerserver = _load_csi_module(monkeypatch, "controllerserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    existing = _existing_volume(volumeutils)
    expanded_size = 30 * 1024 * 1024
    quota_updates = []
    accounting_attempts = 0

    monkeypatch.setattr(controllerserver, "search_volume", lambda _name: existing)
    monkeypatch.setattr(
        controllerserver,
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: expanded_size,
    )
    monkeypatch.setattr(
        controllerserver,
        "is_hosting_volume_free",
        lambda _hostvol, _size: True,
    )

    def fake_update_subdir(
            _mount, _hostvoltype, _pvname, size, update_metadata=True):
        quota_updates.append((size, update_metadata))
        if update_metadata:
            existing.size = size

    monkeypatch.setattr(
        controllerserver,
        "update_subdir_volume",
        fake_update_subdir,
    )

    def fake_update_free_size(_hostvol, _pvname, _sizechange):
        nonlocal accounting_attempts
        accounting_attempts += 1
        if accounting_attempts == 1:
            raise OSError("the casino ledger is temporarily unavailable")

    monkeypatch.setattr(
        controllerserver,
        "update_free_size",
        fake_update_free_size,
    )

    request = csi_pb2.ControllerExpandVolumeRequest(
        volume_id=existing.volname,
        capacity_range={"required_bytes": expanded_size},
    )

    with pytest.raises(OSError):
        controllerserver.ControllerServer().ControllerExpandVolume(
            request,
            FakeContext(),
        )

    response = controllerserver.ControllerServer().ControllerExpandVolume(
        request,
        FakeContext(),
    )

    assert response.capacity_bytes == expanded_size
    assert quota_updates == [
        (expanded_size, True),
        (expanded_size, False),
    ]
    assert accounting_attempts == 2


def test_equal_size_repair_rejects_missing_reservation_when_pool_is_full(
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

    def fake_is_free(_hostvol, size):
        capacity_checks.append(size)
        return False

    monkeypatch.setattr(controllerserver, "is_hosting_volume_free", fake_is_free)
    monkeypatch.setattr(controllerserver, "apply_subdir_quota", _fail_if_called)
    monkeypatch.setattr(controllerserver, "update_free_size", _fail_if_called)

    context = FakeContext()
    response = controllerserver.ControllerServer().ControllerExpandVolume(
        csi_pb2.ControllerExpandVolumeRequest(
            volume_id=existing.volname,
            capacity_range={"required_bytes": existing.size},
        ),
        context,
    )

    assert response.capacity_bytes == existing.size
    assert context.code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert capacity_checks == [existing.size]


def test_equal_size_repair_checks_only_unaccounted_delta(monkeypatch):
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
    assert capacity_checks == [existing.size - accounted_size]
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
    assert accounting_updates == [(
        existing.hostvol,
        existing.volname,
        -existing.size,
    )]


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
        "get_accounted_pv_size",
        lambda _hostvol, _pvname: existing.size,
    )
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


def test_node_publish_rejects_path_generated_for_another_volume(monkeypatch):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
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
    info_root = tmp_path / "info"
    info_root.mkdir()
    (info_root / "bellagio-pool.info").write_text(
        '{"single_pv_per_pool": false}',
        encoding="utf-8",
    )
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(info_root))
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
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert not (mount_root / "bellagio-pool" /
                _generated_node_path(nodeserver)).exists()


def test_node_publish_rejects_source_type_that_does_not_match_pvtype(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
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


def test_node_publish_rejects_hostvol_symlink_before_mount(
        monkeypatch, tmp_path):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    outside_root = tmp_path / "outside"
    mount_root.mkdir()
    outside_root.mkdir()
    info_root = tmp_path / "info"
    info_root.mkdir()
    (info_root / "bellagio-pool.info").write_text(
        '{"single_pv_per_pool": true}',
        encoding="utf-8",
    )
    (mount_root / "bellagio-pool").symlink_to(
        outside_root,
        target_is_directory=True,
    )
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(info_root))
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
        {"fstype": None},
    )
    assert context.code is None


@pytest.mark.parametrize("voltype", ["External", "Replica1"])
def test_node_publish_accepts_empty_path_for_single_pv_pool(
        monkeypatch, tmp_path, voltype):
    nodeserver = _load_csi_module(monkeypatch, "nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    mount_root = tmp_path / "mnt"
    mount_root.mkdir()
    info_root = tmp_path / "info"
    info_root.mkdir()
    (info_root / "bellagio-pool.info").write_text(
        '{"volname": "bellagio-pool", "single_pv_per_pool": true}',
        encoding="utf-8",
    )
    pv_mounts = []
    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(info_root))
    monkeypatch.setattr(
        nodeserver,
        "mount_glusterfs",
        lambda _volume, mountpoint, _is_client: Path(mountpoint).mkdir(),
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
