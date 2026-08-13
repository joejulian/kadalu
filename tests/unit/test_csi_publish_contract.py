"""CSI publish and unpublish contract tests."""

import errno
import importlib
import json
import os
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest


ROOT = Path(__file__).resolve().parents[2]
POOL_NAME = "bellagio-pool"
VOLUME_ID = "pvc-danny-ocean"
MOUNT_GENERATION = "12345678-1234-5678-9234-567812345678"


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
    raise AssertionError("the Bellagio vault operation must stop here")


def _mountinfo_escape(value):
    """Encode the procfs separators used by the fake heist mount table."""
    return str(value).replace("\\", r"\134").replace(" ", r"\040")


def _write_mountinfo(monkeypatch, volumeutils, path, entries):
    """Install an isolated mountinfo table for one fake casino heist."""
    lines = []
    for index, entry in enumerate(entries, start=700):
        lines.append(
            "%s 48 %s %s %s %s - %s %s %s\n" % (
                index,
                entry.get("device", "0:88"),
                _mountinfo_escape(entry.get("root", "/")),
                _mountinfo_escape(entry["target"]),
                entry.get("options", "rw,relatime"),
                entry.get("fstype", "fuse.glusterfs"),
                _mountinfo_escape(entry.get("source", "kadalu:bellagio-pool")),
                entry.get("super_options", "rw"),
            )
        )
    path.write_text("".join(lines), encoding="utf-8")
    monkeypatch.setattr(volumeutils, "MOUNTINFO_FILE", str(path))


def _load_csi_stack(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    for module_name in ("nodeserver", "volumeutils"):
        sys.modules.pop(module_name, None)

    volumeutils = importlib.import_module("volumeutils")
    nodeserver = importlib.import_module("nodeserver")
    csi_pb2 = importlib.import_module("csi_pb2")
    return volumeutils, nodeserver, csi_pb2


def _prepare_publish(monkeypatch, tmp_path, pvtype, *, source_exists=True):
    volumeutils, nodeserver, csi_pb2 = _load_csi_stack(monkeypatch)
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / POOL_NAME
    pool_root.mkdir(parents=True)
    volume_path = nodeserver.get_volume_path(
        pvtype,
        nodeserver.get_volname_hash(VOLUME_ID),
        VOLUME_ID,
    )
    source = pool_root / volume_path
    if source_exists:
        source.parent.mkdir(parents=True)
        if pvtype == volumeutils.PV_TYPE_SUBVOL:
            source.mkdir()
        else:
            source.write_bytes(b"The Italian Job's obviously fake vault image")

    kubelet_root = tmp_path / "kubelet"
    if pvtype == volumeutils.PV_TYPE_RAWBLOCK:
        target_root = (
            kubelet_root / "plugins" / "kubernetes.io" / "csi"
            / "volumeDevices" / "publish"
        )
    else:
        target_root = (
            kubelet_root / "pods" / "oceans-eleven" / "volumes"
            / "kubernetes.io~csi" / "bellagio-vault"
        )
    target_root.mkdir(parents=True)
    target = target_root / ("device" if pvtype == volumeutils.PV_TYPE_RAWBLOCK
                            else "mount")

    monkeypatch.setattr(nodeserver, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(nodeserver, "KUBELET_DIR", str(kubelet_root))
    monkeypatch.setattr(nodeserver, "CSI_MOUNT_DIR", None)
    info_root = tmp_path / "info"
    info_root.mkdir()
    (info_root / f"{POOL_NAME}.info").write_text(
        json.dumps({
            "bricks": [],
            "single_pv_per_pool": False,
            "type": "Replica1",
            "volname": POOL_NAME,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(nodeserver, "VOLINFO_DIR", str(info_root))
    monkeypatch.setattr(nodeserver, "mount_glusterfs", lambda *_args: None)

    return SimpleNamespace(
        volumeutils=volumeutils,
        nodeserver=nodeserver,
        csi_pb2=csi_pb2,
        source=source,
        target=target,
        volume_path=volume_path,
    )


def _publish_request(env, *, readonly=False, mount_flags=()):
    if env.source.is_dir():
        access_type = {
            "mount": {
                "fs_type": "",
                "mount_flags": list(mount_flags),
            },
        }
    elif "volumeDevices" in str(env.target):
        access_type = {"block": {}}
    else:
        access_type = {
            "mount": {
                "fs_type": "xfs",
                "mount_flags": list(mount_flags),
            },
        }

    return env.csi_pb2.NodePublishVolumeRequest(
        volume_id=VOLUME_ID,
        target_path=str(env.target),
        readonly=readonly,
        volume_capability={
            **access_type,
            "access_mode": {
                "mode": (
                    "MULTI_NODE_READER_ONLY"
                    if readonly
                    else "MULTI_NODE_MULTI_WRITER"
                ),
            },
        },
        volume_context={
            "type": "Replica1",
            "hostvol": POOL_NAME,
            "pvtype": env.source.parts[-4],
            "path": env.volume_path,
            "single_pv_per_pool": "False",
        },
    )


def _set_existing_target(monkeypatch, env, *, same_source, samefile_error=None,
                         readonly=False):
    if "volumeDevices" in str(env.target):
        env.target.touch()
    else:
        env.target.mkdir()

    def samefile(_source, _target):
        if samefile_error is not None:
            raise samefile_error
        return same_source

    monkeypatch.setattr(env.volumeutils.os.path, "samefile", samefile)
    if samefile_error is not None and env.source.is_file():
        def fail_backing_identity(_path):
            raise samefile_error

        monkeypatch.setattr(
            env.volumeutils,
            "_path_backing_identity",
            fail_backing_identity,
        )
    source_stat = env.source.stat()
    backing_inode = source_stat.st_ino if same_source else source_stat.st_ino + 1
    backing_device = "%s:%s" % (
        os.major(source_stat.st_dev),
        os.minor(source_stat.st_dev),
    )
    readonly_flag = getattr(os, "ST_RDONLY", 1)
    monkeypatch.setattr(
        env.volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_flag=readonly_flag if readonly else 0,
        ),
    )
    volume_root = (
        f"/{env.volume_path}"
        if same_source
        else "/subvol/benedict/decoy/the-mirage"
    )
    if env.source.is_file():
        volume_root = "/"
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "%s:%s" % (
                os.major(source_stat.st_dev),
                os.minor(source_stat.st_dev),
            ),
            "root": volume_root,
            "target": env.target,
            "fstype": (
                "fuse.glusterfs"
                if env.source.is_dir()
                else "xfs"
            ),
            "source": (
                "kadalu:bellagio-pool"
                if env.source.is_dir()
                else "/dev/loop17"
            ),
            "options": "ro,relatime" if readonly else "rw,relatime",
        }],
    )

    def execute(*command):
        if command[0] == "findmnt" and "SOURCE" in command:
            return "/dev/loop17", "", 101
        if command[0] == "findmnt" and "OPTIONS" in command:
            options = "ro,relatime" if readonly else "rw,relatime"
            return options, "", 101
        if command[0] == "losetup" and "BACK-INO,BACK-MAJ:MIN" in command:
            return f"{backing_inode} {backing_device}", "", 102
        if command[0] == "losetup" and "BACK-FILE" in command:
            return "/the-mirage/terry-benedict-decoy.img", "", 102
        raise AssertionError(f"unexpected command: {command!r}")

    monkeypatch.setattr(env.volumeutils, "execute", execute)


@pytest.mark.parametrize("pvtype", ["subvol", "virtblock", "rawblock"])
@pytest.mark.parametrize("same_source", [True, False])
def test_existing_target_requires_the_exact_requested_volume(
        monkeypatch, tmp_path, pvtype, same_source):
    env = _prepare_publish(monkeypatch, tmp_path, pvtype)
    _set_existing_target(
        monkeypatch,
        env,
        same_source=same_source,
    )
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(env),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    if same_source:
        assert context.code is None
    else:
        assert context.code == grpc.StatusCode.ALREADY_EXISTS


@pytest.mark.parametrize("pvtype", ["virtblock", "rawblock"])
@pytest.mark.parametrize("error_number", [errno.EIO, errno.ENOTCONN])
def test_transient_block_backing_error_is_unavailable(
        monkeypatch, tmp_path, pvtype, error_number):
    env = _prepare_publish(monkeypatch, tmp_path, pvtype)
    _set_existing_target(
        monkeypatch,
        env,
        same_source=False,
        samefile_error=OSError(error_number, "the Bellagio vault disconnected"),
    )
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(env),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.UNAVAILABLE


def test_stale_subvolume_recovery_uses_mountinfo_root_without_path_probe(
        monkeypatch, tmp_path):
    env = _prepare_publish(monkeypatch, tmp_path, "subvol")
    env.target.mkdir()
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "0:97",
            "root": f"/{env.volume_path}",
            "target": env.target,
            "fstype": "fuse.glusterfs",
            "source": (
                "kadalu:bellagio-pool:"
                f"{MOUNT_GENERATION}-{'a' * 64}"
            ),
        }],
    )
    monkeypatch.setattr(
        env.volumeutils.os.path,
        "samefile",
        _fail_if_called,
    )
    monkeypatch.setattr(
        env.volumeutils,
        "_mounted_volume_source_matches",
        lambda *_args, **_kwargs: False,
    )
    commands = []

    def execute(*command):
        commands.append(command)
        return "", "", 102

    monkeypatch.setattr(env.volumeutils, "execute", execute)

    assert env.volumeutils.mount_volume(
        str(env.source),
        str(env.target),
        env.volumeutils.PV_TYPE_SUBVOL,
        logical_source_path=str(env.source),
        volume_path=env.volume_path,
        host_volume_name=POOL_NAME,
        host_mount_identity=f"{MOUNT_GENERATION}-{'a' * 64}",
        volume_id=VOLUME_ID,
    )
    assert (env.volumeutils.UNMOUNT_CMD, "-l", str(env.target)) in commands
    assert (env.volumeutils.MOUNT_CMD, "--bind", str(env.source), str(env.target)) \
        in commands


def test_migrated_subvolume_recovers_exact_identityless_legacy_mount(
        monkeypatch, tmp_path):
    env = _prepare_publish(monkeypatch, tmp_path, "subvol")
    env.target.mkdir()
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "0:97",
            "root": f"/{env.volume_path}",
            "target": env.target,
            "fstype": "fuse.glusterfs",
            "source": "kadalu:bellagio-pool",
        }],
    )
    monkeypatch.setattr(
        env.volumeutils,
        "_mounted_volume_source_matches",
        lambda *_args, **_kwargs: False,
    )
    commands = []
    monkeypatch.setattr(
        env.volumeutils,
        "execute",
        lambda *command: commands.append(command) or ("", "", 102),
    )

    assert env.volumeutils.mount_volume(
        str(env.source),
        str(env.target),
        env.volumeutils.PV_TYPE_SUBVOL,
        logical_source_path=str(env.source),
        volume_path=env.volume_path,
        host_volume_name=POOL_NAME,
        host_mount_identity=f"{MOUNT_GENERATION}-{'a' * 64}",
        volume_id=VOLUME_ID,
        allow_legacy_host_mount=True,
    )

    assert (env.volumeutils.UNMOUNT_CMD, "-l", str(env.target)) in commands


@pytest.mark.parametrize(
    ("source", "allow_legacy"),
    [
        ("kadalu:bellagio-pool", False),
        (
            "kadalu:bellagio-pool:"
            "87654321-4321-6789-a234-567812345678-" + "b" * 64,
            True,
        ),
    ],
    ids=["no-migration-authorization", "identity-token-present"],
)
def test_legacy_subvolume_fallback_fails_closed_without_exact_authority(
        monkeypatch, tmp_path, source, allow_legacy):
    env = _prepare_publish(monkeypatch, tmp_path, "subvol")
    env.target.mkdir()
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "0:97",
            "root": f"/{env.volume_path}",
            "target": env.target,
            "fstype": "fuse.glusterfs",
            "source": source,
        }],
    )
    monkeypatch.setattr(
        env.volumeutils,
        "_mounted_volume_source_matches",
        lambda *_args, **_kwargs: False,
    )

    with pytest.raises(env.volumeutils.MountTargetConflictError):
        env.volumeutils.mount_volume(
            str(env.source),
            str(env.target),
            env.volumeutils.PV_TYPE_SUBVOL,
            logical_source_path=str(env.source),
            volume_path=env.volume_path,
            host_volume_name=POOL_NAME,
            host_mount_identity=f"{MOUNT_GENERATION}-{'a' * 64}",
            volume_id=VOLUME_ID,
            allow_legacy_host_mount=allow_legacy,
        )


def test_stale_subvolume_recovery_rejects_other_mount_generation(
        monkeypatch, tmp_path):
    env = _prepare_publish(monkeypatch, tmp_path, "subvol")
    env.target.mkdir()
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "0:97",
            "root": f"/{env.volume_path}",
            "target": env.target,
            "fstype": "fuse.glusterfs",
            "source": (
                "kadalu:bellagio-pool:"
                "87654321-4321-6789-a234-567812345678-"
                f"{'b' * 64}"
            ),
        }],
    )
    monkeypatch.setattr(
        env.volumeutils,
        "_mounted_volume_source_matches",
        lambda *_args, **_kwargs: False,
    )

    with pytest.raises(env.volumeutils.MountTargetConflictError):
        env.volumeutils.mount_volume(
            str(env.source),
            str(env.target),
            env.volumeutils.PV_TYPE_SUBVOL,
            logical_source_path=str(env.source),
            volume_path=env.volume_path,
            host_volume_name=POOL_NAME,
            host_mount_identity=f"{MOUNT_GENERATION}-{'a' * 64}",
            volume_id=VOLUME_ID,
        )


def test_existing_mount_must_match_requested_readonly_mode(
        monkeypatch, tmp_path):
    cases = (
        (False, False, None),
        (True, True, None),
        (False, True, grpc.StatusCode.ALREADY_EXISTS),
        (True, False, grpc.StatusCode.ALREADY_EXISTS),
    )

    for index, (existing_readonly, requested_readonly, expected) in enumerate(cases):
        case_root = tmp_path / f"the-sting-{index}"
        env = _prepare_publish(
            monkeypatch,
            case_root,
            "subvol",
        )
        _set_existing_target(
            monkeypatch,
            env,
            same_source=True,
            readonly=existing_readonly,
        )
        context = FakeContext()

        env.nodeserver.NodeServer().NodePublishVolume(
            _publish_request(env, readonly=requested_readonly),
            context,
        )

        assert context.code == expected


def test_missing_pinned_source_is_not_found(monkeypatch, tmp_path):
    env = _prepare_publish(
        monkeypatch,
        tmp_path,
        "subvol",
        source_exists=False,
    )
    monkeypatch.setattr(env.nodeserver, "mount_volume", _fail_if_called)
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(env),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.NOT_FOUND


def test_node_publish_forwards_readonly_filesystem_and_mount_flags(
        monkeypatch, tmp_path):
    env = _prepare_publish(monkeypatch, tmp_path, "virtblock")
    calls = []

    def mount_volume(source, target, pvtype, **kwargs):
        calls.append((source, target, pvtype, kwargs))
        return True

    monkeypatch.setattr(env.nodeserver, "mount_volume", mount_volume)
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(
            env,
            readonly=True,
            mount_flags=("nodev", "noexec"),
        ),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    assert context.code is None
    assert len(calls) == 1
    _, target, pvtype, kwargs = calls[0]
    assert target == str(env.target)
    assert pvtype == env.volumeutils.PV_TYPE_VIRTBLOCK
    assert kwargs == {
        "fstype": "xfs",
        "host_mount_identity": None,
        "host_volume_name": POOL_NAME,
        "allow_legacy_host_mount": False,
        "logical_source_path": str(env.source),
        "readonly": True,
        "mount_flags": ["nodev", "noexec"],
        "volume_path": env.volume_path,
        "volume_id": VOLUME_ID,
    }


@pytest.mark.parametrize(
    "mount_flag",
    [
        "ro",
        "rw",
        "bind",
        "rbind",
        "remount",
        "move",
        "private",
        "rprivate",
        "shared",
        "rshared",
        "slave",
        "rslave",
        "unbindable",
        "runbindable",
    ],
)
def test_node_publish_rejects_driver_controlled_mount_flags(
        monkeypatch, tmp_path, mount_flag):
    env = _prepare_publish(monkeypatch, tmp_path, "subvol")
    monkeypatch.setattr(env.volumeutils, "execute", _fail_if_called)
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(env, readonly=True, mount_flags=(mount_flag,)),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert not env.target.exists()


@pytest.mark.parametrize("pvtype", ["subvol", "virtblock", "rawblock"])
def test_readonly_publish_uses_readonly_mount_and_safe_flags(
        monkeypatch, tmp_path, pvtype):
    volumeutils, _nodeserver, _csi_pb2 = _load_csi_stack(monkeypatch)
    source = tmp_path / "the-italian-job-source"
    target = tmp_path / "bellagio-target"
    if pvtype == volumeutils.PV_TYPE_SUBVOL:
        source.mkdir()
    else:
        source.write_bytes(b"A fake Mini Cooper payload")
    commands = []

    def execute(*command):
        commands.append(command)
        if command[:3] == ("losetup", "-f", "--show"):
            return "/dev/loop31", "", 101
        return "", "", 102

    monkeypatch.setattr(volumeutils, "execute", execute)

    assert volumeutils.mount_volume(
        str(source),
        str(target),
        pvtype,
        fstype="xfs",
        readonly=True,
        mount_flags=["nodev", "noexec"],
    )

    mount_commands = [
        command for command in commands
        if command and command[0] == volumeutils.MOUNT_CMD
    ]
    option_text = ",".join(
        argument
        for command in mount_commands
        for argument in command
        if isinstance(argument, str)
    )
    assert "ro" in option_text.split(",")
    assert "nodev" in option_text.split(",")
    assert "noexec" in option_text.split(",")

    if pvtype in (
            volumeutils.PV_TYPE_SUBVOL,
            volumeutils.PV_TYPE_RAWBLOCK):
        assert any("--bind" in command for command in mount_commands)
        assert any("remount" in ",".join(command)
                   for command in mount_commands)
    else:
        assert len(mount_commands) == 1
        assert "-t" in mount_commands[0]

    if pvtype == volumeutils.PV_TYPE_RAWBLOCK:
        assert (
            "losetup",
            "-f",
            "--show",
            "--read-only",
            str(source),
        ) in commands


@pytest.mark.parametrize("pvtype", ["virtblock", "rawblock"])
def test_loop_publish_retry_uses_stable_backing_identity_after_fd_closes(
        monkeypatch, tmp_path, pvtype):
    env = _prepare_publish(monkeypatch, tmp_path, pvtype)
    if pvtype == env.volumeutils.PV_TYPE_RAWBLOCK:
        env.target.touch()
    else:
        env.target.mkdir()

    stale_descriptor = os.open(env.source, os.O_RDONLY)
    stale_source = f"/proc/{os.getpid()}/fd/{stale_descriptor}"
    os.close(stale_descriptor)
    assert not os.path.exists(stale_source)

    source_stat = env.source.stat()
    backing_identity = "%s %s:%s" % (
        source_stat.st_ino,
        os.major(source_stat.st_dev),
        os.minor(source_stat.st_dev),
    )
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": backing_identity.split()[1],
            "root": "/",
            "target": env.target,
            "fstype": "xfs",
            "source": "/dev/loop53",
        }],
    )
    commands = []

    def execute(*command):
        commands.append(command)
        if command[0] == "losetup" and "BACK-INO,BACK-MAJ:MIN" in command:
            return backing_identity, "", 102
        if command[0] == "losetup" and "BACK-FILE" in command:
            return stale_source, "", 102
        raise AssertionError(f"unexpected command: {command!r}")

    monkeypatch.setattr(env.volumeutils, "execute", execute)
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(env),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    assert context.code is None
    assert not any("BACK-FILE" in command for command in commands)


@pytest.mark.parametrize("pvtype", ["virtblock", "rawblock"])
def test_stale_block_target_recovers_from_node_owned_publish_identity(
        monkeypatch, tmp_path, pvtype):
    env = _prepare_publish(monkeypatch, tmp_path, pvtype)
    if pvtype == env.volumeutils.PV_TYPE_RAWBLOCK:
        env.target.touch()
        mount_source = "devtmpfs"
        mount_root = "/loop53"
        mount_fstype = "devtmpfs"
    else:
        env.target.mkdir()
        mount_source = "/dev/loop53"
        mount_root = "/"
        mount_fstype = "xfs"
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "7:53",
            "root": mount_root,
            "target": env.target,
            "fstype": mount_fstype,
            "source": mount_source,
        }],
    )
    mount_identity = f"{MOUNT_GENERATION}-{'a' * 64}"
    expected_state = env.volumeutils._publish_state(
        VOLUME_ID,
        pvtype,
        env.volume_path,
        POOL_NAME,
        mount_identity,
    )
    Path(env.volumeutils._publish_state_path(str(env.target))).write_text(
        json.dumps(expected_state),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        env.volumeutils,
        "_mounted_volume_source_matches",
        lambda *_args, **_kwargs: False,
    )
    commands = []

    def execute(*command):
        commands.append(command)
        if command[:3] == ("losetup", "-f", "--show"):
            return "/dev/loop61", "", 101
        if command[0] == "losetup" and "BACK-FILE" in command:
            return "/proc/999999/fd/17", "", 102
        return "", "", 102

    monkeypatch.setattr(env.volumeutils, "execute", execute)

    assert env.volumeutils.mount_volume(
        str(env.source),
        str(env.target),
        pvtype,
        fstype="xfs",
        logical_source_path=str(env.source),
        volume_path=env.volume_path,
        host_volume_name=POOL_NAME,
        host_mount_identity=mount_identity,
        volume_id=VOLUME_ID,
    )

    assert ("losetup", "-d", "/dev/loop53") in commands
    assert (env.volumeutils.UNMOUNT_CMD, "-l", str(env.target)) in commands
    assert not any("BACK-FILE" in command for command in commands)
    assert json.loads(Path(
        env.volumeutils._publish_state_path(str(env.target))
    ).read_text(encoding="utf-8")) == expected_state


def test_stale_block_target_without_publish_identity_fails_closed(
        monkeypatch, tmp_path):
    env = _prepare_publish(monkeypatch, tmp_path, "virtblock")
    env.target.mkdir()
    _write_mountinfo(
        monkeypatch,
        env.volumeutils,
        env.target.parent / "mountinfo",
        [{
            "device": "7:53",
            "root": "/",
            "target": env.target,
            "fstype": "xfs",
            "source": "/dev/loop53",
        }],
    )
    monkeypatch.setattr(
        env.volumeutils,
        "_mounted_volume_source_matches",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(env.volumeutils, "execute", _fail_if_called)

    with pytest.raises(env.volumeutils.MountTargetConflictError):
        env.volumeutils.mount_volume(
            str(env.source),
            str(env.target),
            env.volumeutils.PV_TYPE_VIRTBLOCK,
            logical_source_path=str(env.source),
            volume_path=env.volume_path,
            host_volume_name=POOL_NAME,
            host_mount_identity=f"{MOUNT_GENERATION}-{'a' * 64}",
            volume_id=VOLUME_ID,
        )


def test_rawblock_loop_is_detached_when_bind_mount_fails(
        monkeypatch, tmp_path):
    volumeutils, _nodeserver, _csi_pb2 = _load_csi_stack(monkeypatch)
    source = tmp_path / "the-score-vault-image"
    source.write_bytes(b"A fake Montreal Customs House vault")
    target = tmp_path / "the-score-device"
    commands = []

    def execute(*command):
        commands.append(command)
        if command[:4] == ("losetup", "-f", "--show", str(source)):
            return "/dev/loop23", "", 101
        if command[0] == volumeutils.MOUNT_CMD:
            raise volumeutils.CommandException(
                32,
                "",
                "the heist crew could not bind the loop device",
            )
        return "", "", 102

    monkeypatch.setattr(volumeutils, "execute", execute)

    with pytest.raises(volumeutils.CommandException):
        volumeutils.mount_volume(
            str(source),
            str(target),
            volumeutils.PV_TYPE_RAWBLOCK,
        )

    assert ("losetup", "-d", "/dev/loop23") in commands


def test_nomad_shaped_rawblock_unpublish_detaches_loop_and_removes_target(
        monkeypatch, tmp_path):
    volumeutils, nodeserver, csi_pb2 = _load_csi_stack(monkeypatch)
    csi_root = tmp_path / "csi"
    target = csi_root / "allocations" / "oceans-eleven" / "bellagio-device"
    target.parent.mkdir(parents=True)
    target.touch()
    monkeypatch.setattr(nodeserver, "CSI_MOUNT_DIR", str(csi_root))
    _write_mountinfo(
        monkeypatch,
        volumeutils,
        target.parent / "mountinfo",
        [{
            "device": "7:41",
            "root": "/loop41",
            "target": target,
            "fstype": "devtmpfs",
            "source": "devtmpfs",
        }],
    )
    commands = []

    def execute(*command):
        commands.append(command)
        return "", "", 102

    monkeypatch.setattr(volumeutils, "execute", execute)
    context = FakeContext()

    response = nodeserver.NodeServer().NodeUnpublishVolume(
        csi_pb2.NodeUnpublishVolumeRequest(
            volume_id=VOLUME_ID,
            target_path=str(target),
        ),
        context,
    )

    assert isinstance(response, csi_pb2.NodeUnpublishVolumeResponse)
    assert context.code is None
    assert commands == [
        ("losetup", "-d", "/dev/loop41"),
        (volumeutils.UNMOUNT_CMD, "-l", str(target)),
    ]
    assert not target.exists()


def test_unpublish_tolerates_loop_already_auto_cleared(monkeypatch, tmp_path):
    volumeutils, _nodeserver, _csi_pb2 = _load_csi_stack(monkeypatch)
    target = tmp_path / "the-italian-job-device"
    target.touch()
    _write_mountinfo(
        monkeypatch,
        volumeutils,
        tmp_path / "mountinfo",
        [{
            "device": "7:37",
            "root": "/loop37",
            "target": target,
            "fstype": "devtmpfs",
            "source": "devtmpfs",
        }],
    )
    monkeypatch.setattr(
        volumeutils,
        "_loop_device_is_configured",
        lambda _loop: False,
    )
    commands = []

    def execute(*command):
        commands.append(command)
        if command[0] == "losetup":
            raise volumeutils.CommandException(1, "", "no such device")
        return "", "", 102

    monkeypatch.setattr(volumeutils, "execute", execute)

    volumeutils.unmount_volume(str(target))

    assert commands == [
        ("losetup", "-d", "/dev/loop37"),
        (volumeutils.UNMOUNT_CMD, "-l", str(target)),
    ]
    assert not target.exists()


def test_failed_loop_detach_preserves_target_for_retry(monkeypatch, tmp_path):
    volumeutils, _nodeserver, _csi_pb2 = _load_csi_stack(monkeypatch)
    target = tmp_path / "the-score-device"
    target.touch()
    _write_mountinfo(
        monkeypatch,
        volumeutils,
        tmp_path / "mountinfo",
        [{
            "device": "7:19",
            "root": "/loop19",
            "target": target,
            "fstype": "devtmpfs",
            "source": "devtmpfs",
        }],
    )
    monkeypatch.setattr(
        volumeutils,
        "_loop_device_is_configured",
        lambda _loop: True,
    )
    commands = []
    detach_attempts = 0

    def execute(*command):
        nonlocal detach_attempts
        commands.append(command)
        if command[0] == "losetup":
            detach_attempts += 1
            if detach_attempts == 1:
                raise volumeutils.CommandException(
                    1,
                    "",
                    "Rusty could not release the fake loop device",
                )
        return "", "", 102

    monkeypatch.setattr(volumeutils, "execute", execute)

    with pytest.raises(volumeutils.CommandException):
        volumeutils.unmount_volume(str(target))
    assert target.exists()
    assert (volumeutils.UNMOUNT_CMD, "-l", str(target)) not in commands

    volumeutils.unmount_volume(str(target))

    assert detach_attempts == 2
    assert commands[-2:] == [
        ("losetup", "-d", "/dev/loop19"),
        (volumeutils.UNMOUNT_CMD, "-l", str(target)),
    ]
    assert not target.exists()


def test_node_target_operations_serialize_only_the_exact_target(monkeypatch):
    _volumeutils, nodeserver, _csi_pb2 = _load_csi_stack(monkeypatch)
    monkeypatch.setattr(nodeserver, "TARGET_LOCKS", {})
    first_entered = threading.Event()
    release_first = threading.Event()
    other_finished = threading.Event()
    entered = []

    @nodeserver.serialized_target_operation
    def operation(_server, request, _context):
        entered.append(request.label)
        if request.label == "danny-ocean":
            first_entered.set()
            assert release_first.wait(timeout=2)
        if request.label == "rusty-ryan":
            other_finished.set()

    target = "/var/lib/kubelet/pods/oceans-eleven/volumes/bellagio/mount"
    first = SimpleNamespace(target_path=target, label="danny-ocean")
    same = SimpleNamespace(target_path=target, label="linus-caldwell")
    other = SimpleNamespace(
        target_path=(
            "/var/lib/kubelet/pods/the-italian-job/volumes/turin/mount"
        ),
        label="rusty-ryan",
    )
    first_thread = threading.Thread(
        target=operation,
        args=(object(), first, object()),
    )
    same_thread = threading.Thread(
        target=operation,
        args=(object(), same, object()),
    )
    other_thread = threading.Thread(
        target=operation,
        args=(object(), other, object()),
    )

    first_thread.start()
    assert first_entered.wait(timeout=2)
    same_thread.start()
    other_thread.start()
    assert other_finished.wait(timeout=2)
    assert "linus-caldwell" not in entered

    release_first.set()
    first_thread.join(timeout=2)
    same_thread.join(timeout=2)
    other_thread.join(timeout=2)

    assert not first_thread.is_alive()
    assert not same_thread.is_alive()
    assert not other_thread.is_alive()
    assert entered.index("linus-caldwell") > entered.index("danny-ocean")
    assert nodeserver.TARGET_LOCKS == {}


def test_node_publish_reports_conflicting_hosting_mount(monkeypatch, tmp_path):
    env = _prepare_publish(
        monkeypatch,
        tmp_path,
        "subvol",
    )
    monkeypatch.setattr(
        env.nodeserver,
        "mount_glusterfs",
        lambda *_args: (_ for _ in ()).throw(
            env.volumeutils.MountTargetConflictError(
                "Benedict's fake vault already occupies the target"
            )
        ),
    )
    monkeypatch.setattr(env.nodeserver, "mount_volume", _fail_if_called)
    context = FakeContext()

    response = env.nodeserver.NodeServer().NodePublishVolume(
        _publish_request(env),
        context,
    )

    assert isinstance(response, env.csi_pb2.NodePublishVolumeResponse)
    assert context.code == grpc.StatusCode.ALREADY_EXISTS


def test_node_unpublish_removes_unmounted_directory_target(
        monkeypatch, tmp_path):
    volumeutils, nodeserver, csi_pb2 = _load_csi_stack(monkeypatch)
    csi_root = tmp_path / "csi"
    target = csi_root / "allocations" / "the-sting" / "mount"
    target.mkdir(parents=True)
    monkeypatch.setattr(nodeserver, "CSI_MOUNT_DIR", str(csi_root))
    monkeypatch.setattr(
        volumeutils.os.path,
        "ismount",
        lambda _path: False,
    )
    monkeypatch.setattr(volumeutils, "execute", _fail_if_called)
    context = FakeContext()

    response = nodeserver.NodeServer().NodeUnpublishVolume(
        csi_pb2.NodeUnpublishVolumeRequest(
            volume_id=VOLUME_ID,
            target_path=str(target),
        ),
        context,
    )

    assert isinstance(response, csi_pb2.NodeUnpublishVolumeResponse)
    assert context.code is None
    assert not target.exists()
