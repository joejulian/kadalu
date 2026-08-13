"""Regression tests for CSI process and Gluster mount lifecycle handling."""

import importlib
import json
import signal
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]


class TrackingLock:
    """Record whether a mount-process check occurs while locked."""

    def __init__(self):
        self.held = False

    def __enter__(self):
        assert not self.held
        self.held = True
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.held = False


def _load_csi_module(monkeypatch, name):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "csi"))
    sys.modules.pop(name, None)
    return importlib.import_module(name)


def _native_volume(tmp_path, volumeutils):
    volname = "bellagio-pool"
    (tmp_path / f"{volname}.info").write_text(
        json.dumps({"bricks": [{"node": "server-bellagio-vault-0"}]}),
        encoding="utf-8",
    )
    volumeutils.VOLINFO_DIR = str(tmp_path)
    return {"name": volname, "type": "Replica1"}


def _external_volume():
    return {
        "name": "bellagio-pool",
        "type": "External",
        "g_volname": "bellagio-vault",
        "g_host": "server-bellagio-vault-0",
        "g_options": "log-level=WARNING",
    }


def test_main_registers_sighup_handler_before_mounting_storage(monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    events = []

    class FakeServer:
        """Minimal gRPC server used to stop the main loop immediately."""

        def add_insecure_port(self, _endpoint):
            return None

        def start(self):
            events.append("server-start")

        def stop(self, _grace):
            events.append("server-stop")

    fake_server = FakeServer()
    monkeypatch.setattr(main, "logging_setup", lambda: events.append("logging"))
    monkeypatch.setattr(
        main.signal,
        "signal",
        lambda signum, handler: events.append((signum, handler)),
    )
    monkeypatch.setattr(main, "mount_storage", lambda: events.append("mount"))
    monkeypatch.setattr(main.grpc, "server", lambda _executor: fake_server)
    monkeypatch.setattr(
        main.futures, "ThreadPoolExecutor", lambda **_kwargs: object())
    monkeypatch.setattr(
        main.csi_pb2_grpc,
        "add_ControllerServicer_to_server",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        main.csi_pb2_grpc,
        "add_NodeServicer_to_server",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        main.csi_pb2_grpc,
        "add_IdentityServicer_to_server",
        lambda *_args: None,
    )
    monkeypatch.setattr(main, "ControllerServer", object)
    monkeypatch.setattr(main, "NodeServer", object)
    monkeypatch.setattr(main, "IdentityServer", object)
    monkeypatch.setattr(
        main.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt),
    )

    main.main()

    signal_event = events[1]
    assert signal_event == (signal.SIGHUP, main._handle_sighup)
    assert events.index(signal_event) < events.index("mount")
    signal_event[1](signal.SIGHUP, None)


def test_obsolete_configmap_watcher_is_not_packaged_or_started():
    start = (ROOT / "csi" / "start.py").read_text(encoding="utf-8")
    dockerfile = (ROOT / "csi" / "Dockerfile").read_text(encoding="utf-8")

    assert "volumewatch" not in start
    assert "watch-vol-changes" not in dockerfile
    assert "inotify-tools" not in dockerfile
    assert not (ROOT / "csi" / "watch-vol-changes.sh").exists()


def test_native_mount_rechecks_established_mount_while_holding_lock(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _native_volume(tmp_path, volumeutils)
    lock = TrackingLock()
    checks = []

    def mount_established(_volname, _mountpoint):
        checks.append(lock.held)
        return len(checks) == 2

    monkeypatch.setattr(volumeutils, "mount_lock", lock)
    monkeypatch.setattr(volumeutils, "is_server_pod_reachable", lambda *_args: True)
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        mount_established,
    )
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *_args: pytest.fail("duplicate mount launched"),
    )

    mountpoint = str(tmp_path / "mount")
    assert volumeutils.mount_glusterfs(volume, mountpoint) == mountpoint
    assert checks == [False, True]


def test_existing_native_mount_does_not_require_reachable_server(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _native_volume(tmp_path, volumeutils)
    mountpoint = str(tmp_path / "mount")

    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda volname, path: (
            volname == volume["name"] and path == mountpoint
        ),
    )
    monkeypatch.setattr(
        volumeutils,
        "is_server_pod_reachable",
        lambda *_args: pytest.fail("reachability checked for existing mount"),
    )
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *_args: pytest.fail("existing mount launched again"),
    )

    assert volumeutils.mount_glusterfs(volume, mountpoint) == mountpoint


def test_external_mount_rechecks_established_mount_while_holding_lock(
        monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _external_volume()
    lock = TrackingLock()
    checks = []

    def mount_established(_volname, _mountpoint):
        checks.append(lock.held)
        return len(checks) == 2

    monkeypatch.setattr(volumeutils, "mount_lock", lock)
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        mount_established,
    )
    monkeypatch.setattr(
        volumeutils,
        "mount_glusterfs_with_host",
        lambda *_args, **_kwargs: pytest.fail("duplicate mount launched"),
    )
    monkeypatch.setattr(
        volumeutils.os.path,
        "isfile",
        lambda _path: pytest.fail("quota setup repeated for existing mount"),
    )

    mountpoint = "/mnt/bellagio"
    assert volumeutils.handle_external_volume(
        volume, mountpoint, False, volume["g_host"]
    ) == mountpoint
    assert checks == [False, True]


def test_native_mount_propagates_unreachable_servers(monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _native_volume(tmp_path, volumeutils)
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda *_args: False,
    )
    monkeypatch.setattr(volumeutils, "is_server_pod_reachable", lambda *_args: False)
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *_args: pytest.fail("mount launched without a server"),
    )

    with pytest.raises(volumeutils.CommandException) as error:
        volumeutils.mount_glusterfs(volume, str(tmp_path / "mount"))

    assert error.value.ret == -1


@pytest.mark.parametrize("mount_established", [True, False])
def test_native_mount_accepts_exit_32_only_for_established_mount(
        monkeypatch, tmp_path, mount_established):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _native_volume(tmp_path, volumeutils)
    checks = iter([False, False, mount_established])
    monkeypatch.setattr(volumeutils, "is_server_pod_reachable", lambda *_args: True)
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda _volname, _mountpoint: next(checks),
    )
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *_args: (_ for _ in ()).throw(
            volumeutils.CommandException(32, "", "mount already exists")
        ),
    )
    mountpoint = str(tmp_path / "mount")

    if mount_established:
        assert volumeutils.mount_glusterfs(volume, mountpoint) == mountpoint
    else:
        with pytest.raises(volumeutils.CommandException) as error:
            volumeutils.mount_glusterfs(volume, mountpoint)
        assert error.value.ret == 32


def test_external_mount_returns_after_retry_without_rejected_options(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    commands = []

    def execute(*command):
        commands.append(command)
        if len(commands) == 1:
            raise volumeutils.CommandException(1, "", "invalid option")
        return "", "", 101

    monkeypatch.setattr(volumeutils, "execute", execute)
    mountpoint = str(tmp_path / "mount")

    assert volumeutils.mount_glusterfs_with_host(
        "bellagio-vault",
        mountpoint,
        "server-bellagio-vault-0",
        "log-level=WARNING",
    ) == mountpoint
    assert "--log-level=WARNING" in commands[0]
    assert "--log-level=WARNING" not in commands[1]
    display_index = commands[1].index("--fs-display-name")
    assert commands[1][display_index + 1] == "kadalu:bellagio-vault"


@pytest.mark.parametrize("mount_established", [True, False])
def test_external_mount_accepts_exit_32_only_for_established_mount(
        monkeypatch, tmp_path, mount_established):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    mountpoint = str(tmp_path / "mount")
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *_args: (_ for _ in ()).throw(
            volumeutils.CommandException(32, "", "mount already exists")
        ),
    )
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda volname, path: (
            mount_established
            and volname == "bellagio-vault"
            and path == mountpoint
        ),
    )

    if mount_established:
        assert volumeutils.mount_glusterfs_with_host(
            "bellagio-vault",
            mountpoint,
            "server-bellagio-vault-0",
        ) == mountpoint
    else:
        with pytest.raises(volumeutils.CommandException) as error:
            volumeutils.mount_glusterfs_with_host(
                "bellagio-vault",
                mountpoint,
                "server-bellagio-vault-0",
            )
        assert error.value.ret == 32


@pytest.mark.parametrize("mount_established", [True, False])
def test_external_fallback_accepts_exit_32_only_for_established_mount(
        monkeypatch, tmp_path, mount_established):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    commands = []

    def execute(*command):
        commands.append(command)
        if len(commands) == 1:
            raise volumeutils.CommandException(1, "", "unrecognized option")
        raise volumeutils.CommandException(32, "", "mount already exists")

    monkeypatch.setattr(volumeutils, "execute", execute)
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda volname, mountpoint: (
            mount_established
            and volname == "bellagio-vault"
            and mountpoint == str(tmp_path / "mount")
        ),
    )
    mountpoint = str(tmp_path / "mount")

    if mount_established:
        assert volumeutils.mount_glusterfs_with_host(
            "bellagio-vault",
            mountpoint,
            "server-bellagio-vault-0",
            "log-level=WARNING",
        ) == mountpoint
    else:
        with pytest.raises(volumeutils.CommandException) as error:
            volumeutils.mount_glusterfs_with_host(
                "bellagio-vault",
                mountpoint,
                "server-bellagio-vault-0",
                "log-level=WARNING",
            )
        assert error.value.ret == 32


def test_process_without_mount_table_entry_is_not_established(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    mountpoint = str(tmp_path / "bellagio vault")
    mounts_file = tmp_path / "mounts"
    mounts_file.write_text("", encoding="utf-8")

    monkeypatch.setattr(volumeutils, "MOUNTS_FILE", str(mounts_file))
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_proc_running",
        lambda volname, target: (
            volname == "bellagio-vault" and target == mountpoint
        ),
    )

    assert not volumeutils.is_gluster_mount_established(
        "bellagio-vault",
        mountpoint,
    )


def test_established_mount_requires_exact_volume_and_normalized_target(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    mountpoint = tmp_path / "bellagio vault"
    escaped_target = str(mountpoint).replace(" ", r"\040")
    mounts_file = tmp_path / "mounts"
    mounts_file.write_text(
        f"kadalu:bellagio-vault {escaped_target} "
        "fuse.glusterfs rw,relatime 0 0\n"
        f"kadalu:bellagio-decoy {escaped_target} "
        "fuse.glusterfs rw,relatime 0 0\n"
        f"kadalu:bellagio-vault/roulette {escaped_target} "
        "fuse.glusterfs rw,relatime 0 0\n",
        encoding="utf-8",
    )

    process_targets = []

    def process_running(_volname, target):
        process_targets.append(target)
        return True

    monkeypatch.setattr(volumeutils, "MOUNTS_FILE", str(mounts_file))
    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_proc_running",
        process_running,
    )

    assert volumeutils.is_gluster_mount_established(
        "bellagio-vault",
        f"{mountpoint.parent}/./{mountpoint.name}",
    )
    assert process_targets[0] == str(mountpoint)
    assert not volumeutils.is_gluster_mount_established(
        "bellagio-casino",
        str(mountpoint),
    )
    assert not volumeutils.is_gluster_mount_established(
        "bellagio-vault",
        str(tmp_path / "three-casinos"),
    )
