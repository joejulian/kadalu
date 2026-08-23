"""Regression tests for CSI process and Gluster mount lifecycle handling."""

import errno
import importlib
import json
import signal
import sys
from pathlib import Path
from types import SimpleNamespace

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


def test_mount_identity_rejects_tampered_backend_fingerprint(
        monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = {
        **_external_volume(),
        "volname": "bellagio-pool",
        "volume_id": "pvc-danny-ocean",
        "kadalu_format": "native",
        "gluster_hosts": "server-bellagio-vault-0",
        "gluster_volname": "bellagio-vault",
        "gluster_options": "log-level=WARNING",
        "mount_identity": "12345678-1234-5678-9234-567812345678",
        "mount_config_fingerprint": "0" * 64,
    }

    with pytest.raises(ValueError, match="fingerprint does not match"):
        volumeutils.mount_identity_token(volume)


def test_mount_identity_binds_generation_to_backend_fingerprint(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = {
        **_external_volume(),
        "volname": "bellagio-pool",
        "volume_id": "pvc-danny-ocean",
        "kadalu_format": "native",
        "gluster_hosts": "server-bellagio-vault-0",
        "gluster_volname": "bellagio-vault",
        "gluster_options": "log-level=WARNING",
        "mount_identity": "12345678-1234-5678-9234-567812345678",
    }
    fingerprint = volumeutils.mount_config_fingerprint(volume)
    volume["mount_config_fingerprint"] = fingerprint

    assert volumeutils.mount_identity_token(volume) == (
        f"{volume['mount_identity']}-{fingerprint}"
    )


def test_legacy_mount_fallback_requires_unchanged_migration_fingerprint(
        monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = {
        **_external_volume(),
        "volname": "bellagio-pool",
        "volume_id": "pvc-danny-ocean",
        "gluster_hosts": "server-bellagio-vault-0",
        "gluster_volname": "bellagio-vault",
        "gluster_options": "log-level=WARNING",
        "mount_identity": "12345678-1234-5678-9234-567812345678",
    }
    volume["legacy_mount_config_fingerprint"] = (
        volumeutils.mount_config_fingerprint(volume)
    )

    assert volumeutils.legacy_mount_fallback_authorized(volume)

    volume["gluster_hosts"] = "server-the-mirage-decoy-0"
    assert not volumeutils.legacy_mount_fallback_authorized(volume)


def test_fresh_pool_has_no_legacy_mount_fallback(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")

    assert not volumeutils.legacy_mount_fallback_authorized({
        **_external_volume(),
        "volname": "bellagio-pool",
        "volume_id": "pvc-danny-ocean",
        "gluster_hosts": "server-bellagio-vault-0",
        "gluster_volname": "bellagio-vault",
    })


def test_main_starts_server_before_reconciliation_and_restarts_dead_thread(
        monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    events = []
    mount_results = iter([False, True])
    reconciler_starts = 0
    sleep_calls = 0
    monkeypatch.setenv("CSI_ROLE", "provisioner")

    def mount_storage():
        result = next(mount_results)
        events.append(("mount", result))
        return result

    def _reconcile_twice():
        events.append("reconcile")
        mounts_ready = main.reconcile_storage_mounts_once(None)
        main.time.sleep(main.STORAGE_MOUNT_RETRY_SECONDS)
        main.reconcile_storage_mounts_once(mounts_ready)

    class FakeReconciler:
        """Expose deterministic terminal and live daemon states."""

        def __init__(self, alive):
            self.alive = alive

        def is_alive(self):
            return self.alive

    def start_reconciler():
        nonlocal reconciler_starts
        reconciler_starts += 1
        if reconciler_starts == 1:
            _reconcile_twice()
            return FakeReconciler(False)
        events.append("reconciler-restart")
        return FakeReconciler(True)

    def sleep(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        events.append(("sleep", seconds))
        if sleep_calls == 3:
            raise KeyboardInterrupt

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
    monkeypatch.setattr(
        main,
        "start_storage_mount_reconciler",
        start_reconciler,
    )
    monkeypatch.setattr(main, "mount_storage", mount_storage)
    monkeypatch.setattr(
        main,
        "mark_storage_ready",
        lambda: events.append("storage-ready"),
    )
    monkeypatch.setattr(main, "storage_ready_marker_exists", lambda: False)
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
        main,
        "clear_storage_ready",
        lambda: events.append("clear-ready"),
    )
    monkeypatch.setattr(
        main.time,
        "sleep",
        sleep,
    )

    main.main()

    signal_event = events[1]
    assert signal_event == (signal.SIGHUP, main._handle_sighup)
    assert events.index(signal_event) < events.index("clear-ready")
    assert events.index("clear-ready") < events.index("server-start")
    assert events.index("server-start") < events.index("reconcile")
    assert events.index("reconcile") < events.index(("mount", False))
    assert events.index(("mount", False)) < events.index(("mount", True))
    assert events.index(("mount", True)) < events.index("storage-ready")
    assert events.count("clear-ready") == 2
    assert events.index("reconciler-restart") > max(
        index for index, event in enumerate(events) if event == "clear-ready"
    )
    signal_event[1](signal.SIGHUP, None)


@pytest.mark.parametrize(
    "error_type",
    ["command", "conflict", "oserror", "value"],
)
def test_startup_mount_failure_does_not_exit_entire_csi_server(
        monkeypatch, error_type):
    main = _load_csi_module(monkeypatch, "main")
    monkeypatch.setenv("CSI_ROLE", "provisioner")
    volumes = [
        {
            "name": "benedict-decoy",
            "single_pv_per_pool": False,
        },
        {
            "name": "bellagio-vault",
            "single_pv_per_pool": False,
        },
    ]
    monkeypatch.setattr(
        main,
        "get_pv_hosting_volumes",
        lambda _filters, iteration=40: volumes,
    )
    mounted = []
    errors = {
        "command": main.CommandException(
            -1,
            "connect to the casino vault",
            "the vault crew is temporarily unreachable",
        ),
        "conflict": main.MountTargetConflictError(
            "Benedict occupied the fake vault mount"
        ),
        "oserror": OSError(errno.ESTALE, "the decoy mount is stale"),
        "value": ValueError("the fake mount identity is invalid"),
    }

    def mount(volume, _mountpoint):
        if volume["name"] == "benedict-decoy":
            raise errors[error_type]
        mounted.append(volume["name"])

    monkeypatch.setattr(main, "mount_glusterfs", mount)

    all_mounted = main.mount_storage()

    assert mounted == ["bellagio-vault"]
    assert all_mounted is False


def test_first_pool_failure_clears_readiness_before_later_pool_attempt(
        monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    monkeypatch.setenv("CSI_ROLE", "provisioner")
    volumes = [
        {
            "name": "benedict-decoy",
            "single_pv_per_pool": False,
        },
        {
            "name": "bellagio-vault",
            "single_pv_per_pool": False,
        },
    ]
    marker = {"exists": True}
    events = []

    def mount(volume, _mountpoint):
        events.append(("mount", volume["name"]))
        if volume["name"] == "benedict-decoy":
            raise main.CommandException(
                -1,
                "connect to the decoy vault",
                "the decoy crew is unreachable",
            )

    def clear_ready():
        marker["exists"] = False
        events.append(("ready", False))

    monkeypatch.setattr(
        main,
        "get_pv_hosting_volumes",
        lambda _filters, iteration=40: volumes,
    )
    monkeypatch.setattr(main, "mount_glusterfs", mount)
    monkeypatch.setattr(main, "clear_storage_ready", clear_ready)
    monkeypatch.setattr(
        main,
        "storage_ready_marker_exists",
        lambda: marker["exists"],
    )

    assert main.mount_storage() is False
    assert events == [
        ("mount", "benedict-decoy"),
        ("ready", False),
        ("mount", "bellagio-vault"),
    ]


def test_readiness_clear_error_does_not_skip_later_healthy_pool(
        monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    monkeypatch.setenv("CSI_ROLE", "provisioner")
    volumes = [
        {"name": "benedict-decoy", "single_pv_per_pool": False},
        {"name": "bellagio-vault", "single_pv_per_pool": False},
    ]
    events = []

    def mount(volume, _mountpoint):
        events.append(("mount", volume["name"]))
        if volume["name"] == "benedict-decoy":
            raise OSError(errno.EHOSTUNREACH, "the decoy crew is offline")

    def clear_ready():
        events.append(("clear", "attempted"))
        raise OSError(errno.EIO, "the readiness marker is temporarily busy")

    monkeypatch.setattr(
        main,
        "get_pv_hosting_volumes",
        lambda _filters, iteration=40: volumes,
    )
    monkeypatch.setattr(main, "mount_glusterfs", mount)
    monkeypatch.setattr(main, "storage_ready_marker_exists", lambda: True)
    monkeypatch.setattr(main, "clear_storage_ready", clear_ready)

    assert main.mount_storage() is False
    assert events == [
        ("mount", "benedict-decoy"),
        ("clear", "attempted"),
        ("mount", "bellagio-vault"),
    ]


def test_mount_reconciliation_disables_nested_volume_discovery_retry(
        monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    discovery_calls = []
    monkeypatch.setenv("CSI_ROLE", "provisioner")

    def get_volumes(filters, iteration):
        discovery_calls.append((filters, iteration))
        return []

    monkeypatch.setattr(main, "get_pv_hosting_volumes", get_volumes)

    assert main.mount_storage() is True
    assert discovery_calls == [({}, 0)]


def test_mount_readiness_tracks_failure_and_recovery_without_flapping(
        monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    attempts = iter([True, False, True, True])
    events = []
    marker = {"exists": False}

    def mark_ready():
        marker["exists"] = True
        events.append("ready")

    def clear_ready():
        marker["exists"] = False
        events.append("unready")

    monkeypatch.setattr(
        main,
        "mount_storage",
        lambda: next(attempts),
    )
    monkeypatch.setattr(
        main,
        "mark_storage_ready",
        mark_ready,
    )
    monkeypatch.setattr(
        main,
        "clear_storage_ready",
        clear_ready,
    )
    monkeypatch.setattr(
        main,
        "storage_ready_marker_exists",
        lambda: marker["exists"],
    )

    mounts_ready = None
    states = []
    for _ in range(4):
        mounts_ready = main.reconcile_storage_mounts_once(mounts_ready)
        states.append(mounts_ready)

    assert states == [True, False, True, True]
    assert events == ["ready", "unready", "ready"]


def test_healthy_mounts_restore_missing_marker_without_steady_rewrite(
        monkeypatch, caplog):
    main = _load_csi_module(monkeypatch, "main")
    marker = {"exists": False}
    mark_calls = []

    def mark_ready():
        marker["exists"] = True
        mark_calls.append("mark")

    monkeypatch.setattr(main, "mount_storage", lambda: True)
    monkeypatch.setattr(main, "mark_storage_ready", mark_ready)
    monkeypatch.setattr(
        main,
        "storage_ready_marker_exists",
        lambda: marker["exists"],
    )

    assert main.reconcile_storage_mounts_once(True) is True
    assert main.reconcile_storage_mounts_once(True) is True

    assert mark_calls == ["mark"]
    assert "Restored missing storage readiness marker" in caplog.text


def test_unhealthy_mounts_remove_unexpected_marker_without_steady_rewrite(
        monkeypatch, caplog):
    main = _load_csi_module(monkeypatch, "main")
    marker = {"exists": True}
    clear_calls = []

    def clear_ready():
        marker["exists"] = False
        clear_calls.append("clear")

    monkeypatch.setattr(main, "mount_storage", lambda: False)
    monkeypatch.setattr(main, "clear_storage_ready", clear_ready)
    monkeypatch.setattr(
        main,
        "storage_ready_marker_exists",
        lambda: marker["exists"],
    )

    assert main.reconcile_storage_mounts_once(False) is False
    assert main.reconcile_storage_mounts_once(False) is False

    assert clear_calls == ["clear"]
    assert "Removed unexpected storage readiness marker" in caplog.text


def test_background_mount_reconciliation_runs_every_five_seconds(
        monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    events = []

    def reconcile(previous_ready):
        events.append(("reconcile", previous_ready))
        if len(events) == 5:
            raise KeyboardInterrupt
        return previous_ready is not True

    monkeypatch.setattr(main, "reconcile_storage_mounts_once", reconcile)
    monkeypatch.setattr(
        main.time,
        "sleep",
        lambda seconds: events.append(("sleep", seconds)),
    )

    with pytest.raises(KeyboardInterrupt):
        main.reconcile_storage_mounts()

    assert events == [
        ("reconcile", None),
        ("sleep", main.STORAGE_MOUNT_RETRY_SECONDS),
        ("reconcile", True),
        ("sleep", main.STORAGE_MOUNT_RETRY_SECONDS),
        ("reconcile", False),
    ]


def test_background_reconciler_survives_failure_and_restores_readiness(
        monkeypatch, caplog):
    main = _load_csi_module(monkeypatch, "main")
    outcomes = iter([
        True,
        RuntimeError("the vault map is malformed"),
        False,
        True,
    ])
    events = []
    clear_attempts = 0
    marker = {"exists": False}

    def mount_storage():
        outcome = next(outcomes)
        events.append(("mount", outcome is True))
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def sleep(seconds):
        events.append(("sleep", seconds))
        if sum(event[0] == "sleep" for event in events) == 4:
            raise KeyboardInterrupt

    def clear_ready():
        nonlocal clear_attempts
        clear_attempts += 1
        events.append(("clear", clear_attempts))
        if clear_attempts == 1:
            raise OSError(errno.EIO, "the stale readiness marker is busy")
        marker["exists"] = False

    def mark_ready():
        marker["exists"] = True
        events.append(("ready", True))

    monkeypatch.setattr(main, "mount_storage", mount_storage)
    monkeypatch.setattr(
        main,
        "mark_storage_ready",
        mark_ready,
    )
    monkeypatch.setattr(
        main,
        "clear_storage_ready",
        clear_ready,
    )
    monkeypatch.setattr(
        main,
        "storage_ready_marker_exists",
        lambda: marker["exists"],
    )
    monkeypatch.setattr(main.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        main.reconcile_storage_mounts()

    assert events == [
        ("mount", True),
        ("ready", True),
        ("sleep", main.STORAGE_MOUNT_RETRY_SECONDS),
        ("mount", False),
        ("clear", 1),
        ("sleep", main.STORAGE_MOUNT_RETRY_SECONDS),
        ("mount", False),
        ("clear", 2),
        ("sleep", main.STORAGE_MOUNT_RETRY_SECONDS),
        ("mount", True),
        ("ready", True),
        ("sleep", main.STORAGE_MOUNT_RETRY_SECONDS),
    ]
    assert "Storage mount reconciliation failed" in caplog.text
    assert (
        "Unable to clear storage readiness after reconcile error"
        in caplog.text
    )


def test_background_reconciler_survives_readiness_marker_io_errors(
        monkeypatch, caplog):
    main = _load_csi_module(monkeypatch, "main")
    events = []
    mark_attempts = 0
    marker = {"exists": False}

    def mark_ready():
        nonlocal mark_attempts
        mark_attempts += 1
        events.append(("mark", mark_attempts))
        if mark_attempts == 1:
            raise OSError(errno.EIO, "the readiness marker write failed")
        marker["exists"] = True

    def clear_ready():
        events.append(("clear", True))
        raise OSError(errno.EIO, "the readiness marker clear failed")

    def sleep(_seconds):
        if mark_attempts == 2:
            raise KeyboardInterrupt

    monkeypatch.setattr(main, "mount_storage", lambda: True)
    monkeypatch.setattr(main, "mark_storage_ready", mark_ready)
    monkeypatch.setattr(main, "clear_storage_ready", clear_ready)
    monkeypatch.setattr(
        main,
        "storage_ready_marker_exists",
        lambda: marker["exists"],
    )
    monkeypatch.setattr(main.time, "sleep", sleep)

    with pytest.raises(KeyboardInterrupt):
        main.reconcile_storage_mounts()

    assert events == [
        ("mark", 1),
        ("clear", True),
        ("mark", 2),
    ]
    assert "Storage mount reconciliation failed" in caplog.text
    assert (
        "Unable to clear storage readiness after reconcile error"
        in caplog.text
    )


@pytest.mark.parametrize("terminal_error", [None, KeyboardInterrupt()])
def test_reconciler_runner_clears_readiness_on_every_terminal_path(
        monkeypatch, terminal_error):
    main = _load_csi_module(monkeypatch, "main")
    events = []

    def reconcile():
        events.append("reconcile")
        if terminal_error is not None:
            raise terminal_error

    monkeypatch.setattr(main, "reconcile_storage_mounts", reconcile)
    monkeypatch.setattr(
        main,
        "clear_storage_ready",
        lambda: events.append("unready"),
    )

    if terminal_error is None:
        main.run_storage_mount_reconciler()
    else:
        with pytest.raises(type(terminal_error)):
            main.run_storage_mount_reconciler()

    assert events == ["reconcile", "unready"]


def test_reconciler_watchdog_restarts_dead_thread_fail_closed(monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    events = []

    class FakeReconciler:
        """Expose only the liveness state consumed by the watchdog."""

        def __init__(self, alive):
            self.alive = alive

        def is_alive(self):
            return self.alive

    replacement = FakeReconciler(True)
    monkeypatch.setattr(
        main,
        "clear_storage_ready",
        lambda: events.append("unready"),
    )
    monkeypatch.setattr(
        main,
        "start_storage_mount_reconciler",
        lambda: events.append("restart") or replacement,
    )

    assert main.ensure_storage_mount_reconciler(
        FakeReconciler(False)
    ) is replacement
    assert events == ["unready", "restart"]

    events.clear()
    assert main.ensure_storage_mount_reconciler(replacement) is replacement
    assert events == []


def test_spawned_reconciler_uses_terminal_cleanup_runner(monkeypatch):
    main = _load_csi_module(monkeypatch, "main")
    captured = {}

    class FakeThread:
        """Capture thread construction without running the infinite target."""

        def __init__(self, *, target, name, daemon):
            captured.update(target=target, name=name, daemon=daemon)

        def start(self):
            captured["started"] = True

    monkeypatch.setattr(main.threading, "Thread", FakeThread)

    reconciler = main.start_storage_mount_reconciler()

    assert isinstance(reconciler, FakeThread)
    assert captured == {
        "target": main.run_storage_mount_reconciler,
        "name": "storage-mount-reconciler",
        "daemon": True,
        "started": True,
    }


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
    monkeypatch.setattr(
        volumeutils,
        "_remove_stale_gluster_mount",
        lambda *_args: None,
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


def test_native_mount_detaches_stale_fuse_target_before_launch(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _native_volume(tmp_path, volumeutils)
    mountpoint = str(tmp_path / "bellagio-mount")
    events = []

    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda *_args: False,
    )
    monkeypatch.setattr(volumeutils, "is_server_pod_reachable", lambda *_args: True)
    monkeypatch.setattr(
        volumeutils,
        "_remove_stale_gluster_mount",
        lambda volname, target: events.append(("detach", volname, target)),
    )
    monkeypatch.setattr(
        volumeutils,
        "_execute_glusterfs_mount",
        lambda _command, volname, target:
            events.append(("launch", volname, target)),
    )

    assert volumeutils.mount_glusterfs(volume, mountpoint) == mountpoint
    assert events == [
        ("detach", "bellagio-pool", mountpoint),
        ("launch", "bellagio-pool", mountpoint),
    ]


def test_native_mount_never_probes_stale_fuse_path_before_detach(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    volume = _native_volume(tmp_path, volumeutils)
    mountpoint = str(tmp_path / "bellagio-stale-mount")
    stale = True
    original_exists = volumeutils.os.path.exists
    events = []

    def guarded_exists(path):
        if stale and str(path) == mountpoint:
            raise OSError(errno.ENOTCONN, "the fake FUSE client disconnected")
        return original_exists(path)

    def remove_stale(volname, target):
        nonlocal stale
        events.append(("detach", volname, target))
        stale = False

    monkeypatch.setattr(
        volumeutils,
        "is_gluster_mount_established",
        lambda *_args: False,
    )
    monkeypatch.setattr(volumeutils, "is_server_pod_reachable", lambda *_args: True)
    monkeypatch.setattr(volumeutils.os.path, "exists", guarded_exists)
    monkeypatch.setattr(volumeutils, "_remove_stale_gluster_mount", remove_stale)
    monkeypatch.setattr(
        volumeutils,
        "_execute_glusterfs_mount",
        lambda _command, volname, target:
            events.append(("launch", volname, target)),
    )

    assert volumeutils.mount_glusterfs(volume, mountpoint) == mountpoint
    assert events == [
        ("detach", "bellagio-pool", mountpoint),
        ("launch", "bellagio-pool", mountpoint),
    ]


def test_stale_cleanup_refuses_to_unmount_a_different_gluster_volume(
        monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    mountpoint = tmp_path / "bellagio-mount"
    mounts_file = tmp_path / "mounts"
    mounts_file.write_text(
        f"kadalu:benedicts-vault {mountpoint} "
        "fuse.glusterfs rw,relatime 0 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(volumeutils, "MOUNTS_FILE", str(mounts_file))
    monkeypatch.setattr(
        volumeutils,
        "execute",
        lambda *_args: pytest.fail("a different healthy mount was unmounted"),
    )

    with pytest.raises(volumeutils.MountTargetConflictError):
        volumeutils._remove_stale_gluster_mount(
            "bellagio-pool",
            str(mountpoint),
        )


def test_capacity_accounting_uses_statvfs_fragment_size(monkeypatch, tmp_path):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    pool = "bellagio-capacity-pool"
    mount_root = tmp_path / "mnt"
    pool_root = mount_root / pool
    pool_root.mkdir(parents=True)
    monkeypatch.setattr(volumeutils, "HOSTVOL_MOUNTDIR", str(mount_root))
    monkeypatch.setattr(
        volumeutils,
        "mount_glusterfs",
        lambda _volume, mountpoint: mountpoint,
    )
    monkeypatch.setattr(
        volumeutils,
        "retry_errors",
        lambda function, args, _errors: function(*args),
    )
    monkeypatch.setattr(
        volumeutils.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            f_blocks=100,
            f_bsize=4096,
            f_frsize=10,
        ),
    )

    selected = volumeutils.mount_and_select_hosting_volume(
        [{"name": pool, "type": "Replica1"}],
        950,
    )

    assert selected is None
    with volumeutils.SizeAccounting(pool, str(pool_root)) as accounting:
        assert accounting.get_stats()["total_size_bytes"] == 1000


def test_simple_quota_verification_uses_statvfs_fragment_size(
        monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    stats = SimpleNamespace(f_blocks=100, f_bsize=4096, f_frsize=10)
    monkeypatch.setattr(
        volumeutils,
        "retry_errors",
        lambda _function, _args, _errors: stats,
    )
    monkeypatch.setattr(
        volumeutils.time,
        "sleep",
        lambda _seconds: pytest.fail("correct quota capacity was not observed"),
    )

    volumeutils._wait_for_simple_quota(
        "/mnt/bellagio",
        "subvol/aa/bb/pvc-rusty-ryan",
        1000,
        "creation",
    )


def test_successful_gluster_command_requires_kernel_mount(monkeypatch):
    volumeutils = _load_csi_module(monkeypatch, "volumeutils")
    monkeypatch.setattr(volumeutils, "execute", lambda *_args: ("", "", 101))
    monkeypatch.setattr(
        volumeutils,
        "_wait_for_gluster_mount",
        lambda *_args: False,
    )

    with pytest.raises(volumeutils.CommandException, match="without establishing"):
        volumeutils._execute_glusterfs_mount(
            ["glusterfs", "/mnt/bellagio"],
            "bellagio-vault",
            "/mnt/bellagio",
        )


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
    monkeypatch.setattr(
        volumeutils,
        "_wait_for_gluster_mount",
        lambda *_args: True,
    )
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
