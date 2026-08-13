"""Tests for storage-device preparation in the server pod."""

import importlib
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]


def _load_glusterfsd(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "lib"))
    monkeypatch.syspath_prepend(str(ROOT / "server"))
    serverutils = types.ModuleType("serverutils")
    serverutils.generate_brick_volfile = lambda *_args: None
    serverutils.generate_client_volfile = lambda *_args: None
    monkeypatch.setitem(sys.modules, "serverutils", serverutils)
    sys.modules.pop("glusterfsd", None)
    return importlib.import_module("glusterfsd")


def test_xfs_brick_grows_after_mount(monkeypatch, tmp_path):
    glusterfsd = _load_glusterfsd(monkeypatch)
    commands = []

    monkeypatch.setattr(
        glusterfsd,
        "execute",
        lambda *command: commands.append(command),
    )

    brick_path = tmp_path / "brick" / "data"
    glusterfsd.create_and_mount_brick(
        "/dev/bellagio-vault",
        str(brick_path),
        "xfs",
    )

    assert commands == [
        ("mount", "/dev/bellagio-vault", str(brick_path.parent)),
        (glusterfsd.XFS_GROWFS_CMD, "-d", str(brick_path.parent)),
    ]


def test_already_mounted_xfs_brick_still_grows(monkeypatch, tmp_path):
    glusterfsd = _load_glusterfsd(monkeypatch)
    commands = []

    def fake_execute(*command):
        commands.append(command)
        if command[0] == "mount":
            raise glusterfsd.CommandException(
                32,
                "mount",
                "already mounted",
            )

    monkeypatch.setattr(glusterfsd, "execute", fake_execute)

    brick_path = tmp_path / "brick" / "data"
    glusterfsd.create_and_mount_brick(
        "/dev/bellagio-vault",
        str(brick_path),
        "xfs",
    )

    assert commands[-1] == (
        glusterfsd.XFS_GROWFS_CMD,
        "-d",
        str(brick_path.parent),
    )


def test_non_xfs_brick_does_not_run_xfs_growfs(monkeypatch, tmp_path):
    glusterfsd = _load_glusterfsd(monkeypatch)
    commands = []

    monkeypatch.setattr(
        glusterfsd,
        "execute",
        lambda *command: commands.append(command),
    )

    brick_path = tmp_path / "brick" / "data"
    glusterfsd.create_and_mount_brick(
        "/dev/bellagio-vault",
        str(brick_path),
        "ext4",
    )

    assert commands == [
        ("mount", "/dev/bellagio-vault", str(brick_path.parent)),
    ]


def test_xfs_growth_failure_stops_brick_startup(monkeypatch, tmp_path):
    glusterfsd = _load_glusterfsd(monkeypatch)

    def fake_execute(*command):
        if command[0] == glusterfsd.XFS_GROWFS_CMD:
            raise glusterfsd.CommandException(
                1,
                "xfs_growfs",
                "the vault device cannot be grown",
            )

    monkeypatch.setattr(glusterfsd, "execute", fake_execute)

    brick_path = tmp_path / "brick" / "data"
    with pytest.raises(SystemExit) as exit_info:
        glusterfsd.create_and_mount_brick(
            "/dev/bellagio-vault",
            str(brick_path),
            "xfs",
        )

    assert exit_info.value.code == 1
