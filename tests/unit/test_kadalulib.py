"""Tests for shared Kadalu helpers."""

import socket
import sys
import threading
import time

import pytest

from lib.kadalulib import (CommandException, execute, get_legacy_volume_path,
                           get_volname_hash, get_volume_path,
                           is_host_reachable, is_server_pod_reachable,
                           volume_name_component)


def _listening_socket(address):
    listener = socket.socket(address[0], socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(address[1])
    listener.listen()
    return listener


def _accept_once(listener):
    connection, _ = listener.accept()
    connection.close()


def test_reachability_uses_healthy_server_when_first_server_is_down():
    listener = _listening_socket((socket.AF_INET, ("127.0.0.1", 0)))
    port = listener.getsockname()[1]
    accept_thread = threading.Thread(target=_accept_once, args=(listener,))
    accept_thread.start()

    try:
        started = time.monotonic()
        assert is_server_pod_reachable(
            ["127.0.0.2", "127.0.0.1"], port=port, timeout=1
        )
        assert time.monotonic() - started < 1
    finally:
        listener.close()
        accept_thread.join(timeout=1)


def test_reachability_returns_false_within_total_deadline():
    listener = _listening_socket((socket.AF_INET, ("127.0.0.1", 0)))
    port = listener.getsockname()[1]
    listener.close()

    started = time.monotonic()
    assert not is_server_pod_reachable(
        ["127.0.0.2", "127.0.0.1"], port=port, timeout=0.2
    )
    assert time.monotonic() - started < 1


def test_reachability_supports_ipv6():
    if not socket.has_ipv6:
        pytest.skip("IPv6 is unavailable")

    try:
        listener = _listening_socket((socket.AF_INET6, ("::1", 0)))
    except OSError:
        pytest.skip("IPv6 loopback is unavailable")

    port = listener.getsockname()[1]
    accept_thread = threading.Thread(target=_accept_once, args=(listener,))
    accept_thread.start()

    try:
        assert is_server_pod_reachable(["::1"], port=port, timeout=1)
    finally:
        listener.close()
        accept_thread.join(timeout=1)


def test_external_host_reachability_tries_host_after_connection_failure():
    listener = _listening_socket((socket.AF_INET, ("127.0.0.1", 0)))
    port = listener.getsockname()[1]
    accept_thread = threading.Thread(target=_accept_once, args=(listener,))
    accept_thread.start()

    try:
        assert is_host_reachable(["127.0.0.2", "127.0.0.1"], port)
    finally:
        listener.close()
        accept_thread.join(timeout=1)


def test_execute_terminates_a_command_that_exceeds_its_deadline():
    started = time.monotonic()

    with pytest.raises(CommandException) as raised:
        execute(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            timeout=0.05,
        )

    assert time.monotonic() - started < 1
    assert raised.value.ret == 124
    assert "timed out" in raised.value.err


def test_legacy_path_never_aliases_an_opaque_id_after_hash_collision():
    victim = "pvc-victim"
    crafted = "x4682/../pvc-victim"
    assert get_volname_hash(victim)[:4] == get_volname_hash(crafted)[:4]

    victim_path = get_legacy_volume_path(
        "subvol",
        get_volname_hash(victim),
        victim,
    )
    crafted_path = get_legacy_volume_path(
        "subvol",
        get_volname_hash(crafted),
        crafted,
    )

    assert victim_path is not None
    assert crafted_path is None


def test_encoded_layout_never_aliases_a_legacy_prefix_name():
    opaque = "../bellagio-9443"
    legacy_name = volume_name_component(opaque)
    assert get_volname_hash(opaque)[:4] == get_volname_hash(legacy_name)[:4]

    opaque_path = get_volume_path(
        "subvol",
        get_volname_hash(opaque),
        opaque,
    )
    legacy_path = get_legacy_volume_path(
        "subvol",
        get_volname_hash(legacy_name),
        legacy_name,
    )

    assert legacy_path is not None
    assert opaque_path != legacy_path
    assert opaque_path.startswith("subvol/.kadalu-v2/")
