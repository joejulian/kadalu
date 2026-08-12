"""Tests for shared Kadalu helpers."""

import socket
import threading
import time

import pytest

from lib.kadalulib import is_host_reachable, is_server_pod_reachable


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
