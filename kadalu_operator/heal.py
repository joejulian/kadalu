"""Bounded, pool-wide heal queries for storage upgrade preflight checks."""

from functools import partial
import logging
import re
import subprocess
import time


COUNTERS = (
    "Total Number of entries",
    "Number of entries in heal pending",
    "Number of entries in split-brain",
    "Number of entries possibly healing",
)


class HealQueryError(RuntimeError):
    """A server could not supply a heal summary."""


def _parse_summary(summary, expected_bricks):
    if not isinstance(summary, str) or expected_bricks < 1:
        return None
    statuses = re.findall(r"^Status:[ \t]*(\S[^\n]*?)[ \t]*$",
                          summary, re.MULTILINE)
    if len(statuses) != expected_bricks:
        return None
    counters = []
    for label in COUNTERS:
        values = re.findall(
            rf"^{re.escape(label)}:[ \t]*([0-9]+)[ \t]*$",
            summary, re.MULTILINE,
        )
        if len(values) != expected_bricks:
            return None
        counters.extend(int(value) for value in values)
    return statuses, counters


def storage_heal_summary_is_complete(summary, expected_bricks):
    """Return whether the summary contains every brick and numeric counter."""
    return _parse_summary(summary, expected_bricks) is not None


def storage_heal_summary_is_clean(summary, expected_bricks):
    """Require every brick connected and every heal counter equal to zero."""
    parsed = _parse_summary(summary, expected_bricks)
    if parsed is None:
        return False
    statuses, counters = parsed
    return (all(status == "Connected" for status in statuses)
            and all(value == 0 for value in counters))


def query_summary(volname, server, timeout, namespace="kadalu",
                  kubectl_cmd="/usr/bin/kubectl"):
    """Query one server with a command timeout bounded by the polling deadline."""
    command = (
        kubectl_cmd, "-n", namespace, "exec", server, "-c", "server", "--",
        "/opt/libexec/glusterfs/glfsheal", volname, "info-summary",
        "volfile-path", f"/var/lib/kadalu/volfiles/{volname}.vol",
    )
    try:
        result = subprocess.run(command, check=True, timeout=timeout,
                                capture_output=True, text=True)
    except (OSError, subprocess.SubprocessError) as error:
        raise HealQueryError(f"heal query failed on {server}: {error}") from error
    return result.stdout


def wait_for_storage_heal(volname, server_names, timeout_seconds=900,
                          poll_interval=5, query=None):
    """Wait for a clean pool summary, using other servers only as fallbacks.

    ``query(server, remaining_seconds)`` may be supplied by callers with their
    own command runner; failures must raise HealQueryError. A complete summary
    covers the entire pool, even when it reports pending work.
    """
    servers = list(server_names)
    if not servers or timeout_seconds <= 0 or poll_interval <= 0:
        raise ValueError("heal polling needs servers and positive time bounds")
    expected_bricks = len(servers)
    if query is None:
        query = partial(query_summary, volname)

    deadline = time.monotonic() + timeout_seconds
    last_error = "no server accepted the heal query"
    while True:
        for server in servers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                summary = query(server, remaining)
            except HealQueryError as error:
                last_error = str(error)
                continue
            if storage_heal_summary_is_clean(summary, expected_bricks):
                logging.info("Storage heal gate is clean: pool=%s server=%s",
                             volname, server)
                return
            last_error = f"unclean or incomplete summary from {server}"
            if storage_heal_summary_is_complete(summary, expected_bricks):
                # Repeating this query from every brick cannot advance healing.
                servers.remove(server)
                servers.insert(0, server)
                break

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Timed out waiting for storage pool {volname} to report "
                f"every brick Connected with zero heal entries: {last_error}. "
                "Further brick StatefulSet rollouts were not attempted."
            )
        time.sleep(min(poll_interval, remaining))
