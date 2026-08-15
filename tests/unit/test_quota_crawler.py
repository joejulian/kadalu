"""Behavior tests for the CSI quota crawler."""

import os
import signal
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
CRAWLER = ROOT / "csi" / "quota-crawler.sh"
VOLUME_DIR = "/mnt/bellagio-pool/subvol/11/22/pvc-oceans-eleven"


def _write_executable(path, contents):
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


@pytest.fixture(name="crawler_run")
def fixture_crawler_run(tmp_path):
    """Run exactly one crawler pass with controlled command results."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    state = tmp_path / "setfattr-state"
    calls = tmp_path / "setfattr-calls"

    _write_executable(
        bin_dir / "find",
        """#!/bin/sh
case " $* " in
  *" -printf . "*) printf '.' ;;
  *) printf '%s\\n' "$QUOTA_TEST_DIR" ;;
esac
""",
    )
    _write_executable(
        bin_dir / "df",
        """#!/bin/sh
printf '%s\\n' 'Filesystem 1B-blocks Used Available Use% Mounted on'
printf '%s\\n' 'bellagio 10000 4242 5758 43% /mnt/bellagio-pool'
""",
    )
    _write_executable(
        bin_dir / "setfattr",
        """#!/bin/sh
count=0
if [ -f "$QUOTA_TEST_STATE" ]; then
  count=$(cat "$QUOTA_TEST_STATE")
fi
count=$((count + 1))
printf '%s\\n' "$count" >"$QUOTA_TEST_STATE"
printf '%s\\n' "$*" >>"$QUOTA_TEST_CALLS"

case "$QUOTA_TEST_SCENARIO:$count" in
  success:*)
    exit 0
    ;;
  unsupported:1)
    echo 'setfattr: Operation not supported' >&2
    exit 1
    ;;
  recover:1|namespace-fails:1|retry-fails:1)
    echo 'setfattr: Operation not permitted' >&2
    exit 1
    ;;
  namespace-fails:2)
    echo 'setfattr: Read-only file system' >&2
    exit 1
    ;;
  retry-fails:3|unrelated-error:1)
    echo 'setfattr: Input/output error' >&2
    exit 1
    ;;
esac
""",
    )
    _write_executable(
        bin_dir / "sleep",
        """#!/bin/sh
kill -TERM "$PPID"
""",
    )

    def run(scenario):
        env = os.environ.copy()
        env.update({
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "QUOTA_TEST_CALLS": str(calls),
            "QUOTA_TEST_DIR": VOLUME_DIR,
            "QUOTA_TEST_SCENARIO": scenario,
            "QUOTA_TEST_STATE": str(state),
        })
        completed = subprocess.run(
            ["bash", str(CRAWLER)],
            check=False,
            env=env,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            text=True,
            timeout=5,
        )
        assert completed.returncode in (-signal.SIGTERM, 128 + signal.SIGTERM)
        recorded_calls = calls.read_text(encoding="utf-8").splitlines()
        return completed.stdout, recorded_calls

    return run


def _quota_call():
    return (
        "-n glusterfs.quota.total-usage -v 4242 "
        f"{VOLUME_DIR}"
    )


def _namespace_call():
    return (
        "-n trusted.glusterfs.namespace -v true "
        f"{VOLUME_DIR}"
    )


def test_success_does_not_rewrite_namespace(crawler_run):
    output, calls = crawler_run("success")

    assert calls == [_quota_call()]
    assert "Failed to" not in output


def test_unsupported_quota_xattr_is_ignored_without_namespace_rewrite(
        crawler_run):
    output, calls = crawler_run("unsupported")

    assert calls == [_quota_call()]
    assert "Operation not supported" not in output


def test_failed_quota_update_restores_namespace_and_retries(crawler_run):
    output, calls = crawler_run("recover")

    assert calls == [_quota_call(), _namespace_call(), _quota_call()]
    assert "Failed to update quota usage" in output
    assert "Operation not permitted" in output


def test_namespace_restore_failure_is_reported_without_retry(crawler_run):
    output, calls = crawler_run("namespace-fails")

    assert calls == [_quota_call(), _namespace_call()]
    assert "Failed to restore quota namespace" in output
    assert "Read-only file system" in output


def test_quota_retry_failure_is_reported(crawler_run):
    output, calls = crawler_run("retry-fails")

    assert calls == [_quota_call(), _namespace_call(), _quota_call()]
    assert "after restoring namespace" in output
    assert "Input/output error" in output


def test_unrelated_failure_is_reported_without_namespace_rewrite(crawler_run):
    output, calls = crawler_run("unrelated-error")

    assert calls == [_quota_call()]
    assert "Failed to update quota usage" in output
    assert "Input/output error" in output
