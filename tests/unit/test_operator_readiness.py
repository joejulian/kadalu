"""Operator process-supervisor readiness tests."""

import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]


def _load_start(monkeypatch):
    events = []
    monkeypatch.syspath_prepend(str(ROOT))

    class FakeProc:
        """Record the process definition without starting a real crew."""

        def __init__(self, name, executable, args):
            self.name = name
            self.executable = executable
            self.args = args

    class FakeMonitor:
        """Expose supervisor actions for deterministic assertions."""

        instances = []
        fail_monitor = False

        def __init__(self):
            self.procs = []
            self.instances.append(self)

        def add_process(self, proc):
            self.procs.append(proc)

        def start_all(self):
            events.append("ocean-crew-started")

        def monitor(self):
            events.append("bellagio-monitored")
            if self.fail_monitor:
                raise RuntimeError("Bellagio supervisor stopped")

        def monitor_proc(self, state, terminating):
            events.append(("rusty-restarted", state.proc.name, terminating))
            return "Rusty Ryan"

    fake_kadalulib = types.ModuleType("kadalulib")
    fake_kadalulib.Monitor = FakeMonitor
    fake_kadalulib.Proc = FakeProc
    fake_kadalulib.logging_setup = lambda: None
    monkeypatch.setitem(sys.modules, "kadalulib", fake_kadalulib)
    sys.modules.pop("kadalu_operator.start", None)
    start = importlib.import_module("kadalu_operator.start")
    return start, FakeMonitor, events


def _process_state(name, return_code):
    return SimpleNamespace(
        enabled=True,
        proc=SimpleNamespace(name=name),
        subproc=SimpleNamespace(poll=lambda: return_code),
    )


def test_operator_child_exit_clears_readiness_before_restart(monkeypatch):
    start, monitor_type, events = _load_start(monkeypatch)
    monkeypatch.setattr(
        start,
        "clear_operator_ready",
        lambda: events.append("bellagio-unready"),
    )

    result = start.OperatorMonitor().monitor_proc(
        _process_state("operator", 11),
        terminating=False,
    )

    assert result == "Rusty Ryan"
    assert events == [
        "bellagio-unready",
        ("rusty-restarted", "operator", False),
    ]
    assert len(monitor_type.instances) == 1


def test_exporter_exit_does_not_clear_operator_readiness(monkeypatch):
    start, _monitor_type, events = _load_start(monkeypatch)
    monkeypatch.setattr(
        start,
        "clear_operator_ready",
        lambda: events.append("bellagio-unready"),
    )

    start.OperatorMonitor().monitor_proc(
        _process_state("metrics", 11),
        terminating=False,
    )

    assert events == [("rusty-restarted", "metrics", False)]


def test_supervisor_clears_readiness_at_start_and_on_exit(monkeypatch):
    start, monitor_type, events = _load_start(monkeypatch)
    monitor_type.fail_monitor = True
    monkeypatch.setattr(
        start,
        "clear_operator_ready",
        lambda: events.append("bellagio-unready"),
    )

    with pytest.raises(RuntimeError, match="Bellagio supervisor stopped"):
        start.main()

    assert events == [
        "bellagio-unready",
        "ocean-crew-started",
        "bellagio-monitored",
        "bellagio-unready",
    ]
    assert [proc.name for proc in monitor_type.instances[0].procs] == [
        "operator",
        "metrics",
    ]
