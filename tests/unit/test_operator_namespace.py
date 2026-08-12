"""Operator namespace behavior tests."""

import importlib
import json
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _load_module(monkeypatch, module_name):
    monkeypatch.syspath_prepend(str(ROOT / "cli" / "kubectl_kadalu"))
    monkeypatch.syspath_prepend(str(ROOT / "kadalu_operator"))
    fake_kadalulib = types.ModuleType("kadalulib")
    fake_kadalulib.CommandException = RuntimeError
    fake_kadalulib.execute = lambda *args, **kwargs: None
    fake_kadalulib.get_single_pv_per_pool = lambda spec: False
    fake_kadalulib.is_host_reachable = lambda host, port: True
    fake_kadalulib.logf = lambda message, **kwargs: message
    fake_kadalulib.logging_setup = lambda: None
    fake_kadalulib.send_analytics_tracker = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "kadalulib", fake_kadalulib)
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_operator_reads_configmap_from_configured_namespace(monkeypatch):
    monkeypatch.setenv("KADALU_NAMESPACE", "bellagio-crew")
    main = _load_module(monkeypatch, "kadalu_operator.main")
    commands = []

    class Response:
        stdout = json.dumps({
            "data": {
                "the-vault.info": json.dumps({"volname": "the-vault"}),
            },
        })

    monkeypatch.setattr(
        main,
        "utils_execute",
        lambda command: commands.append(command) or Response(),
    )

    assert main.get_configmap_data("the-vault") == {"volname": "the-vault"}
    assert commands == [[
        "/usr/bin/kubectl", "get", "configmap", "kadalu-info",
        "-n", "bellagio-crew", "-ojson",
    ]]


def test_operator_defaults_to_fork_registry(monkeypatch):
    monkeypatch.delenv("IMAGES_HUB", raising=False)
    main = _load_module(monkeypatch, "kadalu_operator.main")

    assert main.IMAGES_HUB == "ghcr.io"


def test_operator_stops_delete_when_metadata_is_unavailable(monkeypatch):
    main = _load_module(monkeypatch, "kadalu_operator.main")
    monkeypatch.setattr(main, "get_configmap_data", lambda _name: None)
    get_num_pvs_called = False

    def get_num_pvs(_data):
        nonlocal get_num_pvs_called
        get_num_pvs_called = True

    monkeypatch.setattr(main, "get_num_pvs", get_num_pvs)
    main.handle_deleted(None, {"metadata": {"name": "the-vault"}})

    assert get_num_pvs_called is False


def test_operator_watches_only_configured_namespace(monkeypatch):
    monkeypatch.setenv("KADALU_NAMESPACE", "bellagio-crew")
    main = _load_module(monkeypatch, "kadalu_operator.main")
    list_calls = []
    stream_calls = []

    class CustomObjects:
        def list_namespaced_custom_object(self, *args, **kwargs):
            list_calls.append((args, kwargs))
            return {
                "items": [],
                "metadata": {"resourceVersion": "eleven-ocean"},
            }

    custom_objects = CustomObjects()
    monkeypatch.setattr(
        main.client,
        "CustomObjectsApi",
        lambda _client: custom_objects,
    )

    class Watch:
        def stream(self, *args, **kwargs):
            stream_calls.append((args, kwargs))
            return []

    monkeypatch.setattr(main.watch, "Watch", Watch)
    main.watch_stream(None, object())

    assert list_calls == [(('kadalu-operator.storage', 'v1alpha1',
                            'bellagio-crew', 'kadalustorages'), {})]
    assert stream_calls[0][0][1:] == (
        "kadalu-operator.storage",
        "v1alpha1",
        "bellagio-crew",
        "kadalustorages",
    )
    assert stream_calls[0][1] == {"resource_version": "eleven-ocean"}


def test_exporter_queries_configured_namespace(monkeypatch):
    monkeypatch.setenv("KADALU_NAMESPACE", "bellagio-crew")
    exporter = _load_module(monkeypatch, "kadalu_operator.exporter")
    commands = []

    class Response:
        stdout = json.dumps({"items": []})

    monkeypatch.setattr(
        exporter,
        "execute",
        lambda command: commands.append(command) or Response(),
    )

    assert exporter.get_pod_data() == {}
    assert commands == [[
        "/usr/bin/kubectl", "get", "pods", "-l",
        "app.kubernetes.io/part-of=kadalu",
        "--field-selector=status.phase==Running",
        "-n", "bellagio-crew", "-ojson",
    ]]


def test_exporter_returns_empty_pods_when_kubectl_fails(monkeypatch):
    exporter = _load_module(monkeypatch, "kadalu_operator.exporter")

    def fail(_command):
        raise exporter.CommandError(1, "the casino is closed")

    monkeypatch.setattr(exporter, "execute", fail)
    assert exporter.get_pod_data() == {}
