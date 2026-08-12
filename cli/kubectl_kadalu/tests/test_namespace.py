"""Namespace behavior for kubectl-kadalu."""

import importlib
import json
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import healinfo
import install
import logs
import option_reset
import option_set
import remove_archived_pv
import storage_add
import storage_list
import storage_remove
import utils


NAMESPACE = "bellagio-crew"
CONTEXT = "night-shift"


def args(**overrides):
    """Build command arguments with harmless fictional values."""
    values = {
        "kubectl_cmd": "k3s kubectl",
        "kubectl_context": CONTEXT,
        "namespace": NAMESPACE,
        "verbose": False,
        "dry_run": False,
        "script_mode": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def response(stdout="", stderr=""):
    """Return a successful command response."""
    return utils.CmdResponse(0, stdout, stderr)


def assert_namespaced(command):
    """Assert that kubectl is pinned to both context and namespace."""
    assert command[:4] == ["k3s", "kubectl", "--context", CONTEXT]
    assert ["--namespace", NAMESPACE] == command[4:6]


@pytest.mark.parametrize(
    "argv",
    [
        ["kubectl-kadalu", "-n", NAMESPACE, "version"],
        ["kubectl-kadalu", "version", "-n", NAMESPACE],
        ["kubectl-kadalu", "--namespace", NAMESPACE, "storage-list"],
        ["kubectl-kadalu", "storage-list", "--namespace", NAMESPACE],
    ],
)
def test_namespace_is_accepted_on_either_side_of_subcommand(monkeypatch, argv):
    cli = importlib.import_module("kubectl_kadalu.__main__")
    monkeypatch.setattr(sys, "argv", argv)

    parsed = cli.get_args()

    assert parsed.namespace == NAMESPACE


def test_namespace_before_subcommand_is_not_replaced_by_default(monkeypatch):
    cli = importlib.import_module("kubectl_kadalu.__main__")
    monkeypatch.setattr(
        sys,
        "argv",
        ["kubectl-kadalu", "--namespace", NAMESPACE, "logs", "--podname", "pod/ocean"],
    )

    parsed = cli.get_args()

    assert parsed.namespace == NAMESPACE


@pytest.mark.parametrize("namespace", ["Bellagio", "two words", "../../vault", "crew_"])
def test_invalid_kubernetes_namespace_is_rejected(namespace):
    with pytest.raises(Exception):
        utils.validate_namespace(namespace)


def test_namespaced_kubectl_command_is_explicit():
    command = utils.namespaced_kubectl_cmd(args())

    assert_namespaced(command)


def test_version_queries_selected_namespace(monkeypatch):
    cli = importlib.import_module("kubectl_kadalu.__main__")
    calls = []
    replies = iter([response("pod/operator\n"), response("heist-test\n")])

    def execute(command):
        calls.append(command)
        return next(replies)

    monkeypatch.setattr(utils, "execute", execute)
    command_args = args()

    pods = cli.get_all_kadalu_pods(command_args)
    version = cli.get_kadalu_version_in_pod(pods[0], command_args)

    assert version == "heist-test"
    assert all(assert_namespaced(command) is None for command in calls)


def test_log_and_heal_commands_use_selected_namespace(monkeypatch):
    calls = []
    replies = iter([
        response("log line\n"),
        response("Running"),
        response("heal info\n"),
        response("heal triggered\n"),
    ])

    def execute(command):
        calls.append(command)
        return next(replies)

    monkeypatch.setattr(utils, "execute", execute)
    command_args = args(
        podname="pod/ocean",
        container=None,
        allcontainers=False,
        name="vault",
    )

    logs.run(command_args)
    assert healinfo.check_server_pod_is_up("server-vault-0-0", command_args)
    healinfo.exec_server_and_fetch_healinfo("server-vault-0-0", command_args)
    healinfo.exec_csi_and_heal(command_args)

    assert all(assert_namespaced(command) is None for command in calls)


def test_storage_reads_and_archived_pv_request_use_selected_namespace(monkeypatch):
    calls = []
    configmap = json.dumps({"data": {"vault.info": "{}"}})
    replies = iter([
        response(configmap),
        response(configmap),
        response(json.dumps({"spec": {"options": []}})),
        response("deleted\n"),
    ])

    def execute(command):
        calls.append(command)
        return next(replies)

    monkeypatch.setattr(utils, "execute", execute)
    command_args = args(name="vault", pvc=None)

    assert storage_remove.get_configmap_data(command_args) == {}
    assert remove_archived_pv.get_configmap_data(command_args) == {}
    assert storage_list.get_options_from_crd(command_args, "vault") == []
    assert remove_archived_pv.request_pv_delete(command_args).stdout == "deleted\n"

    assert all(assert_namespaced(command) is None for command in calls)


@pytest.mark.parametrize("module", [option_set, option_reset])
def test_storage_option_read_and_apply_use_selected_namespace(monkeypatch, tmp_path, module):
    calls = []
    storage = json.dumps({
        "metadata": {"name": "vault", "namespace": NAMESPACE},
        "spec": {"options": [{"key": "performance.quick-read", "value": "on"}]},
    })

    def execute(command):
        calls.append(command)
        return response(storage if "get" in command else "configured\n")

    monkeypatch.setattr(utils, "execute", execute)
    monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
    command_args = args(
        name="vault",
        options=["performance.quick-read", "off"] if module is option_set else ["performance.quick-read"],
        all=False,
    )

    module.run(command_args)

    assert len(calls) == 2
    assert all(assert_namespaced(command) is None for command in calls)


def test_storage_add_yaml_and_apply_use_selected_namespace(monkeypatch):
    calls = []

    def execute(command):
        calls.append(command)
        return response("created\n")

    monkeypatch.setattr(utils, "execute", execute)
    command_args = args(
        name="vault",
        type="Replica1",
        pv_reclaim_policy=None,
        volume_id=None,
        single_pv_per_pool=False,
        external=None,
        device=["rusty.example.invalid:/dev/fake"],
        path=[],
        pvc=[],
        tiebreaker=None,
        disperse_data=0,
        disperse_redundancy=0,
    )

    data = storage_add.storage_add_data(command_args)
    storage_add.run(command_args)

    assert data["metadata"]["namespace"] == NAMESPACE
    assert 'namespace: "bellagio-crew"' in storage_add.to_storage_yaml(data)
    assert len(calls) == 1
    assert_namespaced(calls[0])


def test_storage_remove_yaml_and_delete_use_selected_namespace(monkeypatch, capsys):
    calls = []

    def execute(command):
        calls.append(command)
        return response("deleted\n")

    monkeypatch.setattr(utils, "execute", execute)

    storage_remove.run(args(name="vault"))

    assert 'namespace: "bellagio-crew"' in capsys.readouterr().out
    assert len(calls) == 1
    assert_namespaced(calls[0])


def test_custom_namespace_is_rendered_through_operator_manifest():
    manifest = """---
kind: Namespace
apiVersion: v1
metadata:
  name: kadalu
---
kind: ServiceAccount
apiVersion: v1
metadata:
  name: kadalu-operator
  namespace: kadalu
---
kind: Deployment
apiVersion: apps/v1
metadata:
  name: operator
  namespace: kadalu
spec:
  template:
    spec:
      containers:
        - name: kadalu-operator
          env:
            - name: KADALU_NAMESPACE
              value: "kadalu"
"""

    rendered = install.render_operator_manifest(manifest, NAMESPACE)

    assert "namespace: kadalu" not in rendered
    assert "name: KADALU_NAMESPACE\n              value: \"bellagio-crew\"" in rendered
    assert "kind: Namespace\napiVersion: v1\nmetadata:\n  name: bellagio-crew" in rendered


def test_openshift_service_account_subjects_use_selected_namespace():
    manifest = """---
kind: SecurityContextConstraints
apiVersion: security.openshift.io/v1
metadata:
  name: kadalu-scc
users:
  - system:serviceaccount:kadalu:kadalu-server-sa
  - system:serviceaccount:kadalu:kadalu-operator
"""

    rendered = install.render_operator_manifest(manifest, NAMESPACE)

    assert "system:serviceaccount:kadalu:" not in rendered
    assert "system:serviceaccount:bellagio-crew:kadalu-server-sa" in rendered
    assert "system:serviceaccount:bellagio-crew:kadalu-operator" in rendered


@pytest.mark.parametrize(
    "manifest_name",
    [
        "kadalu-operator.yaml",
        "kadalu-operator-microk8s.yaml",
        "kadalu-operator-openshift.yaml",
        "kadalu-operator-rke.yaml",
    ],
)
def test_current_operator_manifests_retarget_all_namespace_references(manifest_name):
    repository_root = Path(__file__).resolve().parents[3]
    manifest = (repository_root / "manifests" / manifest_name).read_text(
        encoding="utf-8"
    )

    rendered = install.render_operator_manifest(manifest, NAMESPACE)

    assert re.search(r"(?m)^\s*namespace:\s*['\"]?kadalu['\"]?\s*$", rendered) is None
    assert "kind: Namespace\napiVersion: v1\nmetadata:\n  name: kadalu" not in rendered
    assert "system:serviceaccount:kadalu:" not in rendered
    assert "name: KADALU_NAMESPACE\n              value: \"bellagio-crew\"" in rendered


def test_install_checks_and_applies_to_selected_namespace(monkeypatch, tmp_path):
    calls = []
    operator_file = tmp_path / "operator.yaml"
    operator_file.write_text(
        "kind: Namespace\napiVersion: v1\nmetadata:\n  name: kadalu\n",
        encoding="utf-8",
    )
    replies = iter([response(""), response("configured\n")])

    def execute(command):
        calls.append(command)
        return next(replies)

    monkeypatch.setattr(utils, "execute", execute)
    command_args = args(local_yaml=str(operator_file), version="heist-test", type="kubernetes")

    install.run(command_args)

    assert calls[0][4:] == ["get", "namespace", NAMESPACE, "--ignore-not-found", "-oname"]
    assert_namespaced(calls[1])
    rendered_path = Path(calls[1][-1])
    # The temporary manifest is cleaned after kubectl returns.
    assert not rendered_path.exists()


def test_cli_sources_do_not_hardcode_kadalu_namespace():
    source_dir = Path(__file__).resolve().parents[1]
    for source in source_dir.glob("*.py"):
        content = source.read_text(encoding="utf-8")
        assert "-nkadalu" not in content
        assert '"-n", "kadalu"' not in content
