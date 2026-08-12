"""Tests for the unprivileged operator runtime."""

import os
import subprocess

import pytest
import yaml


def _operator_deployment():
    try:
        output = subprocess.run(
            [
                "helm",
                "template",
                "kadalu",
                "helm/kadalu",
                "--namespace",
                "the-vault",
                "--set",
                "operator.enabled=true",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        pytest.skip("helm is unavailable")

    return next(
        document
        for document in yaml.safe_load_all(output)
        if document
        and document.get("kind") == "Deployment"
        and document["metadata"]["name"] == "operator"
    )


def test_operator_runs_unprivileged_with_read_only_root():
    deployment = _operator_deployment()
    pod_spec = deployment["spec"]["template"]["spec"]
    pod_security = pod_spec["securityContext"]
    container = pod_spec["containers"][0]
    container_security = container["securityContext"]

    assert pod_security["runAsNonRoot"] is True
    assert pod_security["runAsUser"] == 1000
    assert pod_security["runAsGroup"] == 1000
    assert pod_security["seccompProfile"]["type"] == "RuntimeDefault"
    assert container_security == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "privileged": False,
        "readOnlyRootFilesystem": True,
    }
    assert {item["name"]: item for item in pod_spec["volumes"]}["tmp"] == {
        "name": "tmp",
        "emptyDir": {},
    }
    assert {item["name"]: item for item in container["volumeMounts"]}["tmp"] == {
        "name": "tmp",
        "mountPath": "/tmp",
    }


def test_rendered_manifests_are_separate_from_templates(tmp_path, monkeypatch):
    templates_dir = tmp_path / "templates"
    output_dir = tmp_path / "output"
    templates_dir.mkdir()
    (templates_dir / "service.yaml.j2").write_text(
        "name: {{ name }}\n", encoding="utf-8"
    )

    # Imports are intentionally local so the manifest-only test above does not
    # require the operator's runtime dependencies.
    from kadalu_operator import main

    monkeypatch.setattr(main, "TEMPLATES_DIR", os.fspath(templates_dir))
    output = output_dir / "service.yaml"
    main.template(os.fspath(output), name="the-vault")

    assert output.read_text(encoding="utf-8") == "name: the-vault"
    assert list(templates_dir.iterdir()) == [templates_dir / "service.yaml.j2"]
