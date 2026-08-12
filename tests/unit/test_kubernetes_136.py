"""Kubernetes 1.36 manifest compatibility tests."""

import subprocess
from pathlib import Path

import pytest
import yaml
from jinja2 import Template


ROOT = Path(__file__).resolve().parents[2]
SIDECAR_IMAGES = {
    "csi-node-driver-registrar": (
        "registry.k8s.io/sig-storage/csi-node-driver-registrar:"
        "v2.17.0@sha256:f9de845b170155199f2a2a3f9531cf13d78e31235e9db6b"
        "6582a8b0db0a50dad"
    ),
    "csi-provisioner": (
        "registry.k8s.io/sig-storage/csi-provisioner:"
        "v6.3.0@sha256:a4b0b1a37605b7b04a293e136edf7006ec1786a8eb3f4e5a"
        "945f81d667dcc371"
    ),
    "csi-resizer": (
        "registry.k8s.io/sig-storage/csi-resizer:"
        "v2.2.1@sha256:ea1d25e23479000c7e8eeb92d827df66258df4e482ca054c5"
        "e7ce3fc0f5c41a5"
    ),
    "csi-liveness-probe": (
        "registry.k8s.io/sig-storage/livenessprobe:"
        "v2.19.0@sha256:06da0d5b8908072f2e4522692aee8dc119fba7247a9658"
        "497e1153992cd777e9"
    ),
}


def _render_csi_documents():
    template = Template(
        (ROOT / "templates/csi.yaml.j2").read_text(encoding="utf-8")
    )
    rendered = template.render(
        namespace="the-vault",
        images_hub="registry.example.invalid",
        docker_user="ocean-crew",
        kadalu_version="test",
        k8s_dist="kubernetes",
        kubelet_dir="/var/lib/kubelet",
        verbose="no",
        csi_sidecar_registry="registry.k8s.io",
    )
    return [document for document in yaml.safe_load_all(rendered) if document]


def _helm_documents():
    try:
        rendered = subprocess.run(
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
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except FileNotFoundError:
        pytest.skip("helm is unavailable")
    return [document for document in yaml.safe_load_all(rendered) if document]


def test_fork_images_default_to_ghcr_namespace():
    values = yaml.safe_load(
        (ROOT / "helm/kadalu/values.yaml").read_text(encoding="utf-8")
    )

    assert values["global"]["image"] == {
        "registry": "ghcr.io",
        "repository": "joejulian",
        "pullPolicy": "IfNotPresent",
    }

    for manifest in sorted((ROOT / "manifests").glob("kadalu-operator*.yaml")):
        documents = [
            document
            for document in yaml.safe_load_all(
                manifest.read_text(encoding="utf-8")
            )
            if document
        ]
        operator = next(
            document
            for document in documents
            if document.get("kind") == "Deployment"
            and document["metadata"]["name"] == "operator"
        )
        container = operator["spec"]["template"]["spec"]["containers"][0]
        assert container["image"] == (
            "ghcr.io/joejulian/kadalu-operator:devel"
        )

    for dockerfile in (
        ROOT / "csi/Dockerfile",
        ROOT / "kadalu_operator/Dockerfile",
        ROOT / "server/Dockerfile",
    ):
        assert dockerfile.read_text(encoding="utf-8").splitlines()[0] == (
            'ARG builder_image="ghcr.io/joejulian/kadalu-builder:devel"'
        )


def test_kubernetes_development_tools_use_fork_image_defaults():
    minikube_script = (ROOT / "tests/minikube.sh").read_text(encoding="utf-8")
    assert (
        'KADALU_BUILD_IMAGE_REPO=${KADALU_BUILD_IMAGE_REPO:-"joejulian"}'
        in minikube_script
    )
    assert (
        'KADALU_IMAGE_REPO=${KADALU_IMAGE_REPO:-"ghcr.io/joejulian"}'
        in minikube_script
    )
    assert (
        'copy_image_to_cluster "${KADALU_BUILD_IMAGE_REPO}/kadalu-operator:'
        '${KADALU_VERSION}" "${KADALU_IMAGE_REPO}/kadalu-operator:'
        '${KADALU_VERSION}"'
        in minikube_script
    )

    run_local = (ROOT / "extras/scripts/run-local").read_text(encoding="utf-8")
    assert 'DOCKER_USER="${DOCKER_USER:-joejulian}"' in run_local
    assert 'IMAGES_HUB="${IMAGES_HUB:-ghcr.io}"' in run_local
    for image in ("kadalu-server", "kadalu-operator", "kadalu-csi"):
        assert (
            f'docker tag "${{DOCKER_USER}}/{image}:${{TAG}}" '
            f'"${{IMAGES_HUB}}/${{DOCKER_USER}}/{image}:${{TAG}}"'
            in run_local
        )

    for job_file in ("controller.nomad", "nodeplugin.nomad"):
        nomad_job = (ROOT / "nomad" / job_file).read_text(encoding="utf-8")
        assert 'default = "devel"' in nomad_job.replace("     =", " =")
        assert (
            'image = "ghcr.io/joejulian/kadalu-csi:'
            '${var.kadalu_version}"' in nomad_job
        )


def test_current_multi_arch_csi_sidecars_are_digest_pinned():
    documents = _render_csi_documents()
    containers = {
        container["name"]: container
        for document in documents
        for container in document["spec"]["template"]["spec"]["containers"]
    }

    for name, image in SIDECAR_IMAGES.items():
        assert containers[name]["image"] == image

    registrar = containers["csi-node-driver-registrar"]
    assert "--http-endpoint=:9807" in registrar["args"]
    assert registrar["livenessProbe"]["httpGet"] == {
        "path": "/healthz",
        "port": "registrar-hlth",
    }


def test_controller_sidecars_have_leader_election_health_checks():
    documents = _render_csi_documents()
    provisioner = next(
        document
        for document in documents
        if document["metadata"]["name"] == "kadalu-csi-provisioner"
    )
    containers = {
        container["name"]: container
        for container in provisioner["spec"]["template"]["spec"]["containers"]
    }

    assert "csi-attacher" not in containers
    for name in ("csi-provisioner", "csi-resizer"):
        assert "--leader-election" in containers[name]["args"]
        assert containers[name]["livenessProbe"]["httpGet"]["path"] == (
            "/healthz/leader-election"
        )


def test_nodeplugin_upgrade_requires_explicit_node_by_node_deletion():
    nodeplugin = next(
        document
        for document in _render_csi_documents()
        if document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )

    assert nodeplugin["spec"]["updateStrategy"] == {"type": "OnDelete"}


def test_node_health_check_never_restarts_fuse_owner():
    nodeplugin = next(
        document
        for document in _render_csi_documents()
        if document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )
    containers = {
        container["name"]: container
        for container in nodeplugin["spec"]["template"]["spec"]["containers"]
    }

    assert "livenessProbe" not in containers["kadalu-nodeplugin"]
    assert containers["csi-liveness-probe"]["readinessProbe"]["httpGet"] == {
        "path": "/healthz",
        "port": 9808,
    }
    assert containers["kadalu-logging"]["image"].startswith(
        "registry.example.invalid/library/busybox:"
    )


def test_csi_rbac_supports_current_sidecar_apis():
    documents = _helm_documents()
    roles = {
        (document["kind"], document["metadata"]["name"]): document
        for document in documents
        if document["kind"] in {"ClusterRole", "Role"}
    }

    leader_role = roles[("Role", "kadalu-csi-leader-election")]
    assert any(
        rule["apiGroups"] == ["coordination.k8s.io"]
        and rule["resources"] == ["leases"]
        and {"create", "get", "list", "update", "watch"}.issubset(rule["verbs"])
        for rule in leader_role["rules"]
    )

    resizer = roles[("ClusterRole", "kadalu-csi-external-resizer")]
    assert any(
        rule["apiGroups"] == ["storage.k8s.io"]
        and rule["resources"] == ["volumeattributesclasses"]
        and rule["verbs"] == ["get", "list", "watch"]
        for rule in resizer["rules"]
    )

    provisioner = roles[("ClusterRole", "kadalu-csi-external-provisioner")]
    assert all(
        "volumeattachments" not in rule.get("resources", [])
        for rule in provisioner["rules"]
    )

    legacy_attacher = roles[("ClusterRole", "kadalu-csi-external-attacher")]
    assert legacy_attacher["rules"] == []


def test_only_stable_csidriver_api_is_packaged():
    templates = ROOT / "templates"
    assert not (templates / "csi-driver-object.yaml.j2").exists()

    document = yaml.safe_load(
        (templates / "csi-driver-object-v1.yaml.j2").read_text(encoding="utf-8")
    )
    assert document["apiVersion"] == "storage.k8s.io/v1"
    assert document["kind"] == "CSIDriver"
    assert document["spec"]["attachRequired"] is False
    assert document["spec"]["podInfoOnMount"] is False
    assert document["spec"]["volumeLifecycleModes"] == ["Persistent"]
    assert "fsGroupPolicy" not in document["spec"]
