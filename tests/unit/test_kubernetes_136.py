"""Kubernetes 1.36 manifest compatibility tests."""

import subprocess
from pathlib import Path

import pytest
import yaml
from jinja2 import Template


ROOT = Path(__file__).resolve().parents[2]
BUSYBOX_IMAGE = (
    "docker.io/library/busybox:1.37.0@sha256:"
    "9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0"
)
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


def _render_csi_documents(
        provisioner_replicas=None, nodeplugin_tolerations=None):
    template = Template(
        (ROOT / "templates/csi.yaml.j2").read_text(encoding="utf-8")
    )
    values = {
        "namespace": "the-vault",
        "images_hub": "registry.example.invalid",
        "docker_user": "ocean-crew",
        "kadalu_version": "test",
        "k8s_dist": "kubernetes",
        "kubelet_dir": "/var/lib/kubelet",
        "verbose": "no",
        "csi_sidecar_registry": "registry.k8s.io",
        "busybox_image": BUSYBOX_IMAGE,
        "nodeplugin_tolerations": (
            []
            if nodeplugin_tolerations is None
            else nodeplugin_tolerations
        ),
    }
    if provisioner_replicas is not None:
        values["provisioner_replicas"] = provisioner_replicas
    rendered = template.render(**values)
    return [document for document in yaml.safe_load_all(rendered) if document]


def _render_server(tolerations):
    template = Template(
        (ROOT / "templates" / "server.yaml.j2").read_text(encoding="utf-8")
    )
    rendered = template.render(
        namespace="the-vault",
        serverpod_name="server-bellagio-vault-0",
        volname="bellagio-vault",
        voltype="Replica1",
        images_hub="registry.example.invalid",
        docker_user="ocean-crew",
        kadalu_version="test",
        k8s_dist="kubernetes",
        verbose="no",
        tolerations=tolerations,
        kube_hostname="linus-caldwell",
        shd_required=False,
        brick_path="/bricks/bellagio-vault/data/brick",
        brick_node_id="linus-caldwell",
        volume_id="00000000-0000-4000-8000-000000000001",
        brick_index=0,
        brick_device="",
        pvc_name="",
        host_brick_path="/tmp/kadalu-ci/brick0",
        brick_device_dir="",
    )
    return yaml.safe_load(rendered)


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


def test_storage_crd_declares_custom_storage_class_name():
    crd = yaml.safe_load((
        ROOT
        / "helm/kadalu/charts/operator/crds/kadalu_storage.yaml"
    ).read_text(encoding="utf-8"))
    storage_class = (
        crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
        ["properties"]["spec"]["properties"]["storageClassName"]
    )

    assert storage_class["type"] == "string"
    assert storage_class["minLength"] == 1
    assert storage_class["maxLength"] == 253
    assert "pattern" in storage_class
    assert "(?" not in storage_class["pattern"]
    assert "immutable" in storage_class["description"].lower()


def test_server_tolerations_preserve_supported_fields_as_typed_yaml():
    tolerations = [
        {"operator": "Exists"},
        {
            "key": "yes",
            "operator": "Equal",
            "value": "null",
            "effect": "NoExecute",
            "tolerationSeconds": 0,
        },
        {
            "key": "mirage-any-effect",
            "operator": "Exists",
            "effect": "",
        },
        {"key": "bellagio-default-operator", "value": "crew"},
    ]

    server = _render_server(tolerations)

    assert server["spec"]["template"]["spec"]["tolerations"] == [
        {"operator": "Exists"},
        {
            "key": "yes",
            "operator": "Equal",
            "value": "null",
            "effect": "NoExecute",
            "tolerationSeconds": 0,
        },
        {
            "key": "mirage-any-effect",
            "operator": "Exists",
            "effect": "",
        },
        {"key": "bellagio-default-operator", "value": "crew"},
    ]


@pytest.mark.parametrize(
    "provisioner_replicas",
    [0, 1],
    ids=["fenced", "serving"],
)
def test_csi_nodeplugin_tolerations_render_losslessly(
        provisioner_replicas):
    tolerations = [
        {"operator": "Exists"},
        {
            "key": "yes",
            "operator": "Equal",
            "value": "null",
            "effect": "NoExecute",
            "tolerationSeconds": 0,
        },
        {
            "key": "mirage-any-effect",
            "operator": "Exists",
            "effect": "",
        },
        {"key": "bellagio-default-operator", "value": "crew"},
    ]

    nodeplugin = next(
        document
        for document in _render_csi_documents(
            provisioner_replicas=provisioner_replicas,
            nodeplugin_tolerations=tolerations,
        )
        if document["kind"] == "DaemonSet"
        and document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )

    assert nodeplugin["spec"]["template"]["spec"]["tolerations"] == [
        {"operator": "Exists"},
        {
            "key": "yes",
            "operator": "Equal",
            "value": "null",
            "effect": "NoExecute",
            "tolerationSeconds": 0,
        },
        {
            "key": "mirage-any-effect",
            "operator": "Exists",
            "effect": "",
        },
        {"key": "bellagio-default-operator", "value": "crew"},
    ]


def test_csi_nodeplugin_renders_explicit_empty_toleration_list():
    nodeplugin = next(
        document
        for document in _render_csi_documents(nodeplugin_tolerations=[])
        if document["kind"] == "DaemonSet"
        and document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )

    assert nodeplugin["spec"]["template"]["spec"]["tolerations"] == []


def test_operator_can_patch_persistent_volume_reclaim_policy():
    operator_role = next(
        document
        for document in _helm_documents()
        if document.get("kind") == "ClusterRole"
        and document.get("metadata", {}).get("name") == "kadalu-operator"
    )
    persistent_volume_rule = next(
        rule
        for rule in operator_role["rules"]
        if rule.get("apiGroups") == [""]
        and "persistentvolumes" in rule.get("resources", [])
    )

    assert "patch" in persistent_volume_rule["verbs"]


def test_operator_can_list_server_statefulsets_for_identity_recovery():
    operator_role = next(
        document
        for document in _helm_documents()
        if document.get("kind") == "Role"
        and document.get("metadata", {}).get("name") == "kadalu-operator"
    )
    statefulset_rule = next(
        rule
        for rule in operator_role["rules"]
        if rule.get("apiGroups") == ["apps"]
        and "statefulsets" in rule.get("resources", [])
    )

    assert "list" in statefulset_rule["verbs"]


def test_operator_upgrade_avoids_normal_rolling_overlap():
    operator = next(
        document
        for document in _helm_documents()
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "operator"
    )

    assert operator["spec"]["replicas"] == 1
    assert operator["spec"]["strategy"] == {
        "type": "RollingUpdate",
        "rollingUpdate": {
            "maxSurge": 0,
            "maxUnavailable": 1,
        },
    }


def test_operator_readiness_tracks_the_reconciler_process():
    operator = next(
        document
        for document in _helm_documents()
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "operator"
    )
    container = next(
        item
        for item in operator["spec"]["template"]["spec"]["containers"]
        if item["name"] == "kadalu-operator"
    )

    assert container["readinessProbe"] == {
        "exec": {
            "command": [
                "/bin/sh",
                "-c",
                "test -f /tmp/operator-ready",
            ],
        },
        "timeoutSeconds": 3,
        "periodSeconds": 5,
        "failureThreshold": 1,
    }
    assert "livenessProbe" not in container


def test_fork_images_default_to_ghcr_namespace():
    values = yaml.safe_load(
        (ROOT / "helm/kadalu/values.yaml").read_text(encoding="utf-8")
    )

    assert values["global"]["image"] == {
        "registry": "ghcr.io",
        "repository": "joejulian",
        "pullPolicy": "IfNotPresent",
    }
    assert values["global"]["busyboxImage"] == BUSYBOX_IMAGE

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
        assert 'variable "mount_identity"' in nomad_job
        assert 'mount_config_fingerprint = sha256(jsonencode({' in nomad_job
        assert '"mount_identity": "${local.effective_mount_identity}"' \
            in nomad_job
        assert (
            '"mount_config_fingerprint": '
            '"${local.mount_config_fingerprint}"'
        ) in nomad_job


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


def test_controller_readiness_requires_storage_and_live_csi_socket():
    provisioner = next(
        document
        for document in _render_csi_documents()
        if document["metadata"]["name"] == "kadalu-csi-provisioner"
    )
    containers = {
        container["name"]: container
        for container in provisioner["spec"]["template"]["spec"]["containers"]
    }
    driver = containers["kadalu-provisioner"]
    health_probe = containers["csi-liveness-probe"]

    assert driver["readinessProbe"]["exec"]["command"] == [
        "/bin/sh",
        "-c",
        "test -f /plugin/storage-ready",
    ]
    assert health_probe["readinessProbe"]["httpGet"] == {
        "path": "/healthz",
        "port": 9809,
    }
    assert driver["livenessProbe"]["httpGet"] == {
        "path": "/healthz",
        "port": "driver-health",
    }
    assert "startupProbe" not in driver


@pytest.mark.parametrize(
    ("requested", "expected"),
    [(None, 1), (0, 0), (1, 1), (2, 1)],
)
def test_controller_replica_fence_is_renderable(requested, expected):
    provisioner = next(
        document
        for document in _render_csi_documents(requested)
        if document["metadata"]["name"] == "kadalu-csi-provisioner"
    )

    assert provisioner["spec"]["replicas"] == expected


def test_nodeplugin_upgrade_requires_explicit_node_by_node_deletion():
    nodeplugin = next(
        document
        for document in _render_csi_documents()
        if document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )

    assert nodeplugin["spec"]["updateStrategy"] == {"type": "OnDelete"}


def test_nodeplugin_startup_does_not_rewrite_existing_pvc_mounts():
    """A restart must leave kubelet's existing per-PVC mounts untouched."""
    nodeplugin = next(
        document
        for document in _render_csi_documents()
        if document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )
    container = next(
        item
        for item in nodeplugin["spec"]["template"]["spec"]["containers"]
        if item["name"] == "kadalu-nodeplugin"
    )

    assert "lifecycle" not in container


def test_nodeplugin_receives_the_same_kubelet_root_it_mounts():
    nodeplugin = next(
        document
        for document in _render_csi_documents()
        if document["metadata"]["name"] == "kadalu-csi-nodeplugin"
    )
    container = next(
        item
        for item in nodeplugin["spec"]["template"]["spec"]["containers"]
        if item["name"] == "kadalu-nodeplugin"
    )
    environment = {
        item["name"]: item["value"]
        for item in container["env"]
        if "value" in item
    }

    assert environment["KUBELET_DIR"] == "/var/lib/kubelet"


@pytest.mark.parametrize(
    ("template_name", "values"),
    [
        (
            "storageclass-kadalu.custom.yaml.j2",
            {
                "hostvol_name": "bellagio-vault",
                "storage_class_name": "default",
                "namespace": "kadalu",
                "storage_uid": "uid-bellagio-vault",
                "volume_id": "00000000-0000-4000-8000-000000000001",
                "mount_identity": "00000000-0000-4000-8000-000000000002",
                "backend_fingerprint": "1" * 64,
                "single_pv_per_pool": False,
            },
        ),
        (
            "external-storageclass.yaml.j2",
            {
                "volname": "mirage-vault",
                "storage_class_name": "mirage-vault-class",
                "namespace": "kadalu",
                "storage_uid": "uid-mirage-vault",
                "volume_id": "00000000-0000-4000-8000-000000000003",
                "mount_identity": "00000000-0000-4000-8000-000000000004",
                "backend_fingerprint": "2" * 64,
                "gluster_hosts": "gluster.example.invalid",
                "gluster_volname": "mirage",
                "gluster_options": "",
                "single_pv_per_pool": False,
            },
        ),
    ],
)
@pytest.mark.parametrize("reclaim_policy", ["Delete", "Retain"])
def test_storage_class_templates_render_explicit_reclaim_policy(
        template_name, values, reclaim_policy):
    template = Template(
        (ROOT / "templates" / template_name).read_text(encoding="utf-8")
    )

    storage_class = yaml.safe_load(template.render(
        **values,
        reclaim_policy=reclaim_policy,
    ))

    assert storage_class["metadata"]["name"] == values["storage_class_name"]
    assert storage_class["reclaimPolicy"] == reclaim_policy
    assert storage_class["metadata"]["annotations"] == {
        "kadalu.io/storage-namespace": "kadalu",
        "kadalu.io/storage-name": (
            values.get("hostvol_name") or values["volname"]
        ),
        "kadalu.io/storage-uid": values["storage_uid"],
        "kadalu.io/volume-id": values["volume_id"],
        "kadalu.io/mount-identity": values["mount_identity"],
        "kadalu.io/backend-fingerprint": values["backend_fingerprint"],
    }
    assert (
        "storageclass.kubernetes.io/is-default-class"
        not in storage_class["metadata"]["annotations"]
    )
    assert (
        "storageclass.beta.kubernetes.io/is-default-class"
        not in storage_class["metadata"]["annotations"]
    )


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
    assert containers["kadalu-logging"]["image"] == BUSYBOX_IMAGE


def test_operator_passes_independent_busybox_image_to_runtime_renderer():
    operator = next(
        document
        for document in _helm_documents()
        if document.get("kind") == "Deployment"
        and document["metadata"]["name"] == "operator"
    )
    container = operator["spec"]["template"]["spec"]["containers"][0]
    environment = {
        variable["name"]: variable.get("value")
        for variable in container["env"]
    }

    assert environment["BUSYBOX_IMAGE"] == BUSYBOX_IMAGE


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
