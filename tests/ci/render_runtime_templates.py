#!/usr/bin/env python3
"""Render representative operator runtime manifests for compatibility tests."""

import argparse
from pathlib import Path

from jinja2 import Environment, StrictUndefined


ROOT = Path(__file__).resolve().parents[2]
NAMESPACE = "ocean-crew"


def render(template_name, **values):
    """Render one runtime template and fail on any missing variable."""
    source = (ROOT / "templates" / template_name).read_text(encoding="utf-8")
    environment = Environment(
        autoescape=False,
        keep_trailing_newline=True,
        undefined=StrictUndefined,
    )
    return environment.from_string(source).render(**values).strip()


def runtime_manifests():
    """Return sanitized manifests exercising every operator template family."""
    return [
        render("namespace.yaml.j2", namespace=NAMESPACE),
        render(
            "configmap.yaml.j2",
            namespace=NAMESPACE,
            uid="00000000-0000-4000-8000-000000000000",
        ),
        render(
            "services.yaml.j2",
            namespace=NAMESPACE,
            volname="bellagio-vault",
        ),
        render(
            "server.yaml.j2",
            namespace=NAMESPACE,
            serverpod_name="server-bellagio-vault-0",
            volname="bellagio-vault",
            voltype="Replica1",
            images_hub="registry.example.invalid",
            docker_user="heist-crew",
            kadalu_version="test",
            k8s_dist="kubernetes",
            verbose="no",
            tolerations=[
                {
                    "key": "crew",
                    "operator": "Equal",
                    "value": "ocean",
                    "effect": "NoExecute",
                    "tolerationSeconds": 0,
                },
                {
                    "operator": "Exists",
                },
            ],
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
        ),
        render(
            "storageclass-kadalu.custom.yaml.j2",
            namespace=NAMESPACE,
            hostvol_name="bellagio-vault",
            storage_class_name="kadalu.bellagio-vault",
            storage_uid="uid-bellagio-vault",
            volume_id="00000000-0000-4000-8000-000000000001",
            mount_identity="00000000-0000-4000-8000-000000000002",
            backend_fingerprint="1" * 64,
            single_pv_per_pool=False,
            reclaim_policy="Delete",
        ),
        render(
            "external-storageclass.yaml.j2",
            namespace=NAMESPACE,
            volname="mirage-vault",
            storage_class_name="kadalu.mirage-vault",
            storage_uid="uid-mirage-vault",
            volume_id="00000000-0000-4000-8000-000000000003",
            mount_identity="00000000-0000-4000-8000-000000000004",
            backend_fingerprint="2" * 64,
            gluster_hosts="gluster.example.invalid",
            gluster_volname="mirage",
            gluster_options="log-level=WARNING",
            single_pv_per_pool=False,
            reclaim_policy="Delete",
        ),
        render("csi-driver-object-v1.yaml.j2"),
        render(
            "csi.yaml.j2",
            namespace=NAMESPACE,
            images_hub="registry.example.invalid",
            docker_user="heist-crew",
            kadalu_version="test",
            k8s_dist="kubernetes",
            kubelet_dir="/var/lib/kubelet",
            verbose="no",
            csi_sidecar_registry="registry.k8s.io",
            busybox_image=(
                "docker.io/library/busybox:1.37.0@sha256:"
                "9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0"
            ),
            nodeplugin_tolerations=[
                {
                    "key": "crew",
                    "operator": "Equal",
                    "value": "ocean",
                    "effect": "NoExecute",
                    "tolerationSeconds": 0,
                },
                {
                    "operator": "Exists",
                },
            ],
        ),
    ]


def main():
    """Write the representative runtime manifest bundle."""
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    document = "\n---\n".join(runtime_manifests()) + "\n"
    args.output.write_text(document, encoding="utf-8")


if __name__ == "__main__":
    main()
