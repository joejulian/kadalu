"""Operator tests for durable ownership of whole-pool CSI volumes."""

import importlib
import json
import sys
import types
import uuid
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
POOL_NAME = "bellagio-pool"
VOLUME_ID = "pvc-danny-ocean"
OTHER_VOLUME_ID = "pvc-rusty-ryan"
MOUNT_IDENTITY = "11111111-2222-4333-8444-555555555555"
HOSTING_VOLUME_ID = "22222222-3333-4444-8555-666666666666"
OTHER_HOSTING_VOLUME_ID = "33333333-4444-4555-8666-777777777777"


def _pool_volume_id(name):
    return str(uuid.uuid5(
        uuid.NAMESPACE_DNS,
        f"{name}.hosting.heist.invalid",
    ))


def _single_pv_per_pool(data):
    legacy_format = data.get("kadalu_format")
    if legacy_format is not None:
        return (
            isinstance(legacy_format, str)
            and legacy_format.lower() != "native"
        )

    value = data.get("single_pv_per_pool", False)
    if isinstance(value, str):
        return value.lower() == "true"
    return value


def _load_operator(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT))
    monkeypatch.syspath_prepend(str(ROOT / "cli" / "kubectl_kadalu"))
    fake_kadalulib = types.ModuleType("kadalulib")
    fake_kadalulib.CommandException = RuntimeError
    fake_kadalulib.execute = lambda *_args, **_kwargs: None
    fake_kadalulib.get_single_pv_per_pool = _single_pv_per_pool
    fake_kadalulib.is_host_reachable = lambda _host, _port: True
    fake_kadalulib.logf = lambda message, **_kwargs: message
    fake_kadalulib.logging_setup = lambda: None
    fake_kadalulib.send_analytics_tracker = lambda *_args, **_kwargs: None
    monkeypatch.setitem(sys.modules, "kadalulib", fake_kadalulib)
    fake_kubernetes = types.ModuleType("kubernetes")
    fake_kubernetes.client = SimpleNamespace(
        StorageV1Api=lambda: SimpleNamespace(
            list_storage_class=lambda: SimpleNamespace(items=[]),
        ),
        V1DeleteOptions=lambda **kwargs: SimpleNamespace(**kwargs),
        V1Preconditions=lambda **kwargs: SimpleNamespace(**kwargs),
    )
    fake_kubernetes.config = SimpleNamespace()
    fake_kubernetes.watch = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "kubernetes", fake_kubernetes)
    sys.modules.pop("kadalu_operator.main", None)
    return importlib.import_module("kadalu_operator.main")


class FakeCoreV1Client:
    """Minimal CoreV1 API surface used by claim migration and config writes."""

    def __init__(self, records, persistent_volumes=(), pods=()):
        self.config_map = SimpleNamespace(
            metadata=SimpleNamespace(resource_version="101"),
            data={
                key: json.dumps(value)
                for key, value in records.items()
            },
        )
        self.persistent_volumes = list(persistent_volumes)
        self.pods = list(pods)
        self.patches = []
        self.replacements = []
        self.pv_patches = []
        self.events = []

    def read_namespaced_config_map(self, _name, _namespace):
        return self.config_map

    def list_persistent_volume(self):
        return SimpleNamespace(items=self.persistent_volumes)

    def list_namespaced_pod(self, _namespace):
        return SimpleNamespace(items=[])

    def list_pod_for_all_namespaces(self):
        return SimpleNamespace(items=self.pods)

    def patch_namespaced_config_map(self, name, namespace, body):
        self.patches.append((name, namespace, body))
        self.events.append(("config-map", name))

    def replace_namespaced_config_map(self, name, namespace, body):
        self.config_map = body
        self.patches.append((name, namespace, body))
        self.replacements.append((name, namespace, body))
        self.events.append(("config-map", name))
        return body

    def patch_persistent_volume(self, name, body):
        self.pv_patches.append((name, body))
        self.events.append(("persistent-volume", name))


class FakeStorageV1Api:
    """StorageV1 API which records UID-preconditioned class deletion."""

    def __init__(self, items=()):
        self.items = list(items)
        self.deletes = []

    def list_storage_class(self):
        return SimpleNamespace(items=self.items)

    def delete_storage_class(self, name, body):
        self.deletes.append((name, body))
        self.items = [
            item for item in self.items
            if item.metadata.name != name
        ]


class MutationGuardClient:
    """Fail if an invalid request reaches any Kubernetes API operation."""

    def __getattr__(self, name):
        pytest.fail(f"invalid Bellagio request reached Kubernetes API {name}")


def _persistent_volume(
        *,
        volume_id=VOLUME_ID,
        driver="kadalu",
        hostvol=POOL_NAME,
        storage_class_name=None,
        single_pv_per_pool="true",
        path=None,
        pvtype=None,
        claim_namespace=None,
        claim_name=None,
        reclaim_policy="Delete", creation_timestamp=None):
    attributes = {
        "hostvol": hostvol,
        "single_pv_per_pool": single_pv_per_pool,
    }
    if path is not None:
        attributes["path"] = path
    if pvtype is not None:
        attributes["pvtype"] = pvtype
    claim_ref = None
    if claim_namespace is not None and claim_name is not None:
        claim_ref = SimpleNamespace(
            namespace=claim_namespace,
            name=claim_name,
        )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=volume_id,
            creation_timestamp=creation_timestamp,
        ),
        spec=SimpleNamespace(
            csi=SimpleNamespace(
                driver=driver,
                volume_attributes=attributes,
                volume_handle=volume_id,
            ),
            storage_class_name=storage_class_name,
            claim_ref=claim_ref,
            persistent_volume_reclaim_policy=reclaim_policy,
        ),
    )


def _workload_pod(
        claim_name, *, namespace="oceans-eleven", node_name="casino-node",
        phase="Running"):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="bellagio-heist",
            namespace=namespace,
        ),
        spec=SimpleNamespace(
            node_name=node_name,
            volumes=[SimpleNamespace(
                persistent_volume_claim=SimpleNamespace(
                    claim_name=claim_name,
                ),
            )],
        ),
        status=SimpleNamespace(phase=phase),
    )


def _server_pod(
        volume_id=HOSTING_VOLUME_ID, *, ordinal=0, brick_index=0,
        node="bellagio-storage-one", path="/srv/bellagio-vault",
        device="", pvc="", pool_type="Replica1"):
    env_values = {
        "VOLUME": POOL_NAME,
        "VOLUME_TYPE": pool_type,
        "BRICK_PATH": f"/bricks/{POOL_NAME}/data/brick",
        "NODEID": f"node-{brick_index}",
        "BRICK_INDEX": str(brick_index),
        "BRICK_DEVICE": device,
    }
    if node:
        env_values["HOSTNAME"] = node
    if volume_id is not None:
        env_values["VOLUME_ID"] = volume_id
    env = [
        SimpleNamespace(name=name, value=value)
        for name, value in env_values.items()
    ]
    mountdir = SimpleNamespace(
        name="glusterfsd-mountdir",
        host_path=None,
        persistent_volume_claim=None,
        empty_dir=None,
    )
    if pvc:
        mountdir.persistent_volume_claim = SimpleNamespace(claim_name=pvc)
    elif path:
        mountdir.host_path = SimpleNamespace(
            path=path,
            type="Directory",
        )
    else:
        mountdir.empty_dir = SimpleNamespace()
    volumes = [mountdir]
    mounts = [SimpleNamespace(
        name="glusterfsd-mountdir",
        mount_path=f"/bricks/{POOL_NAME}",
    )]
    if device and not device.startswith("/dev/"):
        volumes.append(SimpleNamespace(
            name="brick-device-dir",
            host_path=SimpleNamespace(
                path=str(Path(device).parent),
                type="Directory",
            ),
        ))
        mounts.append(SimpleNamespace(
            name="brick-device-dir",
            mount_path="/brickdev",
        ))
    affinity = None
    if node:
        affinity = SimpleNamespace(node_affinity=SimpleNamespace(
            required_during_scheduling_ignored_during_execution=(
                SimpleNamespace(node_selector_terms=[SimpleNamespace(
                    match_expressions=[SimpleNamespace(
                        key="kubernetes.io/hostname",
                        operator="In",
                        values=[node],
                    )],
                )])
            ),
        ))
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"server-{POOL_NAME}-{brick_index}-{ordinal}"
        ),
        spec=SimpleNamespace(
            affinity=affinity,
            containers=[SimpleNamespace(
                name="server",
                env=env,
                volume_mounts=mounts,
            )],
            volumes=volumes,
        ),
    )


_API_FIELD_NAMES = {
    "empty_dir": "emptyDir",
    "host_path": "hostPath",
    "match_expressions": "matchExpressions",
    "mount_path": "mountPath",
    "node_affinity": "nodeAffinity",
    "node_selector_terms": "nodeSelectorTerms",
    "persistent_volume_claim": "persistentVolumeClaim",
    "required_during_scheduling_ignored_during_execution": (
        "requiredDuringSchedulingIgnoredDuringExecution"
    ),
    "volume_mounts": "volumeMounts",
}


def _as_api_dict(value):
    if isinstance(value, SimpleNamespace):
        return {
            _API_FIELD_NAMES.get(key, key): _as_api_dict(item)
            for key, item in vars(value).items()
        }
    if isinstance(value, list):
        return [_as_api_dict(item) for item in value]
    return value


def _server_statefulset_evidence(**pod_kwargs):
    pod = _server_pod(**pod_kwargs)
    brick_index = pod_kwargs.get("brick_index", 0)
    return {
        "metadata": {
            "name": f"server-{POOL_NAME}-{brick_index}",
        },
        "spec": {
            "serviceName": POOL_NAME,
            "replicas": 1,
            "template": {"spec": _as_api_dict(pod.spec)},
        },
    }


def _nodeplugin_daemonset(
        *, current=True, tolerations=None, generation=4,
        observed_generation=4):
    desired = 2
    return SimpleNamespace(
        metadata=SimpleNamespace(generation=generation),
        spec=SimpleNamespace(
            template=SimpleNamespace(
                spec=SimpleNamespace(tolerations=tolerations),
            ),
        ),
        status=SimpleNamespace(
            desired_number_scheduled=desired,
            updated_number_scheduled=desired if current else 1,
            number_ready=desired,
            observed_generation=observed_generation,
        ),
    )


def _server_statefulset(**overrides):
    values = {
        "generation": 4,
        "observed_generation": 4,
        "replicas": 1,
        "current_replicas": 1,
        "updated_replicas": 1,
        "ready_replicas": 1,
        "available_replicas": 1,
        "current_revision": "bellagio-current",
        "update_revision": "bellagio-current",
        "desired_replicas": 1,
        "tolerations": None,
        "creation_timestamp": None,
    }
    values.update(overrides)
    return SimpleNamespace(
        metadata=SimpleNamespace(
            generation=values["generation"],
            creation_timestamp=values["creation_timestamp"],
        ),
        spec=SimpleNamespace(
            replicas=values["desired_replicas"],
            template=SimpleNamespace(spec=SimpleNamespace(
                tolerations=values["tolerations"],
            )),
        ),
        status=SimpleNamespace(
            observed_generation=values["observed_generation"],
            replicas=values["replicas"],
            current_replicas=values["current_replicas"],
            updated_replicas=values["updated_replicas"],
            ready_replicas=values["ready_replicas"],
            available_replicas=values["available_replicas"],
            current_revision=values["current_revision"],
            update_revision=values["update_revision"],
        ),
    )


def _legacy_pool_record():
    return {
        "volname": POOL_NAME,
        "type": "Replica1",
        "single_pv_per_pool": True,
        "bricks": [],
    }


def _native_pool_object(
        *, single_pv_per_pool=False, policy="delete",
        storage_class_name=None):
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "Replica1",
            "volume_id": HOSTING_VOLUME_ID,
            "single_pv_per_pool": single_pv_per_pool,
            "pvReclaimPolicy": policy,
            "storage": [{
                "node_id": "node-eleven-ocean",
                "node": "bellagio-storage-one",
                "path": "/srv/bellagio-vault",
            }],
        },
    }
    if storage_class_name is not None:
        obj["spec"]["storageClassName"] = storage_class_name
    return obj


def _external_pool_object(
        *, include_volume_id=True, storage_class_name=None):
    obj = {
        "metadata": {
            "name": POOL_NAME,
            "uid": "uid-bellagio-external",
        },
        "spec": {
            "type": "External",
            "single_pv_per_pool": False,
            "pvReclaimPolicy": "delete",
            "details": {
                "gluster_hosts": [
                    "bellagio-one.example.invalid",
                    "bellagio-two.example.invalid",
                ],
                "gluster_volname": "the-benedict-vault",
                "gluster_options": "log-level=WARNING",
            },
        },
    }
    if include_volume_id:
        obj["spec"]["volume_id"] = HOSTING_VOLUME_ID
    if storage_class_name is not None:
        obj["spec"]["storageClassName"] = storage_class_name
    return obj


def _owned_storage_class(
        operator, obj, *, durable=False,
        volume_id=HOSTING_VOLUME_ID, mount_identity=MOUNT_IDENTITY,
        annotation_overrides=None):
    identity = operator.storage_class_identity(obj)
    annotations = {
        operator.STORAGE_CLASS_NAMESPACE_ANNOTATION: identity["namespace"],
        operator.STORAGE_CLASS_NAME_ANNOTATION: identity["name"],
        operator.STORAGE_CLASS_UID_ANNOTATION: identity["uid"],
    }
    if durable:
        annotations.update({
            operator.STORAGE_CLASS_VOLUME_ID_ANNOTATION: volume_id,
            operator.STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION: mount_identity,
            operator.STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION: (
                identity["backend_fingerprint"]
            ),
        })
    annotations.update(annotation_overrides or {})
    name = obj["spec"].get(
        "storageClassName",
        f"kadalu.{obj['metadata']['name']}",
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=f"uid-storage-class-{name}",
            resource_version="117",
            annotations=annotations,
        ),
        provisioner="kadalu",
        parameters=identity["parameters"],
        reclaim_policy="Delete",
        allow_volume_expansion=(
            not _single_pv_per_pool(obj["spec"])
        ),
    )


def _legacy_storage_class(operator, record):
    """Return the exact unannotated class emitted before owner metadata."""
    identity = operator.storage_class_identity(record)
    name = identity["storage_class_name"]
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=f"uid-storage-class-{name}",
            resource_version="117",
            annotations={},
        ),
        provisioner="kadalu",
        parameters=identity["parameters"],
        reclaim_policy="Delete",
        allow_volume_expansion=True,
    )


def _storage_custom_object(
        *, name=POOL_NAME, pool_type="Replica1", tolerations=None,
        deleting=False):
    metadata = {"name": name}
    if deleting:
        metadata["deletionTimestamp"] = "2026-08-15T04:11:00Z"
    spec = {"type": pool_type}
    if pool_type == "External":
        spec["details"] = {
            "gluster_hosts": ["casino.example.invalid"],
            "gluster_volname": f"{name}-vault",
        }
    else:
        brick_count = {
            "Replica1": 1,
            "Replica2": 2,
            "Replica3": 3,
            "Disperse": 3,
            "Arbiter": 3,
        }.get(pool_type, 1)
        spec["storage"] = [
            {
                "node": f"{name}-storage-{index}",
                "path": f"/srv/{name}-{index}",
            }
            for index in range(brick_count)
        ]
        if pool_type == "Disperse":
            spec["disperse"] = {"data": 2, "redundancy": 1}
    if tolerations is not None:
        spec["tolerations"] = tolerations
    return {"metadata": metadata, "spec": spec}


def _stored_native_pool(
        name=POOL_NAME, *, pool_type="Replica1", bricks=1,
        tolerations=None, storage_class_name=None):
    record = {
        "volname": name,
        "type": pool_type,
        "pvReclaimPolicy": "delete",
        "volume_id": _pool_volume_id(name),
        "bricks": [
            {
                "brick_index": index,
                "node_id": f"node-{index}",
                "brick_path": f"/bricks/{name}/data/brick",
                "node": f"server-{name}-{index}-0.{name}",
                "host_brick_path": f"/srv/{name}-{index}",
                "kube_hostname": f"{name}-storage-{index}",
                "brick_device": "",
                "brick_device_dir": "",
                "pvc_name": "",
            }
            for index in range(bricks)
        ],
    }
    if tolerations is not None:
        record["tolerations"] = tolerations
    if storage_class_name is not None:
        record["storageClassName"] = storage_class_name
    return record


def _saved_record(client, pool_name=POOL_NAME):
    return json.loads(client.config_map.data[f"{pool_name}.info"])


def _deletion_object(record):
    uid = record.setdefault(
        "storage_uid",
        f"uid-{record['volname']}-deletion",
    )
    obj = {
        "metadata": {
            "name": record["volname"],
            "uid": uid,
        },
        "spec": {"type": record["type"]},
    }
    if "storageClassName" in record:
        obj["spec"]["storageClassName"] = record["storageClassName"]
    return obj


def _storage_plan(
        *, items=None, tolerations=None, upgrade_objects=None,
        deletion_orphans=None, invalid_items=None, resource_version="117"):
    planned_items = [] if items is None else items
    return {
        "items": planned_items,
        "reconcile_items": planned_items,
        "invalid_items": [] if invalid_items is None else invalid_items,
        "resource_version": resource_version,
        "nodeplugin_tolerations": (
            [] if tolerations is None else tolerations
        ),
        "upgrade_objects": (
            [] if upgrade_objects is None else upgrade_objects
        ),
        "deletion_orphans": (
            [] if deletion_orphans is None else deletion_orphans
        ),
    }


def _assert_mount_metadata(operator, record):
    assert str(uuid.UUID(record["mount_identity"])) == record["mount_identity"]
    assert record["mount_config_fingerprint"] == (
        operator.mount_config_fingerprint(record)
    )


def test_migration_records_unique_existing_whole_pool_owner(monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": _legacy_pool_record()},
        [_persistent_volume()],
    )

    operator.migrate_legacy_single_pv_claims(client)

    record = _saved_record(client)
    assert record["legacy_single_pv_volume_id"] == VOLUME_ID
    assert record["legacy_mount_config_fingerprint"] == (
        operator.mount_config_fingerprint(record)
    )
    uuid.UUID(record["mount_identity"])
    assert len(record["mount_config_fingerprint"]) == 64
    assert client.patches == [(
        operator.KADALU_CONFIG_MAP,
        operator.NAMESPACE,
        client.config_map,
    )]


@pytest.mark.parametrize(
    "persistent_volumes",
    [
        [],
        [
            _persistent_volume(volume_id=VOLUME_ID),
            _persistent_volume(volume_id=OTHER_VOLUME_ID),
        ],
    ],
    ids=["no-owner", "ambiguous-owner"],
)
def test_migration_leaves_unowned_or_ambiguous_pool_unclaimed(
        monkeypatch, persistent_volumes):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": _legacy_pool_record()},
        persistent_volumes,
    )

    operator.migrate_legacy_single_pv_claims(client)

    record = _saved_record(client)
    assert "legacy_single_pv_volume_id" not in record
    assert record["legacy_mount_config_fingerprint"] == (
        operator.mount_config_fingerprint(record)
    )
    uuid.UUID(record["mount_identity"])
    assert len(record["mount_config_fingerprint"]) == 64
    assert len(client.patches) == 1


def test_migration_ignores_non_kadalu_non_single_and_subvolume_pvs(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": _legacy_pool_record()},
        [
            _persistent_volume(
                volume_id="pvc-terry-benedict",
                driver="the-night-fox.example.invalid",
            ),
            _persistent_volume(
                volume_id="pvc-linus-caldwell",
                single_pv_per_pool="false",
            ),
            _persistent_volume(
                volume_id="pvc-basher-tarr",
                path="subvol/pvc-basher-tarr",
            ),
        ],
    )

    operator.migrate_legacy_single_pv_claims(client)

    record = _saved_record(client)
    assert "legacy_single_pv_volume_id" not in record
    assert record["legacy_mount_config_fingerprint"] == (
        operator.mount_config_fingerprint(record)
    )
    uuid.UUID(record["mount_identity"])
    assert len(record["mount_config_fingerprint"]) == 64
    assert len(client.patches) == 1


@pytest.mark.parametrize("pvtype", ["virtblock", "rawblock"])
def test_migration_blocks_any_legacy_block_pv_without_recovery_state(
        monkeypatch, pvtype):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": _legacy_pool_record()},
        [_persistent_volume(
            pvtype=pvtype,
            claim_namespace="oceans-eleven",
            claim_name="bellagio-vault-claim",
        )],
    )

    with pytest.raises(
            RuntimeError,
            match="stopping workloads alone is not sufficient"):
        operator.migrate_legacy_single_pv_claims(client)

    record = _saved_record(client)
    assert "mount_identity" not in record
    assert "legacy_mount_config_fingerprint" not in record
    assert not client.patches


def test_terminal_workload_does_not_authorize_legacy_block_migration(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    claim_name = "the-italian-job-vault"
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": _legacy_pool_record()},
        [_persistent_volume(
            pvtype="virtblock",
            claim_namespace="oceans-eleven",
            claim_name=claim_name,
        )],
        [_workload_pod(claim_name, phase="Succeeded")],
    )

    with pytest.raises(RuntimeError, match="PersistentVolumes still exist"):
        operator.migrate_legacy_single_pv_claims(client)

    record = _saved_record(client)
    assert "mount_identity" not in record
    assert not client.patches


def test_fresh_pool_never_accepts_legacy_mount_authorization(monkeypatch):
    operator = _load_operator(monkeypatch)
    data = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "Replica1",
        "single_pv_per_pool": False,
        "bricks": [],
        "legacy_mount_config_fingerprint": "a" * 64,
    }

    assert operator.apply_pool_mount_metadata(data, None) is True

    assert "legacy_mount_config_fingerprint" not in data
    _assert_mount_metadata(operator, data)


@pytest.mark.parametrize(
    "record",
    [
        {
            **_legacy_pool_record(),
            "legacy_single_pv_volume_id": VOLUME_ID,
        },
        {
            **_legacy_pool_record(),
            "single_pv_claim_version": 1,
        },
    ],
    ids=["owner-already-recorded", "current-claim-format"],
)
def test_migration_does_not_patch_unchanged_configmap(monkeypatch, record):
    operator = _load_operator(monkeypatch)
    record["mount_identity"] = MOUNT_IDENTITY
    record["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(record)
    )
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [_persistent_volume()],
    )

    operator.migrate_legacy_single_pv_claims(client)

    assert _saved_record(client) == record
    assert not client.patches


def test_native_whole_pool_config_records_claim_format_version(monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    storage = {
        "node_id": "node-eleven-ocean",
        "node": "bellagio-storage-one",
        "path": "/srv/bellagio-vault",
    }
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "Replica1",
            "volume_id": "bellagio-hosting-volume",
            "single_pv_per_pool": True,
            "storage": [storage],
        },
    }

    operator.update_config_map(client, obj)

    record = _saved_record(client)
    assert record["single_pv_per_pool"] is True
    assert record["single_pv_claim_version"] == 1
    _assert_mount_metadata(operator, record)


def test_native_pool_config_persists_server_tolerations(monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    tolerations = [{
        "key": "casino-security",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    obj = _native_pool_object()
    obj["spec"]["tolerations"] = tolerations

    assert operator.update_config_map(client, obj) is True

    assert _saved_record(client)["tolerations"] == tolerations


@pytest.mark.parametrize(
    ("storage_class_name", "expected_name"),
    [
        (None, f"kadalu.{POOL_NAME}"),
        ("default", "default"),
    ],
    ids=["legacy-fallback", "explicit-default"],
)
def test_native_storage_class_name_is_persisted_and_rendered(
        monkeypatch, storage_class_name, expected_name):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(
        policy="retain",
        storage_class_name=storage_class_name,
    )
    obj["metadata"]["uid"] = "uid-bellagio-native-class"
    client = FakeCoreV1Client({})
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[]),
    )
    rendered = []
    reconciled = []
    monkeypatch.setattr(
        operator.os,
        "listdir",
        lambda _path: ["storageclass-kadalu.custom.yaml.j2"],
    )
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **values: rendered.append((filename, values)),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_storage_class_manifest",
        lambda _api, filename, name, policy, identity: reconciled.append(
            (filename, name, policy, identity)
        ),
    )

    assert operator.update_config_map(client, obj) is True
    operator.deploy_storage_class(obj, client, storage_api)

    record = _saved_record(client)
    assert record["storageClassName"] == expected_name
    assert len(rendered) == 1
    assert rendered[0][1]["storage_class_name"] == expected_name
    assert len(reconciled) == 1
    assert reconciled[0][1:3] == (expected_name, "Retain")
    assert reconciled[0][3]["storage_class_name"] == expected_name


def test_missing_whole_pool_record_recovers_unique_existing_owner(monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({}, [_persistent_volume()])

    assert operator.update_config_map(
        client,
        _native_pool_object(single_pv_per_pool=True),
    ) is True

    record = _saved_record(client)
    assert record["legacy_single_pv_volume_id"] == VOLUME_ID
    assert "single_pv_claim_version" not in record


def test_missing_whole_pool_record_fails_closed_for_ambiguous_owners(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({}, [
        _persistent_volume(volume_id=VOLUME_ID),
        _persistent_volume(volume_id=OTHER_VOLUME_ID),
    ])

    assert operator.update_config_map(
        client,
        _native_pool_object(single_pv_per_pool=True),
    ) is False

    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_external_whole_pool_config_records_claim_format_version(monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    tolerations = [{
        "key": "casino-security",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    monkeypatch.setattr(operator, "add_tolerations", lambda *_args: None)
    monkeypatch.setattr(
        operator,
        "reconcile_storage_class_manifest",
        lambda *_args: None,
    )
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "External",
            "volume_id": "bellagio-external-volume",
            "single_pv_per_pool": True,
            "tolerations": tolerations,
            "details": {
                "gluster_host": "bellagio.example.invalid",
                "gluster_volname": "the-benedict-vault",
            },
        },
    }

    operator.handle_external_storage_addition(
        client,
        obj,
        SimpleNamespace(
            list_storage_class=lambda: SimpleNamespace(items=[]),
        ),
    )

    record = _saved_record(client)
    assert record["single_pv_per_pool"] is True
    assert record["single_pv_claim_version"] == 1
    assert record["tolerations"] == tolerations
    _assert_mount_metadata(operator, record)


def test_external_custom_storage_class_name_is_persisted_and_rendered(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object(storage_class_name="default")
    obj["spec"]["pvReclaimPolicy"] = "retain"
    client = FakeCoreV1Client({})
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[]),
    )
    rendered = []
    reconciled = []
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **values: rendered.append((filename, values)),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_storage_class_manifest",
        lambda _api, filename, name, policy, identity: reconciled.append(
            (filename, name, policy, identity)
        ),
    )

    assert operator.handle_external_storage_addition(
        client,
        obj,
        storage_api,
    ) is True

    record = _saved_record(client)
    assert record["storageClassName"] == "default"
    assert len(rendered) == 1
    assert rendered[0][1]["storage_class_name"] == "default"
    assert len(reconciled) == 1
    assert reconciled[0][1:3] == ("default", "Retain")
    assert reconciled[0][3]["storage_class_name"] == "default"


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_reconciliation_preserves_legacy_owner_without_promoting_format(
        monkeypatch, pool_type):
    operator = _load_operator(monkeypatch)
    existing = {
        **_legacy_pool_record(),
        "type": pool_type,
        "legacy_single_pv_volume_id": VOLUME_ID,
    }
    client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    if pool_type == "External":
        monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
        monkeypatch.setattr(operator, "add_tolerations", lambda *_args: None)
        monkeypatch.setattr(
            operator,
            "reconcile_storage_class_manifest",
            lambda *_args: None,
        )
        obj = {
            "metadata": {"name": POOL_NAME},
            "spec": {
                "type": "External",
                "volume_id": "bellagio-external-volume",
                "single_pv_per_pool": True,
                "details": {
                    "gluster_host": "bellagio.example.invalid",
                    "gluster_volname": "the-benedict-vault",
                },
            },
        }
        operator.handle_external_storage_addition(
            client,
            obj,
            SimpleNamespace(
                list_storage_class=lambda: SimpleNamespace(items=[]),
            ),
        )
    else:
        obj = {
            "metadata": {"name": POOL_NAME},
            "spec": {
                "type": "Replica1",
                "volume_id": "bellagio-hosting-volume",
                "single_pv_per_pool": True,
                "storage": [{
                    "node_id": "node-eleven-ocean",
                    "node": "bellagio-storage-one",
                    "path": "/srv/bellagio-vault",
                }],
            },
        }
        operator.update_config_map(client, obj)

    record = _saved_record(client)
    assert record["legacy_single_pv_volume_id"] == VOLUME_ID
    assert "single_pv_claim_version" not in record


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_reconciliation_preserves_current_claim_format(monkeypatch, pool_type):
    operator = _load_operator(monkeypatch)
    existing = {
        **_legacy_pool_record(),
        "type": pool_type,
        "single_pv_claim_version": 1,
    }
    client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    data = {
        "volname": POOL_NAME,
        "type": pool_type,
        "single_pv_per_pool": True,
    }

    operator.preserve_single_pv_claim_metadata(
        data,
        client.config_map.data[f"{POOL_NAME}.info"],
    )

    assert data["single_pv_claim_version"] == 1
    assert "legacy_single_pv_volume_id" not in data


def test_reconciliation_rejects_single_pv_mode_change_without_writing(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        **_legacy_pool_record(),
        "volume_id": "bellagio-hosting-volume",
    }
    existing["mount_identity"] = MOUNT_IDENTITY
    existing["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(existing)
    )
    client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "Replica1",
            "volume_id": "bellagio-hosting-volume",
            "single_pv_per_pool": False,
            "storage": [{
                "node_id": "node-eleven-ocean",
                "node": "bellagio-storage-one",
                "path": "/srv/bellagio-vault",
            }],
        },
    }

    assert operator.update_config_map(client, obj) is False
    assert _saved_record(client) == existing
    assert not client.patches


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_validation_rejects_archive_for_whole_pool_mode(
        monkeypatch, pool_type):
    operator = _load_operator(monkeypatch)
    spec = {
        "type": pool_type,
        "pvReclaimPolicy": "archive",
        "single_pv_per_pool": True,
    }
    if pool_type == "External":
        spec["details"] = {
            "gluster_host": "bellagio.example.invalid",
            "gluster_volname": "the-benedict-vault",
        }
    else:
        spec["storage"] = [{
            "node": "bellagio-storage-one",
            "path": "/srv/bellagio-vault",
        }]

    assert operator.validate_volume_request({
        "metadata": {"name": POOL_NAME},
        "spec": spec,
    }) is False


@pytest.mark.parametrize(
    "obj",
    [
        {},
        {"metadata": {"name": "empty-bellagio-vault"}},
        {
            "metadata": {"name": "string-bellagio-vault"},
            "spec": "the-benedict-job",
        },
        {
            "metadata": {"name": "list-bellagio-vault"},
            "spec": [],
        },
    ],
    ids=["empty-object", "missing-spec", "string-spec", "list-spec"],
)
def test_validation_rejects_missing_or_non_object_spec(monkeypatch, obj):
    operator = _load_operator(monkeypatch)

    assert operator.validate_volume_request(obj) is False


def test_native_generated_name_boundaries_match_server_pod_hostname_limit(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    name_52 = f"bellagio-{'vault' * 8}job"
    name_53 = f"{name_52}s"

    def request(name, brick_count):
        return {
            "metadata": {"name": name},
            "spec": {
                "type": "Replica1",
                "storage": [
                    {
                        "node": f"casino-blade-{index}",
                        "path": f"/srv/bellagio-vault-{index}",
                    }
                    for index in range(brick_count)
                ],
            },
        }

    assert len(name_52) == 52
    assert len(name_53) == 53
    assert operator.validate_volume_request(request(name_52, 10)) is True
    assert operator.validate_volume_request(request(name_53, 1)) is False
    assert operator.validate_volume_request(request(name_52, 11)) is False
    assert operator.validate_volume_request(
        request("bellagio.pool", 1)
    ) is False


@pytest.mark.parametrize(
    "storage_class_name",
    [
        "default",
        "bellagio-vault",
        "vault.storage.example.invalid",
    ],
)
def test_validation_accepts_explicit_storage_class_dns_subdomain(
        monkeypatch, storage_class_name):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name=storage_class_name)

    assert operator.validate_volume_request(obj) is True


@pytest.mark.parametrize(
    "storage_class_name",
    [
        "",
        "Default",
        "default/storage",
        "-default",
        "default-",
        f"{'a' * 251}.io",
    ],
)
def test_validation_rejects_invalid_explicit_storage_class_name(
        monkeypatch, storage_class_name):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name=storage_class_name)

    assert operator.validate_volume_request(obj) is False


@pytest.mark.parametrize(
    "storage",
    [
        [{"pvc": "Bellagio Crew"}],
        [{
            "node": "bellagio-storage-one",
            "path": "srv/bellagio-vault",
        }],
        [{
            "node": "bellagio-storage-one",
            "path": "/srv/casino/../bellagio-vault",
        }],
        [
            {
                "node": "bellagio-storage-one",
                "path": "/srv/bellagio-vault",
            },
            {
                "node": "bellagio-storage-one",
                "path": "/srv/bellagio-vault",
            },
        ],
    ],
    ids=["invalid-pvc", "relative", "noncanonical", "duplicate"],
)
def test_invalid_native_backing_is_rejected_before_cluster_mutation(
        monkeypatch, storage):
    operator = _load_operator(monkeypatch)
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {"type": "Replica1", "storage": storage},
    }

    assert operator.handle_added(MutationGuardClient(), obj) is False


@pytest.mark.parametrize(
    "details",
    [
        {
            "gluster_hosts": "casino.example.invalid",
            "gluster_volname": "benedict-vault",
        },
        {
            "gluster_hosts": ["bad casino host"],
            "gluster_volname": "benedict-vault",
        },
        {"gluster_hosts": ["casino.example.invalid"]},
        {
            "gluster_hosts": ["casino.example.invalid"],
            "gluster_volname": "benedict-vault/../mirage-loot",
        },
        {
            "gluster_hosts": ["casino.example.invalid"],
            "gluster_volname": "benedict-vault",
            "gluster_options": "log-level=WARNING\nmirage-option=on",
        },
        {
            "gluster_hosts": ["casino.example.invalid"],
            "gluster_volname": "benedict-vault",
            "gluster_port": True,
        },
    ],
    ids=[
        "hosts-scalar",
        "invalid-host",
        "missing-volname",
        "noncanonical-volname",
        "control-option",
        "boolean-port",
    ],
)
def test_malformed_external_details_are_rejected_before_cluster_mutation(
        monkeypatch, details):
    operator = _load_operator(monkeypatch)
    obj = {
        "metadata": {"name": "benedict-external-vault"},
        "spec": {"type": "External", "details": details},
    }

    assert operator.handle_added(MutationGuardClient(), obj) is False


@pytest.mark.parametrize(
    "brick",
    [
        {},
        {"pvc": ""},
        {"pvc": "   "},
        {"path": "", "node": "bellagio-storage-one"},
        {"device": "", "node": "bellagio-storage-one"},
        {"path": "/srv/bellagio-vault"},
        {"path": "/srv/bellagio-vault", "node": ""},
        {"device": "/dev/ocean-eleven", "node": "   "},
        {
            "path": "/srv/bellagio-vault",
            "device": "/dev/ocean-eleven",
            "node": "bellagio-storage-one",
        },
        {
            "pvc": "benedict-vault-claim",
            "path": "/srv/bellagio-vault",
            "node": "bellagio-storage-one",
        },
        {"pvc": 11},
    ],
    ids=[
        "no-backing",
        "empty-pvc",
        "blank-pvc",
        "empty-path",
        "empty-device",
        "path-without-node",
        "path-with-empty-node",
        "device-with-blank-node",
        "path-and-device",
        "pvc-and-path",
        "non-string-pvc",
    ],
)
def test_brick_validation_rejects_ephemeral_or_ambiguous_backing(
        monkeypatch, brick):
    operator = _load_operator(monkeypatch)

    assert operator.bricks_validation([brick]) is False


@pytest.mark.parametrize(
    "brick",
    [
        {"pvc": "benedict-vault-claim"},
        {
            "pvc": "",
            "path": "/srv/bellagio-vault",
            "node": "bellagio-storage-one",
        },
        {
            "device": "/dev/ocean-eleven",
            "node": "bellagio-storage-two",
        },
    ],
    ids=["pvc", "path", "device"],
)
def test_brick_validation_accepts_one_durable_backing(monkeypatch, brick):
    operator = _load_operator(monkeypatch)

    assert operator.bricks_validation([brick]) is True


def test_tolerations_are_canonicalized_like_kubernetes(monkeypatch):
    operator = _load_operator(monkeypatch)

    assert operator.normalize_tolerations(
        [
            {
                "key": "security.bellagio.invalid/crew",
                "operator": "",
                "effect": "",
                "value": "eleven",
            },
            {"key": "", "operator": "Exists", "value": ""},
            {
                "key": "mirage-eviction",
                "operator": "Exists",
                "effect": "NoExecute",
                "tolerationSeconds": -1,
            },
        ],
        "Bellagio crew",
    ) == [
        {"operator": "Exists"},
        {
            "effect": "NoExecute",
            "key": "mirage-eviction",
            "operator": "Exists",
            "tolerationSeconds": -1,
        },
        {
            "key": "security.bellagio.invalid/crew",
            "operator": "Equal",
            "value": "eleven",
        },
    ]


@pytest.mark.parametrize(
    "toleration",
    [
        {},
        {"operator": "Equal"},
        {"operator": "Exists", "value": "eleven"},
        {"key": "bellagio", "operator": "TheHeist"},
        {"key": "bellagio", "effect": "DuringTheHeist"},
        {"key": "bad key"},
        {"key": "Bellagio.Invalid/crew"},
        {"key": "bellagio", "value": "bad value"},
        {"key": "bellagio", "tolerationSeconds": 11},
        {
            "key": "bellagio",
            "effect": "NoExecute",
            "tolerationSeconds": True,
        },
        {
            "key": "bellagio",
            "effect": "NoExecute",
            "tolerationSeconds": 2 ** 63,
        },
        {
            "key": "bellagio",
            "effect": "NoExecute",
            "tolerationSeconds": -(2 ** 63) - 1,
        },
    ],
    ids=[
        "empty",
        "empty-key-equal",
        "exists-with-value",
        "operator",
        "effect",
        "key-syntax",
        "key-prefix-syntax",
        "value-syntax",
        "seconds-without-noexecute",
        "boolean-seconds",
        "seconds-over-int64",
        "seconds-under-int64",
    ],
)
def test_toleration_validation_rejects_pod_api_errors(
        monkeypatch, toleration):
    operator = _load_operator(monkeypatch)

    with pytest.raises(RuntimeError, match="invalid toleration"):
        operator.normalize_tolerations([toleration], "Bellagio crew")


def test_invalid_toleration_is_quarantined_from_upgrade_plan(monkeypatch):
    operator = _load_operator(monkeypatch)
    storage_obj = _storage_custom_object(tolerations=[{}])
    plan = operator.prepare_storage_upgrade(
        FakeCoreV1Client({}),
        object(),
        {
            "items": [storage_obj],
            "metadata": {"resourceVersion": "11-invalid-toleration"},
        },
    )

    assert plan["invalid_items"] == [storage_obj]
    assert not plan["reconcile_items"]
    assert not plan["upgrade_objects"]
    assert not plan["deletion_orphans"]


def test_mount_identity_is_stable_while_backend_fingerprint_changes(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    original = {
        "volname": POOL_NAME,
        "volume_id": "bellagio-hosting-volume",
        "type": "Replica1",
        "single_pv_per_pool": False,
        "bricks": [{
            "brick_index": 0,
            "node": "server-bellagio-pool-0-0.bellagio-pool",
        }],
        "options": {"performance.client-io-threads": "on"},
    }
    assert operator.apply_pool_mount_metadata(original, None) is True
    original_identity = original["mount_identity"]
    original_fingerprint = original["mount_config_fingerprint"]

    changed = {
        **original,
        "options": {"performance.client-io-threads": "off"},
    }
    assert operator.apply_pool_mount_metadata(
        changed,
        json.dumps(original),
    ) is True

    assert changed["mount_identity"] == original_identity
    assert changed["mount_config_fingerprint"] != original_fingerprint
    _assert_mount_metadata(operator, changed)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("node", "mirage-storage-one"),
        ("path", "/srv/mirage-decoy-vault"),
        ("device", "/dev/mirage-decoy-vault"),
        ("pvc", "mirage-decoy-claim"),
    ],
    ids=["node", "path", "device", "pvc"],
)
def test_native_modified_rejects_backend_retarget_without_writing(
        monkeypatch, field, value):
    operator = _load_operator(monkeypatch)
    existing = _stored_native_pool()
    existing["volume_id"] = HOSTING_VOLUME_ID
    assert operator.apply_pool_mount_metadata(existing, None) is True
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    obj = _native_pool_object()
    obj["spec"]["volume_id"] = HOSTING_VOLUME_ID
    obj["spec"]["storage"][0].update({
        "node": f"{POOL_NAME}-storage-0",
        "path": f"/srv/{POOL_NAME}-0",
    })
    if field in ("device", "pvc"):
        obj["spec"]["storage"][0].pop("path")
    obj["spec"]["storage"][0][field] = value
    monkeypatch.setattr(
        operator,
        "deploy_storage_class",
        lambda *_args: pytest.fail("backend retarget deployed a StorageClass"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: pytest.fail("backend retarget deployed a server"),
    )

    assert operator.handle_modified(core_client, obj, object()) is False
    assert _saved_record(core_client) == existing
    assert not core_client.patches


def test_native_backend_retarget_aborts_upgrade_before_provisioner_fence(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    storage_obj = _storage_custom_object()
    storage_obj["spec"]["storage"][0]["path"] = (
        "/srv/mirage-decoy-vault"
    )
    storage_list = {
        "items": [storage_obj],
        "metadata": {"resourceVersion": "12-backend-retarget"},
    }
    core_client = FakeCoreV1Client({
        f"{POOL_NAME}.info": _stored_native_pool(),
    })
    monkeypatch.setattr(
        operator,
        "list_storage_resources",
        lambda _client: storage_list,
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: pytest.fail("backend retarget reached the fence"),
    )

    with pytest.raises(RuntimeError, match="immutable native"):
        operator.deploy_csi_upgrade(
            core_client,
            object(),
            object(),
        )


def test_native_disperse_geometry_change_is_immutable(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(
        pool_type="Disperse",
        bricks=3,
    )
    record["disperse"] = {"data": 2, "redundancy": 1}
    storage_obj = _storage_custom_object(pool_type="Disperse")
    storage_obj["spec"]["disperse"] = {"data": 4, "redundancy": 2}
    storage_obj["spec"]["storage"].extend([
        {
            "node": f"{POOL_NAME}-storage-{index}",
            "path": f"/srv/{POOL_NAME}-{index}",
        }
        for index in range(3, 6)
    ])

    with pytest.raises(RuntimeError, match="immutable native"):
        operator.prepare_storage_upgrade(
            FakeCoreV1Client({f"{POOL_NAME}.info": record}),
            object(),
            {
                "items": [storage_obj],
                "metadata": {"resourceVersion": "13-disperse-retarget"},
            },
        )


def test_migration_never_overwrites_recorded_legacy_owner(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        **_legacy_pool_record(),
        "legacy_single_pv_volume_id": VOLUME_ID,
    }
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": existing},
        [_persistent_volume(volume_id=OTHER_VOLUME_ID)],
    )

    operator.migrate_legacy_single_pv_claims(client)

    assert _saved_record(client)["legacy_single_pv_volume_id"] == VOLUME_ID


def test_added_reconciliation_reuses_existing_volume_identity(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "External",
        "single_pv_per_pool": False,
        "gluster_hosts": "bellagio.example.invalid",
        "gluster_volname": "the-benedict-vault",
    }
    client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[])
    received = []
    monkeypatch.setattr(
        operator,
        "handle_external_storage_addition",
        lambda _client, obj, _storage_api=None: received.append(
            obj["spec"]["volume_id"]
        ),
    )
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "External",
            "single_pv_per_pool": False,
            "details": {
                "gluster_host": "bellagio.example.invalid",
                "gluster_volname": "the-benedict-vault",
            },
        },
    }

    operator.handle_added(client, obj)

    assert received == [HOSTING_VOLUME_ID]


def test_added_existing_server_reconciles_offline_config_change(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "Replica1",
        "single_pv_per_pool": False,
        "pvReclaimPolicy": "delete",
        "bricks": [{
            "brick_index": 0,
            "node_id": "node-0",
            "brick_path": f"/bricks/{POOL_NAME}/data/brick",
            "node": f"server-{POOL_NAME}-0-0.{POOL_NAME}",
            "host_brick_path": "/srv/bellagio-vault",
            "kube_hostname": "bellagio-storage-one",
            "brick_device": "",
            "pvc_name": "",
        }],
    }
    client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        SimpleNamespace(metadata=SimpleNamespace(
            name=f"server-{POOL_NAME}-0-0",
        )),
    ])
    deployed_classes = []
    monkeypatch.setattr(
        operator,
        "deploy_storage_class",
        lambda obj, _client, _storage_api=None: deployed_classes.append(
            obj["metadata"]["name"]
        ),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda deployed, _apps=None: deployed_servers.append(
            deployed["metadata"]["name"]
        ),
    )
    deployed_servers = []
    rendered_services = []
    commands = []
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **_kwargs: rendered_services.append(filename),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )
    obj = _native_pool_object(policy="archive")

    operator.handle_added(client, obj, provisioner_fenced=True)

    assert _saved_record(client)["pvReclaimPolicy"] == "archive"
    assert deployed_classes == [POOL_NAME]
    assert deployed_servers == [POOL_NAME]
    # Service creation now belongs to deploy_server_pods so peer DNS exists
    # before its rollout/heal gate. This test replaces that function entirely.
    assert not rendered_services
    assert not commands


def test_missing_native_record_recovers_consistent_server_volume_id(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(),
    ])
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    deployed_servers = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda deployed, _apps=None: deployed_servers.append(
            deployed["metadata"]["name"]
        ),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    obj["spec"].pop("volume_id")
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )

    operator.handle_added(client, obj, storage_api=storage_api)

    assert obj["spec"]["volume_id"] == HOSTING_VOLUME_ID
    assert _saved_record(client)["volume_id"] == HOSTING_VOLUME_ID
    assert deployed_servers == [POOL_NAME]


def test_native_volume_id_recovery_ignores_prefix_collision(monkeypatch):
    operator = _load_operator(monkeypatch)
    annex_pod = _server_pod(OTHER_HOSTING_VOLUME_ID)
    annex_pod.metadata.name = f"server-{POOL_NAME}-annex-0-0"
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        annex_pod,
        _server_pod(),
    ])
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    deployed_servers = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda deployed, _apps=None: deployed_servers.append(
            deployed["metadata"]["name"]
        ),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    obj["spec"].pop("volume_id")
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )

    operator.handle_added(client, obj, storage_api=storage_api)

    assert obj["spec"]["volume_id"] == HOSTING_VOLUME_ID
    assert _saved_record(client)["volume_id"] == HOSTING_VOLUME_ID
    assert deployed_servers == [POOL_NAME]


@pytest.mark.parametrize(
    "pods",
    [
        [_server_pod(None)],
        [_server_pod("not-a-bellagio-uuid")],
        [
            _server_pod(HOSTING_VOLUME_ID, ordinal=0),
            _server_pod(OTHER_HOSTING_VOLUME_ID, ordinal=1),
        ],
    ],
    ids=["missing", "invalid", "conflicting"],
)
def test_missing_native_record_rejects_unreliable_server_volume_id(
        monkeypatch, pods):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=pods)
    obj = _native_pool_object()
    obj["spec"].pop("volume_id")

    operator.handle_added(client, obj)

    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_missing_native_record_rejects_pv_evidence_without_hosting_id(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({}, [
        _persistent_volume(single_pv_per_pool="false"),
    ])
    obj = _native_pool_object()
    obj["spec"].pop("volume_id")

    operator.handle_added(client, obj)

    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_missing_native_record_reconciles_supplied_id_with_live_server(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(),
    ])
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    monkeypatch.setattr(operator, "deploy_server_pods", lambda *_args: None)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is True
    assert _saved_record(client)["volume_id"] == HOSTING_VOLUME_ID


def test_missing_native_record_rejects_supplied_id_disagreeing_with_server(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(),
    ])
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    obj["spec"]["volume_id"] = OTHER_HOSTING_VOLUME_ID
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_missing_native_record_rejects_server_without_cr_owner_proof(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(),
    ])
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-recreated-bellagio"

    assert operator.handle_added(client, obj) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_missing_native_record_rejects_durable_class_without_live_server(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj, durable=True),
        ]),
    )
    client = FakeCoreV1Client({})

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_missing_native_record_rejects_unproven_replica2_tiebreaker(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    obj["spec"].update({
        "type": "Replica2",
        "storage": [
            obj["spec"]["storage"][0],
            {
                "node": "bellagio-storage-two",
                "path": "/srv/bellagio-vault-two",
            },
        ],
        "tiebreaker": {
            "deployment": "bellagio-tiebreaker",
            "node": "bellagio-witness",
            "path": "/srv/bellagio-witness",
        },
    })
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(pool_type="Replica2"),
        _server_pod(
            pool_type="Replica2",
            brick_index=1,
            node="bellagio-storage-two",
            path="/srv/bellagio-vault-two",
        ),
    ])
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_missing_native_record_rejects_pv_with_only_caller_supplied_id(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({}, [
        _persistent_volume(single_pv_per_pool="false"),
    ])

    assert operator.handle_added(client, _native_pool_object()) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


@pytest.mark.parametrize(
    "pod",
    [
        _server_pod(node="mirage-storage-one"),
        _server_pod(path="/srv/mirage-decoy-vault"),
        _server_pod(path="", device="/dev/roulette-wheel"),
        _server_pod(node="", path="", pvc="mirage-decoy-claim"),
        _server_pod(pool_type="Replica2"),
        _server_pod(brick_index=1),
    ],
    ids=["node", "path", "device", "pvc", "type", "index-count"],
)
def test_native_missing_record_rejects_different_live_geometry(
        monkeypatch, pod):
    operator = _load_operator(monkeypatch)

    with pytest.raises(ValueError):
        operator.recover_native_pool_volume_id(
            _native_pool_object(),
            [pod],
        )


def test_native_missing_record_accepts_raw_statefulset_device_evidence(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["spec"]["storage"][0].pop("path")
    obj["spec"]["storage"][0]["device"] = (
        "/srv/bellagio-devices/vault.img"
    )
    stateful_set = _server_statefulset_evidence(
        path="",
        device="/srv/bellagio-devices/vault.img",
    )
    apps_client = SimpleNamespace(
        list_namespaced_stateful_set=lambda _namespace: SimpleNamespace(
            items=[stateful_set]
        ),
    )

    assert operator.recover_native_pool_volume_id(
        obj,
        [],
        apps_client,
    ) == HOSTING_VOLUME_ID


def test_native_missing_record_accepts_unpinned_pvc_server_evidence(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["spec"]["storage"][0] = {"pvc": "bellagio-vault-claim"}
    pod = _server_pod(
        node="",
        path="",
        pvc="bellagio-vault-claim",
    )

    assert operator.recover_native_pool_volume_id(
        obj,
        [pod],
    ) == HOSTING_VOLUME_ID


@pytest.mark.parametrize(
    "storage_class_name",
    [None, "default"],
    ids=["legacy-fallback", "explicit-default"],
)
def test_native_retained_pv_recovers_mount_identity_from_exact_class(
        monkeypatch, storage_class_name):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name=storage_class_name)
    obj["metadata"]["uid"] = "uid-bellagio-native"
    storage_class = _owned_storage_class(operator, obj, durable=True)
    storage_api = FakeStorageV1Api([storage_class])
    client = FakeCoreV1Client({}, [
        _persistent_volume(
            storage_class_name=storage_class_name,
            single_pv_per_pool="false",
        ),
    ])
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(),
    ])
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    monkeypatch.setattr(operator, "deploy_server_pods", lambda *_args: None)

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is True
    record = _saved_record(client)
    assert record["volume_id"] == HOSTING_VOLUME_ID
    assert record["mount_identity"] == MOUNT_IDENTITY
    assert record["storageClassName"] == (
        storage_class_name or f"kadalu.{POOL_NAME}"
    )


def test_external_storage_class_backend_fingerprint_matches_stored_record(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object()
    record = {
        "volname": POOL_NAME,
        "type": "External",
        "single_pv_per_pool": False,
        "gluster_hosts": (
            "bellagio-two.example.invalid,bellagio-one.example.invalid"
        ),
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "log-level=WARNING",
    }

    assert operator.storage_class_backend_fingerprint(obj) == (
        operator.storage_class_backend_fingerprint(record)
    )


@pytest.mark.parametrize("include_volume_id", [False, True])
@pytest.mark.parametrize(
    "storage_class_name",
    [None, "default"],
    ids=["legacy-fallback", "explicit-default"],
)
def test_external_retained_pv_recovers_durable_storage_class_identity(
        monkeypatch, include_volume_id, storage_class_name):
    operator = _load_operator(monkeypatch)
    authoritative = _external_pool_object(
        storage_class_name=storage_class_name,
    )
    storage_class = _owned_storage_class(
        operator,
        authoritative,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    client = FakeCoreV1Client({}, [
        _persistent_volume(
            storage_class_name=storage_class_name,
            single_pv_per_pool="false",
        ),
    ])
    rendered = []
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **values: rendered.append((filename, values)),
    )
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    obj = _external_pool_object(
        include_volume_id=include_volume_id,
        storage_class_name=storage_class_name,
    )

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is True
    record = _saved_record(client)
    assert obj["spec"]["volume_id"] == HOSTING_VOLUME_ID
    assert record["volume_id"] == HOSTING_VOLUME_ID
    assert record["mount_identity"] == MOUNT_IDENTITY
    assert record["storageClassName"] == (
        storage_class_name or f"kadalu.{POOL_NAME}"
    )
    assert rendered[0][1]["mount_identity"] == MOUNT_IDENTITY
    assert rendered[0][1]["backend_fingerprint"] == (
        operator.storage_class_backend_fingerprint(record)
    )


def test_external_retained_pv_rejects_supplied_identity_mismatch(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    authoritative = _external_pool_object()
    storage_class = _owned_storage_class(
        operator,
        authoritative,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    client = FakeCoreV1Client({}, [
        _persistent_volume(single_pv_per_pool="false"),
    ])
    obj = _external_pool_object()
    obj["spec"]["volume_id"] = OTHER_HOSTING_VOLUME_ID

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False

    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches
    assert not client.pv_patches


@pytest.mark.parametrize("legacy_class", [False, True])
@pytest.mark.parametrize("include_volume_id", [False, True])
def test_external_retained_pv_rejects_missing_durable_identity(
        monkeypatch, legacy_class, include_volume_id):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object(include_volume_id=include_volume_id)
    storage_classes = (
        [_owned_storage_class(operator, obj)]
        if legacy_class
        else []
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=storage_classes),
    )
    client = FakeCoreV1Client({}, [
        _persistent_volume(single_pv_per_pool="false"),
    ])
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: pytest.fail(
            "unproven External identity rendered a StorageClass"
        ),
    )

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches
    assert not client.pv_patches


def test_external_missing_record_rejects_legacy_class_without_pvs(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object()
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )
    client = FakeCoreV1Client({})

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches


def test_external_missing_record_without_retained_evidence_is_fresh(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object()
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[]),
    )
    client = FakeCoreV1Client({})
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is True
    record = _saved_record(client)
    assert record["volume_id"] == HOSTING_VOLUME_ID
    assert str(uuid.UUID(record["mount_identity"])) == (
        record["mount_identity"]
    )


@pytest.mark.parametrize(
    ("annotation", "value"),
    [
        ("kadalu.io/storage-uid", "uid-mirage-decoy"),
        ("kadalu.io/volume-id", "not-a-bellagio-uuid"),
        ("kadalu.io/backend-fingerprint", "0" * 64),
    ],
    ids=["uid", "volume-id", "backend"],
)
def test_external_missing_record_rejects_invalid_durable_class(
        monkeypatch, annotation, value):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object(include_volume_id=False)
    authoritative = _external_pool_object()
    storage_class = _owned_storage_class(
        operator,
        authoritative,
        durable=True,
        annotation_overrides={annotation: value},
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    client = FakeCoreV1Client({}, [
        _persistent_volume(single_pv_per_pool="false"),
    ])

    assert operator.handle_added(
        client,
        obj,
        storage_api=storage_api,
    ) is False

    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches
    assert not client.pv_patches


def test_external_handler_rechecks_missing_record_pv_evidence(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object()
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[
            _owned_storage_class(operator, obj),
        ]),
    )
    client = FakeCoreV1Client({}, [
        _persistent_volume(single_pv_per_pool="false"),
    ])

    assert operator.handle_external_storage_addition(
        client,
        obj,
        storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in client.config_map.data
    assert not client.patches
    assert not client.pv_patches


@pytest.mark.parametrize(
    ("pool_policy", "existing_policy", "expected_events"),
    [
        (
            "retain",
            "Delete",
            [
                ("persistent-volume", VOLUME_ID),
                ("config-map", "kadalu-info"),
            ],
        ),
        (
            "delete",
            "Retain",
            [
                ("config-map", "kadalu-info"),
                ("persistent-volume", VOLUME_ID),
            ],
        ),
    ],
    ids=["retain-before-config", "delete-after-config"],
)
def test_existing_pv_reclaim_policy_uses_data_safe_order(
        monkeypatch, pool_policy, existing_policy, expected_events):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "Replica1",
        "single_pv_per_pool": False,
        "pvReclaimPolicy": (
            "delete" if existing_policy == "Delete" else "retain"
        ),
        "bricks": [{
            "brick_index": 0,
            "node_id": "node-eleven-ocean",
            "host_brick_path": "/srv/bellagio-vault",
            "kube_hostname": "bellagio-storage-one",
            "brick_device": "",
            "pvc_name": "",
        }],
    }
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": existing},
        [_persistent_volume(
            single_pv_per_pool="false",
            reclaim_policy=existing_policy,
        )],
    )

    assert operator.update_config_map(
        client,
        _native_pool_object(policy=pool_policy),
    ) is True

    assert client.events == expected_events
    expected_policy = "Retain" if pool_policy == "retain" else "Delete"
    assert client.pv_patches == [(
        VOLUME_ID,
        {"spec": {"persistentVolumeReclaimPolicy": expected_policy}},
    )]


def _runtime_reclaim_policy_resources(
        operator, pool_type="Replica1", current="delete", requested="retain",
        storage_class_name=None):
    """Build one fully owned pool and its mutable Kubernetes surfaces."""
    storage_uid = f"uid-{POOL_NAME}-runtime-policy"
    if pool_type == "External":
        obj = _external_pool_object(
            storage_class_name=storage_class_name,
        )
        volume_id = HOSTING_VOLUME_ID
        record = {
            "volname": POOL_NAME,
            "volume_id": volume_id,
            "storage_uid": obj["metadata"]["uid"],
            "type": "External",
            "pvReclaimPolicy": current,
            "single_pv_per_pool": False,
            "gluster_hosts": (
                "bellagio-one.example.invalid,"
                "bellagio-two.example.invalid"
            ),
            "gluster_volname": "the-benedict-vault",
            "gluster_options": "log-level=WARNING",
            "mount_identity": MOUNT_IDENTITY,
        }
    else:
        obj = _storage_custom_object()
        if storage_class_name is not None:
            obj["spec"]["storageClassName"] = storage_class_name
        obj["metadata"]["uid"] = storage_uid
        volume_id = _pool_volume_id(POOL_NAME)
        obj["spec"].update({
            "volume_id": volume_id,
            "single_pv_per_pool": False,
        })
        record = _stored_native_pool(
            storage_class_name=storage_class_name,
        )
        record.update({
            "volume_id": volume_id,
            "storage_uid": storage_uid,
            "single_pv_per_pool": False,
            "pvReclaimPolicy": current,
            "mount_identity": MOUNT_IDENTITY,
        })
    if storage_class_name is not None:
        record["storageClassName"] = storage_class_name
    obj["metadata"]["resourceVersion"] = "201"
    obj["spec"]["pvReclaimPolicy"] = requested
    record["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(record)
    )
    persistent_volume = _persistent_volume(
        volume_id=VOLUME_ID,
        storage_class_name=storage_class_name,
        single_pv_per_pool="false",
        reclaim_policy=operator.storage_class_reclaim_policy(current),
    )
    storage_class = _owned_storage_class(
        operator,
        obj,
        durable=True,
        volume_id=volume_id,
    )
    storage_class.reclaim_policy = (
        operator.storage_class_reclaim_policy(current)
    )
    return obj, record, persistent_volume, storage_class


def _runtime_storage_client(*responses):
    """Return current CR snapshots, repeating the final response."""
    calls = []

    def get_namespaced_custom_object(
            group, version, namespace, plural, name):
        calls.append((group, version, namespace, plural, name))
        response = responses[min(len(calls) - 1, len(responses) - 1)]
        if isinstance(response, Exception):
            raise response
        return response

    return SimpleNamespace(
        get_namespaced_custom_object=get_namespaced_custom_object,
        calls=calls,
    )


class _FakeCustomObjectsAPIError(RuntimeError):
    """Expose a Kubernetes-style HTTP status for CR read failures."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def _install_runtime_policy_test_hooks(
        monkeypatch, operator, core_client, storage_class, events,
        fence_state):
    """Install observable data and readiness surfaces for a policy test."""
    original_list = core_client.list_persistent_volume
    original_patch = core_client.patch_persistent_volume
    original_config_patch = core_client.patch_namespaced_config_map

    def list_persistent_volumes():
        assert fence_state["active"] is True
        events.append("list-persistent-volumes")
        return original_list()

    def patch_persistent_volume(name, body):
        assert fence_state["active"] is True
        events.append("patch-persistent-volume")
        original_patch(name, body)
        next(
            item for item in core_client.persistent_volumes
            if item.metadata.name == name
        ).spec.persistent_volume_reclaim_policy = (
            body["spec"]["persistentVolumeReclaimPolicy"]
        )

    def patch_config_map(name, namespace, body):
        assert fence_state["active"] is True
        events.append("patch-config-map")
        original_config_patch(name, namespace, body)

    core_client.list_persistent_volume = list_persistent_volumes
    core_client.patch_persistent_volume = patch_persistent_volume
    core_client.patch_namespaced_config_map = patch_config_map

    def quiesce(_core, _apps):
        assert fence_state["active"] is False
        events.append("quiesce")
        fence_state["active"] = True

    def resume(_apps):
        assert fence_state["active"] is True
        events.append("resume")
        fence_state["active"] = False

    def reconcile_storage_class(
            _storage_api, _filename, _name, reclaim_policy, identity=None):
        assert fence_state["active"] is True
        assert _name == storage_class.metadata.name
        events.append("reconcile-storage-class")
        storage_class.reclaim_policy = reclaim_policy
        storage_class.metadata.annotations = {
            operator.STORAGE_CLASS_NAMESPACE_ANNOTATION: (
                identity["namespace"]
            ),
            operator.STORAGE_CLASS_NAME_ANNOTATION: identity["name"],
            operator.STORAGE_CLASS_UID_ANNOTATION: identity["uid"],
            operator.STORAGE_CLASS_VOLUME_ID_ANNOTATION: (
                identity["volume_id"]
            ),
            operator.STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION: (
                identity["mount_identity"]
            ),
            operator.STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION: (
                identity["backend_fingerprint"]
            ),
        }

    monkeypatch.setattr(
        operator,
        "clear_operator_ready",
        lambda: events.append("clear-ready"),
    )
    monkeypatch.setattr(operator, "quiesce_csi_provisioner", quiesce)
    monkeypatch.setattr(operator, "resume_csi_provisioner", resume)
    monkeypatch.setattr(
        operator,
        "wait_for_csi_provisioner_rollout",
        lambda _apps: (
            events.append("wait-provisioner")
            if fence_state["active"] is False
            else pytest.fail("serving wait ran while provisioner was fenced")
        ),
    )
    monkeypatch.setattr(
        operator,
        "mark_operator_ready",
        lambda: events.append("mark-ready"),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        operator.os,
        "listdir",
        lambda _path: ["storageclass-kadalu.custom.yaml.j2"],
    )
    monkeypatch.setattr(
        operator,
        "reconcile_storage_class_manifest",
        reconcile_storage_class,
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: (
            events.append("reconcile-server")
            if fence_state["active"] is True
            else pytest.fail("server reconciliation escaped the fence")
        ),
    )


@pytest.mark.parametrize("handler_name", ["handle_added", "handle_modified"])
@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
@pytest.mark.parametrize(
    ("current", "requested", "expected"),
    [
        ("delete", "retain", "Retain"),
        ("retain", "delete", "Delete"),
        ("delete", "archive", "Delete"),
        ("archive", "delete", "Delete"),
    ],
    ids=[
        "delete-to-retain",
        "retain-to-delete",
        "delete-to-archive",
        "archive-to-delete",
    ],
)
def test_runtime_reclaim_policy_transition_fences_every_data_surface(
        monkeypatch, handler_name, pool_type, current, requested, expected):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(
            operator,
            pool_type,
            current,
            requested,
        )
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    apps_client = object()
    storage_client = _runtime_storage_client(obj)
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    assert getattr(operator, handler_name)(
        core_client,
        obj,
        apps_client,
        storage_api,
        storage_client=storage_client,
    ) is True

    assert events[:2] == ["clear-ready", "quiesce"]
    assert events[-3:] == ["resume", "wait-provisioner", "mark-ready"]
    assert events.count("list-persistent-volumes") >= 2
    assert fence_state["active"] is False
    assert _saved_record(core_client)["pvReclaimPolicy"] == requested
    assert (
        persistent_volume.spec.persistent_volume_reclaim_policy
        == expected
    )
    assert storage_class.reclaim_policy == expected


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_custom_storage_class_retain_transition_keeps_name_and_fence(
        monkeypatch, pool_type):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(
            operator,
            pool_type,
            current="delete",
            requested="retain",
            storage_class_name="default",
        )
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    apps_client = object()
    storage_client = _runtime_storage_client(obj)
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    assert operator.handle_added(
        core_client,
        obj,
        apps_client,
        storage_api,
        storage_client=storage_client,
    ) is True

    assert events[:2] == ["clear-ready", "quiesce"]
    assert events[-3:] == ["resume", "wait-provisioner", "mark-ready"]
    assert _saved_record(core_client)["storageClassName"] == "default"
    assert storage_class.metadata.name == "default"
    assert storage_class.reclaim_policy == "Retain"
    assert (
        persistent_volume.spec.persistent_volume_reclaim_policy
        == "Retain"
    )


def test_runtime_reclaim_policy_uses_current_cr_after_quiesce(monkeypatch):
    operator = _load_operator(monkeypatch)
    stale, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(
            operator,
            current="retain",
            requested="delete",
        )
    )
    current = json.loads(json.dumps(stale))
    current["metadata"]["resourceVersion"] = "202"
    current["spec"]["pvReclaimPolicy"] = "retain"
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    storage_client = _runtime_storage_client(current)
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    assert operator.handle_modified(
        core_client,
        stale,
        object(),
        storage_api,
        storage_client=storage_client,
    ) is True

    assert _saved_record(core_client)["pvReclaimPolicy"] == "retain"
    assert persistent_volume.spec.persistent_volume_reclaim_policy == "Retain"
    assert storage_class.reclaim_policy == "Retain"
    assert events[-3:] == ["resume", "wait-provisioner", "mark-ready"]
    assert len(storage_client.calls) >= 2


def test_runtime_reclaim_policy_rechecks_cr_before_resume(monkeypatch):
    operator = _load_operator(monkeypatch)
    requested, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(
            operator,
            current="delete",
            requested="retain",
        )
    )
    superseding = json.loads(json.dumps(requested))
    superseding["metadata"]["resourceVersion"] = "202"
    superseding["spec"]["pvReclaimPolicy"] = "delete"
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    storage_client = _runtime_storage_client(
        requested,
        superseding,
        superseding,
    )
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    assert operator.handle_modified(
        core_client,
        requested,
        object(),
        storage_api,
        storage_client=storage_client,
    ) is True

    assert _saved_record(core_client)["pvReclaimPolicy"] == "delete"
    assert persistent_volume.spec.persistent_volume_reclaim_policy == "Delete"
    assert storage_class.reclaim_policy == "Delete"
    assert events.count("resume") == 1
    assert events[-3:] == ["resume", "wait-provisioner", "mark-ready"]
    assert len(storage_client.calls) >= 3


@pytest.mark.parametrize(
    ("current_factory", "message"),
    [
        (
            lambda obj: {
                **json.loads(json.dumps(obj)),
                "metadata": {
                    **obj["metadata"],
                    "resourceVersion": "202",
                    "deletionTimestamp": "2026-08-15T04:11:00Z",
                },
            },
            "is being deleted",
        ),
        (
            lambda obj: {
                **json.loads(json.dumps(obj)),
                "metadata": {
                    **obj["metadata"],
                    "uid": "uid-night-fox-replacement",
                    "resourceVersion": "202",
                },
            },
            "changed identity",
        ),
        (
            lambda _obj: _FakeCustomObjectsAPIError(
                404,
                "The Bellagio pool disappeared",
            ),
            "no longer exists",
        ),
    ],
    ids=["deleting", "same-name-replacement", "missing"],
)
def test_runtime_reclaim_policy_rejects_unsafe_current_cr(
        monkeypatch, current_factory, message):
    operator = _load_operator(monkeypatch)
    stale, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(
            operator,
            current="retain",
            requested="delete",
        )
    )
    storage_client = _runtime_storage_client(current_factory(stale))
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    with pytest.raises(RuntimeError, match=message):
        operator.handle_modified(
            core_client,
            stale,
            object(),
            storage_api,
            storage_client=storage_client,
        )

    assert _saved_record(core_client)["pvReclaimPolicy"] == "retain"
    assert not core_client.pv_patches
    assert "resume" not in events
    assert "mark-ready" not in events
    assert fence_state["active"] is True


def test_runtime_reclaim_policy_failure_keeps_fence_and_readiness_down(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(operator)
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    storage_client = _runtime_storage_client(obj)
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )
    monkeypatch.setattr(operator, "update_config_map", lambda *_args: False)

    assert operator.handle_modified(
        core_client,
        obj,
        object(),
        storage_api,
        storage_client=storage_client,
    ) is False

    assert events == ["clear-ready", "quiesce"]
    assert fence_state["active"] is True
    assert _saved_record(core_client)["pvReclaimPolicy"] == "delete"
    assert not core_client.pv_patches


def test_runtime_reclaim_policy_verification_failure_never_resumes(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(operator)
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    storage_client = _runtime_storage_client(obj)
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    def leave_storage_class_stale(*_args, **_kwargs):
        assert fence_state["active"] is True
        events.append("leave-storage-class-stale")

    monkeypatch.setattr(
        operator,
        "reconcile_storage_class_manifest",
        leave_storage_class_stale,
    )

    with pytest.raises(RuntimeError, match="StorageClass.*inconsistent"):
        operator.handle_modified(
            core_client,
            obj,
            object(),
            storage_api,
            storage_client=storage_client,
        )

    assert "resume" not in events
    assert "mark-ready" not in events
    assert fence_state["active"] is True


def test_runtime_reclaim_policy_rollout_failure_restores_fence(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(operator)
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    storage_client = _runtime_storage_client(obj)
    events = []
    fence_state = {"active": False}
    _install_runtime_policy_test_hooks(
        monkeypatch,
        operator,
        core_client,
        storage_class,
        events,
        fence_state,
    )

    def fail_rollout(_apps):
        events.append("rollout-failed")
        raise TimeoutError("Benedict's provisioner never became Ready")

    monkeypatch.setattr(
        operator,
        "wait_for_csi_provisioner_rollout",
        fail_rollout,
    )

    with pytest.raises(TimeoutError, match="never became Ready"):
        operator.handle_modified(
            core_client,
            obj,
            object(),
            storage_api,
            storage_client=storage_client,
        )

    assert events[-4:] == [
        "resume",
        "rollout-failed",
        "clear-ready",
        "quiesce",
    ]
    assert "mark-ready" not in events
    assert fence_state["active"] is True


def test_startup_reclaim_policy_transition_never_releases_global_fence(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(operator)
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    original_patch = core_client.patch_persistent_volume

    def patch_persistent_volume(name, body):
        original_patch(name, body)
        persistent_volume.spec.persistent_volume_reclaim_policy = (
            body["spec"]["persistentVolumeReclaimPolicy"]
        )

    core_client.patch_persistent_volume = patch_persistent_volume
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        operator.os,
        "listdir",
        lambda _path: ["storageclass-kadalu.custom.yaml.j2"],
    )
    monkeypatch.setattr(
        operator,
        "reconcile_storage_class_manifest",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(operator, "deploy_server_pods", lambda *_args: None)
    for function_name in (
            "clear_operator_ready",
            "quiesce_csi_provisioner",
            "resume_csi_provisioner",
            "wait_for_csi_provisioner_rollout",
            "mark_operator_ready"):
        monkeypatch.setattr(
            operator,
            function_name,
            lambda *_args, name=function_name: pytest.fail(
                f"startup transition called {name}"
            ),
        )

    assert operator.handle_modified(
        core_client,
        obj,
        object(),
        storage_api,
        provisioner_fenced=True,
    ) is True
    assert _saved_record(core_client)["pvReclaimPolicy"] == "retain"
    assert (
        persistent_volume.spec.persistent_volume_reclaim_policy
        == "Retain"
    )


def test_runtime_reclaim_policy_transition_without_apps_client_fails_closed(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj, record, persistent_volume, storage_class = (
        _runtime_reclaim_policy_resources(operator)
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [persistent_volume],
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    monkeypatch.setattr(
        operator,
        "clear_operator_ready",
        lambda: pytest.fail("missing Apps client changed readiness"),
    )

    assert operator.handle_modified(
        core_client,
        obj,
        storage_api=storage_api,
    ) is False
    assert _saved_record(core_client) == record
    assert not core_client.pv_patches


@pytest.mark.parametrize(
    ("driver", "hostvol"),
    [
        ("casino.invalid/decoy-csi", POOL_NAME),
        ("kadalu", "mirage-decoy-pool"),
    ],
    ids=["foreign-driver", "different-kadalu-hostvol"],
)
def test_broad_storage_class_attribution_counts_but_never_mutates_foreign_pv(
        monkeypatch, driver, hostvol):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [_persistent_volume(
            driver=driver,
            hostvol=hostvol,
            storage_class_name=f"kadalu.{POOL_NAME}",
            single_pv_per_pool="false",
            reclaim_policy="Delete",
        )],
    )

    assert operator.get_num_pvs(client, record) == 1

    operator.reconcile_pool_pv_reclaim_policy(
        client,
        POOL_NAME,
        "retain",
        before_config_write=True,
    )

    assert not client.pv_patches


def test_storage_class_policy_change_replaces_immutable_object(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(policy="retain")
    obj["metadata"]["uid"] = "uid-bellagio-storage"
    identity = operator.storage_class_identity(obj)
    existing = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            uid="uid-storage-class-bellagio",
            resource_version="117",
            annotations={},
        ),
        provisioner="kadalu",
        parameters=identity["parameters"],
        reclaim_policy="Delete",
        allow_volume_expansion=True,
    )
    storage_api = FakeStorageV1Api([existing])
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    operator.reconcile_storage_class_manifest(
        storage_api,
        "/tmp/bellagio-storageclass.yaml",
        f"kadalu.{POOL_NAME}",
        "Retain",
        identity,
    )

    assert len(storage_api.deletes) == 1
    deleted_name, delete_options = storage_api.deletes[0]
    assert deleted_name == f"kadalu.{POOL_NAME}"
    assert delete_options.preconditions.uid == existing.metadata.uid
    assert delete_options.preconditions.resource_version == "117"
    assert commands == [
        (
            operator.KUBECTL_CMD,
            operator.CREATE_CMD,
            "-f",
            "/tmp/bellagio-storageclass.yaml",
        ),
    ]


def test_absent_storage_class_is_created_atomically(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="bellagio-vault-class")
    obj["metadata"]["uid"] = "uid-bellagio-storage"
    storage_api = FakeStorageV1Api()
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    operator.reconcile_storage_class_manifest(
        storage_api,
        "/tmp/bellagio-storageclass.yaml",
        "bellagio-vault-class",
        "Retain",
        operator.storage_class_identity(obj),
    )

    assert storage_api.deletes == []
    assert commands == [(
        operator.KUBECTL_CMD,
        operator.CREATE_CMD,
        "-f",
        "/tmp/bellagio-storageclass.yaml",
    )]


def test_absent_storage_class_requires_owner_identity(monkeypatch):
    operator = _load_operator(monkeypatch)
    storage_api = FakeStorageV1Api()
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: pytest.fail("unowned Bellagio class was created"),
    )

    with pytest.raises(RuntimeError, match="without owner identity"):
        operator.reconcile_storage_class_manifest(
            storage_api,
            "/tmp/bellagio-storageclass.yaml",
            "bellagio-vault-class",
            "Retain",
        )


def test_current_owned_storage_class_is_not_replaced(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="bellagio-vault-class")
    obj["metadata"]["uid"] = "uid-bellagio-storage"
    obj["spec"]["mount_identity"] = MOUNT_IDENTITY
    storage_class = _owned_storage_class(operator, obj, durable=True)
    storage_api = FakeStorageV1Api([storage_class])
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    operator.reconcile_storage_class_manifest(
        storage_api,
        "/tmp/bellagio-storageclass.yaml",
        "bellagio-vault-class",
        "Delete",
        operator.storage_class_identity(obj),
    )

    assert storage_api.deletes == []
    assert commands == []


def test_storage_class_delete_conflict_prevents_replacement_create(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-storage"
    obj["spec"]["mount_identity"] = MOUNT_IDENTITY
    storage_class = _owned_storage_class(operator, obj, durable=True)

    class ConflictStorageV1Api(FakeStorageV1Api):
        def delete_storage_class(self, name, body):
            self.deletes.append((name, body))
            conflict = RuntimeError("Bellagio class changed")
            conflict.status = 409
            raise conflict

    storage_api = ConflictStorageV1Api([storage_class])
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    with pytest.raises(RuntimeError, match="class changed"):
        operator.reconcile_storage_class_manifest(
            storage_api,
            "/tmp/bellagio-storageclass.yaml",
            f"kadalu.{POOL_NAME}",
            "Retain",
            operator.storage_class_identity(obj),
        )

    assert len(storage_api.deletes) == 1
    assert commands == []


def test_replaced_storage_class_uid_prevents_replacement_create(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-storage"
    obj["spec"]["mount_identity"] = MOUNT_IDENTITY
    storage_class = _owned_storage_class(operator, obj, durable=True)

    class ReplacedStorageV1Api(FakeStorageV1Api):
        def delete_storage_class(self, name, body):
            self.deletes.append((name, body))
            replacement = deepcopy(storage_class)
            replacement.metadata.uid = "uid-storage-class-mirage-decoy"
            replacement.metadata.resource_version = "118"
            self.items = [replacement]

    storage_api = ReplacedStorageV1Api([storage_class])
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    with pytest.raises(RuntimeError, match="replaced during deletion"):
        operator.reconcile_storage_class_manifest(
            storage_api,
            "/tmp/bellagio-storageclass.yaml",
            f"kadalu.{POOL_NAME}",
            "Retain",
            operator.storage_class_identity(obj),
        )

    assert commands == []


def test_foreign_storage_class_is_rejected_before_pool_metadata_write(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-vault"
    storage_class = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            annotations={},
        ),
        provisioner="casino.invalid/decoy-provisioner",
        parameters={
            "storage_name": POOL_NAME,
            "single_pv_per_pool": "False",
        },
        reclaim_policy="Delete",
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    core_client = FakeCoreV1Client({})
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: pytest.fail("foreign class reached server apply"),
    )

    assert operator.handle_added(
        core_client,
        obj,
        storage_api=storage_api,
    ) is False

    assert core_client.patches == []
    assert f"{POOL_NAME}.info" not in core_client.config_map.data


def test_annotated_storage_class_from_other_namespace_is_rejected(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-vault"
    storage_class = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            annotations={
                operator.STORAGE_CLASS_NAMESPACE_ANNOTATION: "mirage",
                operator.STORAGE_CLASS_NAME_ANNOTATION: POOL_NAME,
                operator.STORAGE_CLASS_UID_ANNOTATION: "uid-mirage-vault",
            },
        ),
        provisioner="kadalu",
        parameters={
            "storage_name": POOL_NAME,
            "single_pv_per_pool": "False",
        },
        reclaim_policy="Delete",
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    core_client = FakeCoreV1Client({})

    assert operator.handle_added(
        core_client,
        obj,
        storage_api=storage_api,
    ) is False

    assert core_client.patches == []


def test_exact_legacy_kadalu_storage_class_can_be_annotated_on_upgrade(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-vault"
    legacy = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            annotations={},
        ),
        provisioner="kadalu",
        parameters={
            "storage_name": POOL_NAME,
            "single_pv_per_pool": "False",
        },
        reclaim_policy="Delete",
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[legacy]),
    )

    identity = operator.preflight_storage_class_owner(storage_api, obj)

    assert identity == {
        "namespace": operator.NAMESPACE,
        "name": POOL_NAME,
        "storage_class_name": f"kadalu.{POOL_NAME}",
        "uid": "uid-bellagio-vault",
        "volume_id": HOSTING_VOLUME_ID,
        "mount_identity": "",
        "backend_fingerprint": (
            operator.storage_class_backend_fingerprint(obj)
        ),
        "parameters": {
            "storage_name": POOL_NAME,
            "single_pv_per_pool": "False",
        },
    }


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_storage_class_name_is_immutable_at_pool_record_write_boundary(
        monkeypatch, pool_type):
    operator = _load_operator(monkeypatch)
    if pool_type == "External":
        existing = {
            "volname": POOL_NAME,
            "volume_id": HOSTING_VOLUME_ID,
            "type": "External",
            "single_pv_per_pool": False,
            "gluster_hosts": "bellagio.example.invalid",
            "gluster_volname": "the-benedict-vault",
            "gluster_options": "",
            "storageClassName": "bellagio-vault-class",
        }
    else:
        existing = _stored_native_pool(
            storage_class_name="bellagio-vault-class",
        )
    assert operator.apply_pool_mount_metadata(existing, None) is True
    requested = {
        **existing,
        "storageClassName": "default",
    }

    assert operator.apply_pool_mount_metadata(
        requested,
        json.dumps(existing),
    ) is False
    assert requested["storageClassName"] == "default"


def test_unannotated_custom_storage_class_collision_is_never_adopted(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="default")
    obj["metadata"]["uid"] = "uid-bellagio-custom-class"
    identity = operator.storage_class_identity(obj)
    collision = SimpleNamespace(
        metadata=SimpleNamespace(
            name="default",
            annotations={},
        ),
        provisioner="kadalu",
        parameters=identity["parameters"],
        reclaim_policy="Retain",
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[collision]),
    )

    with pytest.raises(RuntimeError, match="ownership metadata|not owned"):
        operator.preflight_storage_class_owner(storage_api, obj)


def test_custom_name_cannot_create_second_class_for_same_pool(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="default")
    obj["metadata"]["uid"] = "uid-bellagio-custom-class"
    legacy_obj = _native_pool_object()
    legacy_obj["metadata"]["uid"] = obj["metadata"]["uid"]
    existing = _owned_storage_class(
        operator,
        legacy_obj,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[existing]),
    )

    with pytest.raises(RuntimeError, match="different StorageClass"):
        operator.preflight_storage_class_owner(storage_api, obj)


@pytest.mark.parametrize("pool_type", ["Replica1", "External"])
def test_missing_record_custom_name_rejects_unannotated_canonical_class(
        monkeypatch, pool_type):
    operator = _load_operator(monkeypatch)
    if pool_type == "External":
        obj = _external_pool_object(storage_class_name="default")
    else:
        obj = _native_pool_object(storage_class_name="default")
        obj["metadata"]["uid"] = "uid-bellagio-native-custom"
    identity = operator.storage_class_identity(obj)
    canonical_class = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            uid="uid-storage-class-bellagio-legacy",
            resource_version="117",
            annotations={},
        ),
        provisioner="kadalu",
        parameters=identity["parameters"],
        reclaim_policy="Delete",
        allow_volume_expansion=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[canonical_class]),
    )
    core_client = FakeCoreV1Client({})
    monkeypatch.setattr(
        operator,
        "deploy_storage_class",
        lambda *_args: pytest.fail("canonical collision deployed a class"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: pytest.fail("canonical collision deployed a server"),
    )
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: pytest.fail(
            "canonical collision rendered a manifest"
        ),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: pytest.fail("canonical collision mutated Kubernetes"),
    )

    assert operator.handle_added(
        core_client,
        obj,
        storage_api=storage_api,
    ) is False
    assert f"{POOL_NAME}.info" not in core_client.config_map.data
    assert core_client.patches == []
    assert core_client.pv_patches == []


def test_foreign_default_storage_class_collision_precedes_pool_write(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="default")
    obj["metadata"]["uid"] = "uid-bellagio-custom-class"
    collision = SimpleNamespace(
        metadata=SimpleNamespace(name="default", annotations={}),
        provisioner="casino.invalid/decoy-provisioner",
        parameters={
            "storage_name": POOL_NAME,
            "single_pv_per_pool": "False",
        },
        reclaim_policy="Retain",
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[collision]),
    )
    core_client = FakeCoreV1Client({})

    assert operator.handle_added(
        core_client,
        obj,
        storage_api=storage_api,
    ) is False
    assert core_client.patches == []
    assert f"{POOL_NAME}.info" not in core_client.config_map.data


def test_existing_native_record_preflights_durable_class_before_pv_write(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(policy="retain")
    obj["metadata"]["uid"] = "uid-bellagio-native"
    record = {
        "volname": POOL_NAME,
        "type": "Replica1",
        "pvReclaimPolicy": "delete",
        "volume_id": HOSTING_VOLUME_ID,
        "storage_uid": obj["metadata"]["uid"],
        "single_pv_per_pool": False,
        "bricks": [{
            "brick_index": 0,
            "node_id": "node-0",
            "brick_path": f"/bricks/{POOL_NAME}/data/brick",
            "node": f"server-{POOL_NAME}-0-0.{POOL_NAME}",
            "host_brick_path": "/srv/bellagio-vault",
            "kube_hostname": "bellagio-storage-one",
            "brick_device": "",
            "brick_device_dir": "",
            "pvc_name": "",
            "decommissioned": "",
        }],
        "disperse": {"data": 0, "redundancy": 0},
        "options": {},
        "mount_identity": MOUNT_IDENTITY,
    }
    record["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(record)
    )
    storage_class = _owned_storage_class(
        operator,
        obj,
        durable=True,
        mount_identity=OTHER_HOSTING_VOLUME_ID,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [_persistent_volume(
            single_pv_per_pool="false",
            reclaim_policy="Delete",
        )],
    )

    with pytest.raises(RuntimeError, match="durable storage identity"):
        operator.handle_added(client, obj, storage_api=storage_api)

    assert not client.patches
    assert not client.pv_patches


@pytest.mark.parametrize("handler_name", ["handle_added", "handle_modified"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mount_identity", "not-a-bellagio-uuid"),
        ("mount_config_fingerprint", "0" * 64),
    ],
    ids=["mount-identity", "mount-fingerprint"],
)
def test_runtime_reconcile_rejects_tampered_stored_mount_identity(
        monkeypatch, handler_name, field, value):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record.update({
        "volume_id": HOSTING_VOLUME_ID,
        "storage_uid": "uid-bellagio-native",
        "mount_identity": MOUNT_IDENTITY,
    })
    record["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(record)
    )
    record[field] = value
    client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [_persistent_volume(
            single_pv_per_pool="false",
            reclaim_policy="Delete",
        )],
    )
    obj = _native_pool_object(policy="retain")
    obj["metadata"]["uid"] = record["storage_uid"]
    obj["spec"]["storage"][0].update({
        "node": f"{POOL_NAME}-storage-0",
        "path": f"/srv/{POOL_NAME}-0",
    })
    monkeypatch.setattr(
        operator,
        "deploy_storage_class",
        lambda *_args: pytest.fail("tampered record deployed a class"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: pytest.fail("tampered record deployed a server"),
    )

    assert getattr(operator, handler_name)(client, obj) is False
    assert not client.patches
    assert not client.pv_patches


@pytest.mark.parametrize("handler_name", ["handle_added", "handle_modified"])
def test_native_legacy_record_adopts_proven_storage_class_mount_identity(
        monkeypatch, handler_name):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record.update({
        "volume_id": HOSTING_VOLUME_ID,
        "storage_uid": "uid-bellagio-native",
    })
    client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    obj = _native_pool_object()
    obj["metadata"]["uid"] = record["storage_uid"]
    obj["spec"]["storage"][0].update({
        "node": f"{POOL_NAME}-storage-0",
        "path": f"/srv/{POOL_NAME}-0",
    })
    storage_class = _owned_storage_class(
        operator,
        obj,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    monkeypatch.setattr(operator, "deploy_server_pods", lambda *_args: None)

    assert getattr(operator, handler_name)(
        client,
        obj,
        storage_api=storage_api,
    ) is True
    assert _saved_record(client)["mount_identity"] == MOUNT_IDENTITY


def test_external_legacy_record_adopts_proven_class_mount_on_modified(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object()
    record = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "storage_uid": obj["metadata"]["uid"],
        "type": "External",
        "pvReclaimPolicy": "delete",
        "single_pv_per_pool": False,
        "gluster_hosts": (
            "bellagio-one.example.invalid,bellagio-two.example.invalid"
        ),
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "log-level=WARNING",
    }
    client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    storage_class = _owned_storage_class(
        operator,
        obj,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)

    assert operator.handle_modified(
        client,
        obj,
        storage_api=storage_api,
    ) is True
    assert _saved_record(client)["mount_identity"] == MOUNT_IDENTITY


def test_legacy_record_rejects_different_annotated_class_uid_before_write(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record["volume_id"] = HOSTING_VOLUME_ID
    client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-new-bellagio"
    obj["spec"]["storage"][0].update({
        "node": f"{POOL_NAME}-storage-0",
        "path": f"/srv/{POOL_NAME}-0",
    })
    old_obj = {
        "metadata": {
            **obj["metadata"],
            "uid": "uid-old-bellagio",
        },
        "spec": obj["spec"],
    }
    storage_class = _owned_storage_class(
        operator,
        old_obj,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )

    with pytest.raises(RuntimeError, match="owned by another"):
        operator.handle_added(client, obj, storage_api=storage_api)

    assert not client.patches
    assert not client.pv_patches


def test_foreign_storage_class_is_never_deleted(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    foreign = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            annotations={},
        ),
        provisioner="casino.invalid/decoy-provisioner",
        parameters={
            "storage_name": POOL_NAME,
            "single_pv_per_pool": "False",
        },
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[foreign]),
    )
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    with pytest.raises(RuntimeError, match="is not owned"):
        operator.delete_storage_class(
            POOL_NAME,
            "Replica1",
            record,
            storage_api,
        )

    assert commands == []


def test_annotated_storage_class_can_be_deleted_after_record_cleanup(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["metadata"]["uid"] = "uid-bellagio-native"
    storage_class = _owned_storage_class(operator, obj, durable=True)
    storage_api = FakeStorageV1Api([storage_class])
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    operator.delete_storage_class(
        POOL_NAME,
        "Replica1",
        None,
        storage_api,
        expected_storage_uid=obj["metadata"]["uid"],
    )

    assert commands == []
    assert len(storage_api.deletes) == 1
    deleted_name, delete_options = storage_api.deletes[0]
    assert deleted_name == f"kadalu.{POOL_NAME}"
    assert delete_options.preconditions.uid == storage_class.metadata.uid
    assert delete_options.preconditions.resource_version == "117"


def test_custom_storage_class_is_deleted_from_persisted_pool_identity(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="default")
    obj["metadata"]["uid"] = "uid-bellagio-custom-delete"
    core_client = FakeCoreV1Client({})
    assert operator.update_config_map(core_client, obj) is True
    record = _saved_record(core_client)
    storage_class = _owned_storage_class(
        operator,
        obj,
        durable=True,
        volume_id=record["volume_id"],
        mount_identity=record["mount_identity"],
        annotation_overrides={
            operator.STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION: (
                operator.storage_class_backend_fingerprint(record)
            ),
        },
    )
    storage_api = FakeStorageV1Api([storage_class])
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    operator.delete_storage_class(
        POOL_NAME,
        "Replica1",
        record,
        storage_api,
    )

    assert commands == []
    assert len(storage_api.deletes) == 1
    deleted_name, delete_options = storage_api.deletes[0]
    assert deleted_name == "default"
    assert delete_options.preconditions.uid == storage_class.metadata.uid
    assert delete_options.preconditions.resource_version == "117"


def test_interrupted_delete_uses_custom_name_from_cr_tombstone(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object(storage_class_name="default")
    obj["metadata"]["uid"] = "uid-bellagio-custom-delete"
    storage_class = _owned_storage_class(operator, obj, durable=True)
    storage_api = FakeStorageV1Api([storage_class])
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )

    assert operator.handle_deleted(core_client, obj) is True
    assert commands == []
    assert len(storage_api.deletes) == 1
    deleted_name, delete_options = storage_api.deletes[0]
    assert deleted_name == "default"
    assert delete_options.preconditions.uid == storage_class.metadata.uid
    assert delete_options.preconditions.resource_version == "117"


def test_quiesce_waits_for_old_provisioner_to_disappear(monkeypatch):
    operator = _load_operator(monkeypatch)
    pod = SimpleNamespace(metadata=SimpleNamespace(
        name="kadalu-csi-provisioner-0"
    ))
    responses = iter([
        SimpleNamespace(items=[pod]),
        SimpleNamespace(items=[]),
    ])

    class CoreClient:
        def list_namespaced_pod(self, _namespace):
            return next(responses)

    class AppsClient:
        patches = []

        def patch_namespaced_stateful_set(self, name, namespace, body):
            self.patches.append((name, namespace, body))

    apps_client = AppsClient()
    monkeypatch.setattr(operator.time, "sleep", lambda _seconds: None)

    operator.quiesce_csi_provisioner(
        CoreClient(),
        apps_client,
        timeout_seconds=1,
        poll_interval=0,
    )

    assert apps_client.patches == [(
        operator.CSI_PROVISIONER,
        operator.NAMESPACE,
        {"spec": {"replicas": 0}},
    )]


def test_nodeplugin_rollout_requires_metadata_generation(monkeypatch):
    operator = _load_operator(monkeypatch)

    assert operator.csi_nodeplugin_rollout_complete(
        _nodeplugin_daemonset(generation=None)
    ) is False


def test_deploy_csi_rejects_multiple_serving_controllers(monkeypatch):
    operator = _load_operator(monkeypatch)

    with pytest.raises(ValueError, match="zero .* or one"):
        operator.deploy_csi_pods(object(), provisioner_replicas=2)


def test_deploy_csi_passes_configured_busybox_image(monkeypatch, tmp_path):
    operator = _load_operator(monkeypatch)
    rendered = []
    busybox_image = "registry.example.invalid/helpers/busybox:test"
    core_client = SimpleNamespace(
        list_namespaced_pod=lambda _namespace: SimpleNamespace(items=[]),
    )

    monkeypatch.setattr(operator, "MANIFESTS_DIR", str(tmp_path))
    monkeypatch.setattr(operator, "BUSYBOX_IMAGE", busybox_image)
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **values: rendered.append((filename, values)),
    )
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)

    operator.deploy_csi_pods(core_client)

    csi_values = next(
        values
        for filename, values in rendered
        if filename.endswith("/csi.yaml")
    )
    assert csi_values["busybox_image"] == busybox_image


def test_csi_upgrade_keeps_provisioner_fenced_after_nodeplugin_rollout(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    storage_plan = _storage_plan(tolerations=[{
        "key": "bellagio-storage",
        "operator": "Exists",
    }])
    config_map = SimpleNamespace(data={
        f"{POOL_NAME}.info": json.dumps(_legacy_pool_record()),
    })
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: config_map,
        list_persistent_volume=lambda: SimpleNamespace(items=[]),
    )
    daemon_sets = iter([
        _nodeplugin_daemonset(current=False),
        _nodeplugin_daemonset(current=True),
    ])
    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda *_args: next(daemon_sets),
    )
    storage_client = object()
    monkeypatch.setattr(
        operator, "list_storage_resources", lambda _client: object()
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: storage_plan,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_tolerations",
        lambda *_args: calls.append("reconcile-tolerations"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce-provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: calls.append("migrate"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_csi_nodeplugin_rollout",
        lambda received: calls.append("wait-nodeplugin")
        if received is apps_client else None,
    )
    monkeypatch.setattr(operator.time, "sleep", lambda _seconds: None)

    result = operator.deploy_csi_upgrade(
        core_client,
        apps_client,
        storage_client,
    )

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
        "migrate",
        "reconcile-tolerations",
        "wait-nodeplugin",
    ]
    assert result is storage_plan


def test_csi_upgrade_regates_changed_union_and_returns_fresh_plan(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    first_tolerations = [{"key": "bellagio", "operator": "Exists"}]
    changed_tolerations = [{"key": "mirage", "operator": "Exists"}]
    first_plan = _storage_plan(
        tolerations=first_tolerations,
        resource_version="7",
    )
    changed_plan = _storage_plan(
        tolerations=changed_tolerations,
        resource_version="8",
    )
    fresh_plan = _storage_plan(
        tolerations=changed_tolerations,
        resource_version="9",
    )
    plans = iter([first_plan, changed_plan, fresh_plan])
    calls = []
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
        list_persistent_volume=lambda: SimpleNamespace(items=[]),
    )
    apps_client = object()
    storage_client = object()
    monkeypatch.setattr(
        operator,
        "list_storage_resources",
        lambda _client: object(),
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: next(plans),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1,
        nodeplugin_tolerations=None: calls.append((
            "deploy-csi",
            provisioner_replicas,
            nodeplugin_tolerations,
        )),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: calls.append("migrate"),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_tolerations",
        lambda _apps, tolerations: calls.append(("patch", tolerations)),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_csi_nodeplugin_rollout",
        lambda _apps: calls.append("wait"),
    )

    result = operator.deploy_csi_upgrade(
        core_client,
        apps_client,
        storage_client,
    )

    assert result is fresh_plan
    assert calls == [
        "quiesce",
        ("deploy-csi", 0, changed_tolerations),
        "migrate",
        ("patch", changed_tolerations),
        "wait",
    ]


def test_csi_upgrade_heals_fully_serving_pool_before_provisioner_fence(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]
    storage_plan = _storage_plan(upgrade_objects=[obj])
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(),
    )
    storage_client = object()
    events = []
    expected_pods = tuple(
        f"server-{POOL_NAME}-{index}-0" for index in range(3)
    )
    monkeypatch.setattr(
        operator,
        "list_storage_resources",
        lambda _storage: object(),
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: storage_plan,
    )
    monkeypatch.setattr(
        operator,
        "wait_for_storage_heal",
        lambda volname, pods: events.append(
            ("heal", volname, tuple(pods))
        ),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: events.append("fence"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda *_args, **_kwargs: events.append("publish-csi"),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: events.append("record-mount-identity"),
    )
    monkeypatch.setattr(
        operator,
        "gate_nodeplugin_until_current",
        lambda *_args: events.append("gate-nodeplugin") or storage_plan,
    )

    result = operator.deploy_csi_upgrade(
        core_client,
        apps_client,
        storage_client,
    )

    assert result is storage_plan
    assert events == [
        ("heal", POOL_NAME, expected_pods),
        "fence",
        "publish-csi",
        "record-mount-identity",
        "gate-nodeplugin",
    ]


def test_dirty_prefence_heal_aborts_without_fence_or_server_mutation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"mirage-storage-{index}",
            "path": f"/srv/mirage-vault-{index}",
        }
        for index in range(3)
    ]
    storage_plan = _storage_plan(upgrade_objects=[obj])
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(),
    )
    events = []
    monkeypatch.setattr(
        operator,
        "list_storage_resources",
        lambda _storage: object(),
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: storage_plan,
    )

    def dirty_heal(_volname, _pods):
        events.append("dirty-heal")
        raise TimeoutError("Mirage vault heal stayed dirty")

    monkeypatch.setattr(operator, "wait_for_storage_heal", dirty_heal)
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: pytest.fail("dirty pool reached provisioner fence"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda *_args, **_kwargs: pytest.fail("dirty pool published CSI"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: pytest.fail("dirty pool mutated a server"),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: pytest.fail("dirty pool migrated metadata"),
    )

    with pytest.raises(TimeoutError, match="Mirage vault heal stayed dirty"):
        operator.deploy_csi_upgrade(core_client, apps_client, object())

    assert events == ["dirty-heal"]


def test_invalid_storage_is_quarantined_without_poisoning_plan(monkeypatch):
    operator = _load_operator(monkeypatch)
    invalid = {
        "metadata": {"name": "empty-bellagio-vault"},
        "spec": {"type": "Replica3", "storage": []},
    }
    valid = _storage_custom_object(name="valid-mirage-vault")

    plan = operator.prepare_storage_upgrade(
        FakeCoreV1Client({}),
        object(),
        {
            "items": [invalid, valid],
            "metadata": {"resourceVersion": "9-invalid"},
        },
    )

    assert plan["invalid_items"] == [invalid]
    assert plan["reconcile_items"] == [valid]
    assert not plan["deletion_orphans"]


@pytest.mark.parametrize(
    "invalid_spec",
    [None, "the-benedict-job", []],
    ids=["missing", "string", "list"],
)
def test_missing_or_non_object_spec_is_quarantined_without_poisoning_plan(
        monkeypatch, invalid_spec):
    operator = _load_operator(monkeypatch)
    invalid = {
        "metadata": {"name": "invalid-bellagio-vault"},
    }
    if invalid_spec is not None:
        invalid["spec"] = invalid_spec
    valid = _storage_custom_object(name="valid-mirage-vault")

    plan = operator.prepare_storage_upgrade(
        FakeCoreV1Client({}),
        object(),
        {
            "items": [invalid, valid],
            "metadata": {"resourceVersion": "9-invalid-spec"},
        },
    )

    assert plan["invalid_items"] == [invalid]
    assert plan["reconcile_items"] == [valid]
    assert not plan["deletion_orphans"]


def test_invalid_existing_pool_is_quarantined_not_deleted(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    invalid = {
        "metadata": {"name": POOL_NAME},
        "spec": {"type": "Replica3", "storage": []},
    }

    plan = operator.prepare_storage_upgrade(
        FakeCoreV1Client({f"{POOL_NAME}.info": record}),
        SimpleNamespace(
            read_namespaced_stateful_set=lambda *_args: _server_statefulset(
                tolerations=[],
            ),
        ),
        {
            "items": [invalid],
            "metadata": {"resourceVersion": "10-invalid-existing"},
        },
    )

    assert plan["invalid_items"] == [invalid]
    assert not plan["reconcile_items"]
    assert not plan["upgrade_objects"]
    assert not plan["deletion_orphans"]


def test_legacy_block_pv_aborts_before_mutation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": _legacy_pool_record()})
    core_client.persistent_volumes = [
        _persistent_volume(pvtype="rawblock")
    ]
    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda *_args: _nodeplugin_daemonset(
            current=False
        ),
    )
    storage_client = object()
    monkeypatch.setattr(
        operator, "list_storage_resources", lambda _client: object()
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: _storage_plan(),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce-provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: calls.append("migrate"),
    )

    with pytest.raises(RuntimeError, match="PersistentVolumes still exist"):
        operator.deploy_csi_upgrade(core_client, apps_client, storage_client)

    assert calls == []
    assert "mount_identity" not in _saved_record(core_client)
    assert not core_client.patches


def test_nodeplugin_rollout_timeout_keeps_provisioner_at_zero(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    config_map = SimpleNamespace(data={
        f"{POOL_NAME}.info": json.dumps(_legacy_pool_record()),
    })
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: config_map,
        list_persistent_volume=lambda: SimpleNamespace(items=[]),
    )
    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda *_args: _nodeplugin_daemonset(
            current=False
        ),
    )
    storage_client = object()
    monkeypatch.setattr(
        operator, "list_storage_resources", lambda _client: object()
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: _storage_plan(),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_tolerations",
        lambda *_args: calls.append("reconcile-tolerations"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce-provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: calls.append("migrate"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monotonic_values = iter([
        0,
        operator.CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS + 1,
    ])
    monkeypatch.setattr(
        operator.time,
        "monotonic",
        lambda: next(monotonic_values),
    )

    with pytest.raises(TimeoutError, match="drain or unpublish"):
        operator.deploy_csi_upgrade(core_client, apps_client, storage_client)

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
        "migrate",
        "reconcile-tolerations",
    ]


def test_post_migration_block_pv_allows_nodeplugin_rollout(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    config_map = SimpleNamespace(data={
        f"{POOL_NAME}.info": json.dumps({
            "mount_identity": MOUNT_IDENTITY,
        }),
    })
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: config_map,
        list_persistent_volume=lambda: SimpleNamespace(items=[
            _persistent_volume(pvtype="virtblock"),
        ]),
    )
    daemon_sets = iter([
        _nodeplugin_daemonset(current=False),
        _nodeplugin_daemonset(current=True),
    ])
    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda *_args: next(daemon_sets),
    )
    storage_client = object()
    monkeypatch.setattr(
        operator, "list_storage_resources", lambda _client: object()
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: _storage_plan(),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_tolerations",
        lambda *_args: calls.append("reconcile-tolerations"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce-provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: calls.append("migrate"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )

    operator.deploy_csi_upgrade(core_client, apps_client, storage_client)

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
        "migrate",
        "reconcile-tolerations",
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        {"generation": None},
        {"observed_generation": None},
        {"observed_generation": 3},
        {"replicas": 0},
        {"current_replicas": 0},
        {"updated_replicas": 0},
        {"ready_replicas": 0},
        {"available_replicas": 0},
        {"current_revision": "bellagio-old"},
        {"update_revision": None},
        {"desired_replicas": 2},
    ],
)
def test_server_rollout_requires_current_singleton_to_be_available(
        monkeypatch, overrides):
    operator = _load_operator(monkeypatch)

    assert operator.server_statefulset_rollout_complete(
        _server_statefulset(**overrides)
    ) is False


def test_server_rollout_accepts_observed_current_available_singleton(
        monkeypatch):
    operator = _load_operator(monkeypatch)

    assert operator.server_statefulset_rollout_complete(
        _server_statefulset()
    ) is True


def test_server_rollout_timeout_is_configurable(monkeypatch):
    monkeypatch.setenv("KADALU_SERVER_ROLLOUT_TIMEOUT_SECONDS", "1234")
    operator = _load_operator(monkeypatch)

    assert operator.SERVER_ROLLOUT_TIMEOUT_SECONDS == 1234


def test_csi_provisioner_rollout_waits_for_current_available_singleton(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    responses = iter([
        _server_statefulset(ready_replicas=0),
        _server_statefulset(),
    ])
    reads = []
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda name, namespace: (
            reads.append((name, namespace)) or next(responses)
        ),
    )
    sleeps = []
    monkeypatch.setattr(
        operator.time,
        "sleep",
        lambda seconds: sleeps.append(seconds),
    )

    operator.wait_for_csi_provisioner_rollout(
        apps_client,
        timeout_seconds=30,
        poll_interval=0.25,
    )

    assert reads == [
        (operator.CSI_PROVISIONER, operator.NAMESPACE),
        (operator.CSI_PROVISIONER, operator.NAMESPACE),
    ]
    assert sleeps == [0.25]


def test_csi_provisioner_rollout_times_out_when_revision_is_not_current(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda _name, _namespace: (
            _server_statefulset(current_revision="bellagio-old")
        ),
    )
    times = iter([10.0, 11.0])
    monkeypatch.setattr(operator.time, "monotonic", lambda: next(times))

    with pytest.raises(TimeoutError, match="readiness remains false"):
        operator.wait_for_csi_provisioner_rollout(
            apps_client,
            timeout_seconds=1,
            poll_interval=0,
        )


def test_csi_provisioner_rollout_timeout_is_configurable(monkeypatch):
    monkeypatch.setenv(
        "KADALU_CSI_PROVISIONER_ROLLOUT_TIMEOUT_SECONDS",
        "321",
    )
    operator = _load_operator(monkeypatch)

    assert operator.CSI_PROVISIONER_ROLLOUT_TIMEOUT_SECONDS == 321


def test_csi_nodeplugin_rollout_timeout_has_safe_default(monkeypatch):
    monkeypatch.delenv(
        "KADALU_CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS",
        raising=False,
    )
    operator = _load_operator(monkeypatch)

    assert operator.CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS == 3600


def test_csi_nodeplugin_rollout_timeout_is_configurable(monkeypatch):
    monkeypatch.setenv(
        "KADALU_CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS",
        "4321",
    )
    operator = _load_operator(monkeypatch)

    assert operator.CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS == 4321


def test_storage_upgrade_passes_rollout_client_to_server_deployment(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    apps_client = object()
    record = _stored_native_pool(pool_type="Replica3", bricks=3)
    record["volume_id"] = HOSTING_VOLUME_ID
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={
            f"{POOL_NAME}.info": json.dumps(record),
        }),
    )
    storage_client = SimpleNamespace(
        list_namespaced_custom_object=lambda *_args, **_kwargs: {
            "items": [_storage_custom_object(pool_type="Replica3")],
            "metadata": {"resourceVersion": "71"},
        },
    )
    deployments = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda obj, apps: deployments.append((obj, apps)),
    )

    operator.upgrade_storage_pods(
        core_client,
        apps_client,
        storage_client,
    )

    assert len(deployments) == 1
    assert deployments[0][0]["metadata"]["name"] == POOL_NAME
    assert deployments[0][1] is apps_client


def test_storage_upgrade_renders_legacy_pool_with_live_cr_tolerations(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    tolerations = [{
        "key": "casino-security",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    record = _stored_native_pool(pool_type="Replica3", bricks=3)
    record["volume_id"] = HOSTING_VOLUME_ID
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={
            f"{POOL_NAME}.info": json.dumps(record),
        }),
    )
    storage_client = SimpleNamespace(
        list_namespaced_custom_object=lambda *_args, **_kwargs: {
            "items": [_storage_custom_object(
                pool_type="Replica3",
                tolerations=tolerations,
            )],
            "metadata": {"resourceVersion": "72"},
        },
    )
    events = []
    rendered = []

    def render(_filename, **values):
        rendered.append(dict(values))
        events.append(("render", values["serverpod_name"]))

    def execute(*_command):
        events.append(("apply", rendered[-1]["serverpod_name"]))

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(name, _namespace):
            events.append(("wait", name))
            return _server_statefulset()

    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(operator, "lib_execute", execute)
    monkeypatch.setattr(operator, "add_tolerations", lambda *_args: None)
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda volname: events.append(("service", volname)),
    )
    monkeypatch.setattr(
        operator,
        "existing_server_statefulset_names",
        lambda _apps, names: set(names),
    )
    monkeypatch.setattr(
        operator,
        "unavailable_server_statefulset_names",
        lambda _apps, _names: set(),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_storage_heal",
        lambda volname, _names: events.append(("heal", volname)),
    )

    operator.upgrade_storage_pods(
        core_client,
        AppsClient(),
        storage_client,
    )

    assert [values["tolerations"] for values in rendered] == [
        tolerations,
        tolerations,
        tolerations,
    ]
    assert events == [("service", POOL_NAME), ("heal", POOL_NAME)] + [
        event
        for index in range(3)
        for event in (
            ("render", f"server-{POOL_NAME}-{index}"),
            ("apply", f"server-{POOL_NAME}-{index}"),
            ("wait", f"server-{POOL_NAME}-{index}"),
            ("heal", POOL_NAME),
        )
    ]


@pytest.mark.parametrize(
    "storage_items",
    [
        [],
        [_storage_custom_object(deleting=True)],
    ],
    ids=["missing", "deleting"],
)
def test_storage_upgrade_leaves_deletion_orphan_servers_untouched(
        monkeypatch, storage_items):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    if storage_items:
        deletion_uid = "uid-bellagio-deleting"
        record["storage_uid"] = deletion_uid
        storage_items[0]["metadata"]["uid"] = deletion_uid
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={
            f"{POOL_NAME}.info": json.dumps(record),
        }),
    )
    storage_client = SimpleNamespace(
        list_namespaced_custom_object=lambda *_args, **_kwargs: {
            "items": storage_items,
            "metadata": {"resourceVersion": "73"},
        },
    )
    tolerations = [{
        "key": "bellagio-orphan",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=tolerations
        ),
    )
    deployments = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *args: deployments.append(args),
    )

    operator.upgrade_storage_pods(
        core_client,
        apps_client,
        storage_client,
    )

    assert not deployments


def test_storage_upgrade_rejects_active_type_mismatch_before_server_apply(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    core_client = FakeCoreV1Client({
        f"{POOL_NAME}.info": _stored_native_pool(),
    })
    storage_client = SimpleNamespace(
        list_namespaced_custom_object=lambda *_args, **_kwargs: {
            "items": [_storage_custom_object(pool_type="Replica3")],
            "metadata": {"resourceVersion": "74"},
        },
    )
    deployments = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *args: deployments.append(args),
    )

    with pytest.raises(RuntimeError, match=POOL_NAME):
        operator.upgrade_storage_pods(
            core_client,
            object(),
            storage_client,
        )

    assert deployments == []


def test_storage_upgrade_preflights_every_pool_before_first_server_apply(
        monkeypatch):
    operator = _load_operator(monkeypatch)

    def record(name):
        return _stored_native_pool(name)

    missing_pool = "mirage-pool"
    invalid_record = record(missing_pool)
    invalid_record["bricks"][0]["brick_index"] = 1
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={
            f"{POOL_NAME}.info": json.dumps(record(POOL_NAME)),
            f"{missing_pool}.info": json.dumps(invalid_record),
        }),
    )
    storage_client = SimpleNamespace(
        list_namespaced_custom_object=lambda *_args, **_kwargs: {
            "items": [_storage_custom_object()],
            "metadata": {"resourceVersion": "75"},
        },
    )
    deployments = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *args: deployments.append(args),
    )

    with pytest.raises(RuntimeError, match=missing_pool):
        operator.upgrade_storage_pods(
            core_client,
            object(),
            storage_client,
        )

    assert deployments == []


def test_server_bricks_are_applied_and_waited_one_at_a_time(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    rendered_name = None

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]
        events.append(("render", rendered_name))

    def execute(*_command):
        events.append(("apply", rendered_name))

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(name, _namespace):
            events.append(("wait", name))
            return _server_statefulset()

    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica1"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]
    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(operator, "lib_execute", execute)
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda _volname: None,
    )

    operator.deploy_server_pods(obj, AppsClient())

    assert events == [
        ("render", f"server-{POOL_NAME}-0"),
        ("apply", f"server-{POOL_NAME}-0"),
        ("wait", f"server-{POOL_NAME}-0"),
        ("render", f"server-{POOL_NAME}-1"),
        ("apply", f"server-{POOL_NAME}-1"),
        ("wait", f"server-{POOL_NAME}-1"),
        ("render", f"server-{POOL_NAME}-2"),
        ("apply", f"server-{POOL_NAME}-2"),
        ("wait", f"server-{POOL_NAME}-2"),
    ]


def test_replicated_server_rollout_is_heal_gated_between_bricks(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    rendered_name = None

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]
        events.append(("render", rendered_name))

    def execute(*_command):
        events.append(("apply", rendered_name))

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(name, _namespace):
            events.append(("ready", name))
            return _server_statefulset()

    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]
    expected_statefulsets = {
        f"server-{POOL_NAME}-{index}" for index in range(3)
    }
    expected_pods = tuple(
        f"server-{POOL_NAME}-{index}-0" for index in range(3)
    )
    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(operator, "lib_execute", execute)
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda volname: events.append(("service", volname)),
    )
    monkeypatch.setattr(
        operator,
        "existing_server_statefulset_names",
        lambda _apps, names: expected_statefulsets & set(names),
    )
    monkeypatch.setattr(
        operator,
        "unavailable_server_statefulset_names",
        lambda _apps, _names: set(),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_storage_heal",
        lambda volname, names: events.append(
            ("heal", volname, tuple(names))
        ),
    )

    operator.deploy_server_pods(obj, AppsClient())

    heal_event = ("heal", POOL_NAME, expected_pods)
    assert events == [("service", POOL_NAME), heal_event] + [
        event
        for index in range(3)
        for event in (
            ("render", f"server-{POOL_NAME}-{index}"),
            ("apply", f"server-{POOL_NAME}-{index}"),
            ("ready", f"server-{POOL_NAME}-{index}"),
            heal_event,
        )
    ]


def test_unavailable_and_missing_servers_recover_before_heal_and_rotation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    rendered_name = None
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]

    class MissingStatefulSet(Exception):
        status = 404

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(name, _namespace):
            if name == f"server-{POOL_NAME}-2":
                raise MissingStatefulSet("Basher's server is missing")
            if name == f"server-{POOL_NAME}-1":
                return _server_statefulset(available_replicas=0)
            return _server_statefulset()

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]

    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: events.append(("apply", rendered_name)),
    )
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda volname: events.append(("service", volname)),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_server_statefulset_rollout",
        lambda _apps, name: events.append(("ready", name)),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_storage_heal",
        lambda volname, pods: events.append(
            ("heal", volname, tuple(pods))
        ),
    )

    operator.deploy_server_pods(obj, AppsClient())

    heal_event = (
        "heal",
        POOL_NAME,
        tuple(f"server-{POOL_NAME}-{index}-0" for index in range(3)),
    )
    assert events == [
        ("service", POOL_NAME),
        ("apply", f"server-{POOL_NAME}-1"),
        ("ready", f"server-{POOL_NAME}-1"),
        ("apply", f"server-{POOL_NAME}-2"),
        ("ready", f"server-{POOL_NAME}-2"),
        heal_event,
        ("apply", f"server-{POOL_NAME}-0"),
        ("ready", f"server-{POOL_NAME}-0"),
        heal_event,
    ]


def test_replicated_server_reconciliation_requires_apps_client(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = obj["spec"]["storage"] * 3
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda _name: pytest.fail("unsafe reconciliation mutated Service"),
    )

    with pytest.raises(RuntimeError, match="Apps client"):
        operator.deploy_server_pods(obj)


def test_partial_replicated_pool_creates_missing_before_gated_rollout(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    rendered_name = None
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]

    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: events.append(("apply", rendered_name)),
    )
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda _name: events.append(("service", POOL_NAME)),
    )
    monkeypatch.setattr(
        operator,
        "existing_server_statefulset_names",
        lambda _apps, _names: {
            f"server-{POOL_NAME}-1",
            f"server-{POOL_NAME}-2",
        },
    )
    monkeypatch.setattr(
        operator,
        "unavailable_server_statefulset_names",
        lambda _apps, _names: set(),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_server_statefulset_rollout",
        lambda _apps, name: events.append(("ready", name)),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_storage_heal",
        lambda _name, _pods: events.append(("heal", POOL_NAME)),
    )

    operator.deploy_server_pods(obj, object())

    assert events == [
        ("service", POOL_NAME),
        ("apply", f"server-{POOL_NAME}-0"),
        ("ready", f"server-{POOL_NAME}-0"),
        ("heal", POOL_NAME),
        ("apply", f"server-{POOL_NAME}-1"),
        ("ready", f"server-{POOL_NAME}-1"),
        ("heal", POOL_NAME),
        ("apply", f"server-{POOL_NAME}-2"),
        ("ready", f"server-{POOL_NAME}-2"),
        ("heal", POOL_NAME),
    ]


def test_dirty_post_rollout_heal_stops_before_next_brick(monkeypatch):
    operator = _load_operator(monkeypatch)
    applied = []
    rendered_name = None
    heal_checks = 0
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]

    def heal(_name, _pods):
        nonlocal heal_checks
        heal_checks += 1
        if heal_checks == 2:
            raise TimeoutError("Mirage heal remained dirty")

    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: applied.append(rendered_name),
    )
    monkeypatch.setattr(operator, "deploy_storage_service", lambda _name: None)
    monkeypatch.setattr(
        operator,
        "existing_server_statefulset_names",
        lambda _apps, names: set(names),
    )
    monkeypatch.setattr(
        operator,
        "unavailable_server_statefulset_names",
        lambda _apps, _names: set(),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_server_statefulset_rollout",
        lambda *_args: None,
    )
    monkeypatch.setattr(operator, "wait_for_storage_heal", heal)

    with pytest.raises(TimeoutError, match="remained dirty"):
        operator.deploy_server_pods(obj, object())

    assert heal_checks == 2
    assert applied == [f"server-{POOL_NAME}-0"]


def test_retry_reclassifies_recovered_brick_before_advancing(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        f"server-{POOL_NAME}-1",
        f"server-{POOL_NAME}-2",
    }
    applied = []
    rendered_name = None
    heal_checks = 0
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica3"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]

    def apply(*_args):
        applied.append(rendered_name)
        existing.add(rendered_name)

    def heal(_name, _pods):
        nonlocal heal_checks
        heal_checks += 1
        if heal_checks in (1, 3):
            raise TimeoutError("heal retry boundary")

    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(operator, "lib_execute", apply)
    monkeypatch.setattr(operator, "deploy_storage_service", lambda _name: None)
    monkeypatch.setattr(
        operator,
        "existing_server_statefulset_names",
        lambda _apps, names: existing & set(names),
    )
    monkeypatch.setattr(
        operator,
        "unavailable_server_statefulset_names",
        lambda _apps, _names: set(),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_server_statefulset_rollout",
        lambda *_args: None,
    )
    monkeypatch.setattr(operator, "wait_for_storage_heal", heal)

    with pytest.raises(TimeoutError, match="retry boundary"):
        operator.deploy_server_pods(obj, object())
    with pytest.raises(TimeoutError, match="retry boundary"):
        operator.deploy_server_pods(obj, object())

    # The recovered brick is now pre-existing. Retry gates the full pool, then
    # reapplies and re-gates brick zero instead of advancing to brick one.
    assert applied == [
        f"server-{POOL_NAME}-0",
        f"server-{POOL_NAME}-0",
    ]


def test_heal_summary_requires_every_brick_connected_and_clean(monkeypatch):
    operator = _load_operator(monkeypatch)

    def brick(status="Connected", total=0, pending=0, split=0, healing=0):
        return "\n".join((
            f"Status: {status}",
            f"Total Number of entries: {total}",
            f"Number of entries in heal pending: {pending}",
            f"Number of entries in split-brain: {split}",
            f"Number of entries possibly healing: {healing}",
        ))

    clean = "\n\n".join(brick() for _index in range(3))
    assert operator.storage_heal_summary_is_clean(clean, 3) is True
    assert operator.storage_heal_summary_is_clean(
        "\n\n".join((brick(), brick(total=1), brick())),
        3,
    ) is False
    assert operator.storage_heal_summary_is_clean(
        "\n\n".join((brick(), brick(status="Disconnected"), brick())),
        3,
    ) is False
    assert operator.storage_heal_summary_is_clean(clean, 4) is False


def test_arbiter_servers_start_self_heal_daemon(monkeypatch):
    operator = _load_operator(monkeypatch)
    rendered = []
    obj = _native_pool_object()
    obj["spec"]["type"] = "Arbiter"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(3)
    ]
    monkeypatch.setattr(operator, "deploy_storage_service", lambda _name: None)
    monkeypatch.setattr(
        operator,
        "template",
        lambda _filename, **values: rendered.append(values),
    )
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    monkeypatch.setattr(
        operator,
        "existing_server_statefulset_names",
        lambda _apps, _names: set(),
    )
    monkeypatch.setattr(
        operator,
        "wait_for_server_statefulset_rollout",
        lambda *_args: None,
    )
    monkeypatch.setattr(operator, "wait_for_storage_heal", lambda *_args: None)

    operator.deploy_server_pods(obj, object())

    assert len(rendered) == 3
    assert all(values["shd_required"] is True for values in rendered)


def test_heal_gate_execs_glfsheal_with_remaining_deadline(monkeypatch):
    operator = _load_operator(monkeypatch)
    commands = []
    brick = "\n".join((
        "Status: Connected",
        "Total Number of entries: 0",
        "Number of entries in heal pending: 0",
        "Number of entries in split-brain: 0",
        "Number of entries possibly healing: 0",
    ))
    summary = "\n\n".join((brick, brick, brick))
    times = iter((100, 100))
    monkeypatch.setattr(operator.time, "monotonic", lambda: next(times))

    def execute(*args, **kwargs):
        commands.append((args, kwargs))
        return summary, "", 117

    monkeypatch.setattr(operator, "lib_execute", execute)

    operator.wait_for_storage_heal(
        POOL_NAME,
        [f"server-{POOL_NAME}-{index}-0" for index in range(3)],
        timeout_seconds=900,
    )

    assert len(commands) == 1
    args, kwargs = commands[0]
    assert args == (
        operator.KUBECTL_CMD,
        "-n",
        operator.NAMESPACE,
        "exec",
        f"server-{POOL_NAME}-0-0",
        "-c",
        "server",
        "--",
        "/opt/libexec/glusterfs/glfsheal",
        POOL_NAME,
        "info-summary",
        "volfile-path",
        f"/var/lib/kadalu/volfiles/{POOL_NAME}.vol",
    )
    assert kwargs == {"timeout": 900}


def test_server_rollout_timeout_does_not_apply_the_next_brick(monkeypatch):
    operator = _load_operator(monkeypatch)
    applied = []
    rendered_name = None

    def render(_filename, **values):
        nonlocal rendered_name
        rendered_name = values["serverpod_name"]

    def execute(*_command):
        applied.append(rendered_name)

    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            available_replicas=0
        ),
    )
    obj = _native_pool_object()
    obj["spec"]["type"] = "Replica1"
    obj["spec"]["storage"] = [
        {
            "node_id": f"node-{index}",
            "node": f"bellagio-storage-{index}",
            "path": f"/srv/bellagio-vault-{index}",
        }
        for index in range(2)
    ]
    monotonic_values = iter([
        0,
        operator.SERVER_ROLLOUT_TIMEOUT_SECONDS + 1,
    ])
    monkeypatch.setattr(operator, "template", render)
    monkeypatch.setattr(operator, "lib_execute", execute)
    monkeypatch.setattr(
        operator,
        "deploy_storage_service",
        lambda _volname: None,
    )
    monkeypatch.setattr(operator.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        operator.time,
        "monotonic",
        lambda: next(monotonic_values),
    )

    with pytest.raises(TimeoutError, match=f"server-{POOL_NAME}-0"):
        operator.deploy_server_pods(obj, apps_client)

    assert applied == [f"server-{POOL_NAME}-0"]


def test_modified_pool_reconciliation_passes_rollout_client(monkeypatch):
    operator = _load_operator(monkeypatch)
    apps_client = object()
    record = _stored_native_pool()
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={
            f"{POOL_NAME}.info": json.dumps(record),
        }),
    )
    deployments = []
    monkeypatch.setattr(operator, "update_config_map", lambda *_args: True)
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda obj, apps: deployments.append((obj, apps)),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    obj = _native_pool_object()
    obj["spec"]["storage"][0].update({
        "node": f"{POOL_NAME}-storage-0",
        "path": f"/srv/{POOL_NAME}-0",
    })

    operator.handle_modified(core_client, obj, apps_client)

    assert len(deployments) == 1
    assert deployments[0][0] is obj
    assert deployments[0][1] is apps_client


def test_modified_missing_pool_passes_rollout_client_to_added_handler(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    apps_client = object()
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )
    additions = []
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda core, obj, apps, _storage_api=None,
        _provisioner_fenced=False, _storage_client=None: additions.append(
            (core, obj, apps)
        ),
    )
    obj = _native_pool_object()

    operator.handle_modified(core_client, obj, apps_client)

    assert additions == [(core_client, obj, apps_client)]


def test_watch_stream_passes_rollout_client_to_initial_and_first_event_handler(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    core_client = object()
    k8s_client = object()
    apps_client = object()
    initial = _storage_custom_object(
        name="bellagio-vault",
        pool_type="Replica3",
    )
    initial["metadata"]["resourceVersion"] = "7"
    added = _storage_custom_object(name="mirage-vault")
    added["metadata"]["resourceVersion"] = "8"
    modified = _storage_custom_object(
        name="bank-vault",
        pool_type="Replica3",
    )
    modified["metadata"]["resourceVersion"] = "9"
    deleted = {
        "metadata": {"name": "mirage-vault", "resourceVersion": "10"},
        "spec": {"type": "Replica1"},
    }

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            return {
                "items": [initial],
                "metadata": {"resourceVersion": "7"},
            }

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(method, *_args, **kwargs):
            assert method == custom_objects.list_namespaced_custom_object
            assert kwargs["resource_version"] == "7"
            return iter([
                {"type": "ADDED", "object": added},
                {"type": "MODIFIED", "object": modified},
                {"type": "DELETED", "object": deleted},
            ])

    calls = []
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: custom_objects if received is k8s_client else None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.watch,
        "Watch",
        FakeWatch,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "event_targets_deletion_blocked_record",
        lambda _core, _obj: False,
    )
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda core, obj, apps, **_kwargs: (
            calls.append(("added", core, obj, apps)) or True
        ),
    )
    monkeypatch.setattr(
        operator,
        "handle_modified",
        lambda core, obj, apps, **_kwargs: (
            calls.append(("modified", core, obj, apps)) or True
        ),
    )
    monkeypatch.setattr(
        operator,
        "handle_deleted",
        lambda core, obj, **kwargs: (
            calls.append(("deleted", core, obj, kwargs)) or True
        ),
    )

    resource_versions = iter(["7", "8", "9", "10"])

    def reconcile_union(core, apps, storage, storage_list=None):
        calls.append(("union", core, apps, storage, storage_list))
        return _storage_plan(
            items=initial_list["items"] if storage_list else [],
        ) | {"resource_version": next(resource_versions)}

    initial_list = {
        "items": [initial],
        "metadata": {"resourceVersion": "7"},
    }
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        reconcile_union,
        raising=False,
    )

    assert operator.watch_stream(
        core_client,
        k8s_client,
        apps_client,
    ) == "8"

    assert calls == [
        ("union", core_client, apps_client, custom_objects, initial_list),
        ("added", core_client, initial, apps_client),
        ("added", core_client, added, apps_client),
        ("union", core_client, apps_client, custom_objects, None),
    ]


@pytest.mark.parametrize("operation", ["ADDED", "MODIFIED", "DELETED"])
def test_watch_stream_quarantines_cleanup_only_pool_events(
        monkeypatch, operation):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record.update({
        "storage_uid": "uid-bellagio-deleting",
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    record_key = f"{POOL_NAME}.info"
    core_client = FakeCoreV1Client({record_key: record})
    k8s_client = object()
    apps_client = object()
    event_object = _storage_custom_object()
    event_object["metadata"].update({
        "uid": record["storage_uid"],
        "resourceVersion": "8",
    })
    buffered_object = _storage_custom_object()
    buffered_object["metadata"].update({
        "uid": record["storage_uid"],
        "resourceVersion": "9",
    })

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            pytest.fail("the supplied watch cursor must avoid an initial list")

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(method, *_args, **kwargs):
            assert method == custom_objects.list_namespaced_custom_object
            assert kwargs["resource_version"] == "7"
            return iter([
                {"type": operation, "object": event_object},
                {"type": operation, "object": buffered_object},
            ])

    initial_plan = _storage_plan(resource_version="7")
    event_plan = _storage_plan(resource_version="80")
    resumed_plan = _storage_plan(resource_version="90")
    storage_plans = iter([initial_plan, event_plan])
    aggregate_calls = []
    stable_calls = []
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: custom_objects if received is k8s_client else None,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda core, apps, storage: (
            aggregate_calls.append((core, apps, storage))
            or next(storage_plans)
        ),
    )

    def reconcile_stable(core, apps, storage, plan):
        stable_calls.append((core, apps, storage, plan))
        if plan is initial_plan:
            return initial_plan
        assert plan is event_plan
        core_client.config_map.data.pop(record_key)
        return resumed_plan

    monkeypatch.setattr(
        operator,
        "reconcile_watch_storage_until_stable",
        reconcile_stable,
    )
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda *_args, **_kwargs: pytest.fail(
            "cleanup-only ADDED event reached direct mutation"
        ),
    )
    monkeypatch.setattr(
        operator,
        "handle_modified",
        lambda *_args, **_kwargs: pytest.fail(
            "cleanup-only MODIFIED event reached direct mutation"
        ),
    )
    monkeypatch.setattr(
        operator,
        "handle_deleted",
        lambda *_args, **_kwargs: pytest.fail(
            "cleanup-only DELETED event reached direct mutation"
        ),
    )

    assert operator.watch_stream(
        core_client,
        k8s_client,
        apps_client,
        resource_version="7",
    ) == "90"

    expected_aggregate = (core_client, apps_client, custom_objects)
    assert aggregate_calls == [expected_aggregate, expected_aggregate]
    assert stable_calls == [
        (*expected_aggregate, initial_plan),
        (*expected_aggregate, event_plan),
    ]
    assert record_key not in core_client.config_map.data


@pytest.mark.parametrize("operation", ["ADDED", "MODIFIED", "DELETED"])
def test_watch_resumes_after_snapshot_that_removed_cleanup_marker(
        monkeypatch, operation):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record.update({
        "storage_uid": "uid-bellagio-deleting",
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    record_key = f"{POOL_NAME}.info"
    core_client = FakeCoreV1Client({record_key: record})
    k8s_client = object()
    apps_client = object()
    stale_object = _storage_custom_object()
    stale_object["metadata"].update({
        "uid": record["storage_uid"],
        "resourceVersion": "8",
    })

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            pytest.fail("the supplied watch cursor must avoid an initial list")

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(method, *_args, **kwargs):
            assert method == custom_objects.list_namespaced_custom_object
            if kwargs["resource_version"] != "90":
                return iter([{"type": operation, "object": stale_object}])
            return iter(())

    blocked_plan = _storage_plan(resource_version="80")
    resumed_plan = _storage_plan(resource_version="90")

    def finish_cleanup(_core, _apps, _storage, plan):
        assert plan is blocked_plan
        core_client.config_map.data.pop(record_key)
        return resumed_plan

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: custom_objects if received is k8s_client else None,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda core, apps, storage: blocked_plan
        if (core, apps, storage) == (
            core_client,
            apps_client,
            custom_objects,
        )
        else None,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_watch_storage_until_stable",
        finish_cleanup,
    )
    for handler in ("handle_added", "handle_modified", "handle_deleted"):
        monkeypatch.setattr(
            operator,
            handler,
            lambda *_args, **_kwargs: pytest.fail(
                "pre-snapshot event reached direct mutation"
            ),
        )

    assert operator.watch_stream(
        core_client,
        k8s_client,
        apps_client,
        resource_version="7",
    ) == "90"
    assert record_key not in core_client.config_map.data


def test_crd_watch_passes_rollout_client_to_each_stream(monkeypatch):
    operator = _load_operator(monkeypatch)
    core_client = object()
    k8s_client = object()
    apps_client = object()
    calls = []

    class WatchComplete(Exception):
        """Stop the permanent watch loop after its first dispatch."""

    def watch_stream(core, k8s, apps, resource_version=None):
        calls.append((core, k8s, apps, resource_version))
        raise WatchComplete

    monkeypatch.setattr(operator, "watch_stream", watch_stream)

    with pytest.raises(WatchComplete):
        operator.crd_watch(core_client, k8s_client, apps_client)

    assert calls == [(core_client, k8s_client, apps_client, None)]


def test_upgrade_quiesces_before_migration_and_csi_rollout(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )
    api_client = object()
    apps_client = object()
    storage_client = object()
    initial_item = _storage_custom_object(pool_type="External")
    storage_plan = _storage_plan(items=[initial_item])
    monkeypatch.setattr(
        operator.config,
        "load_incluster_config",
        lambda: calls.append("load-config"),
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CoreV1Api",
        lambda: core_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "ApiClient",
        lambda: api_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "AppsV1Api",
        lambda received: apps_client if received is api_client else None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: storage_client if received is api_client else None,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "deploy_config_map",
        lambda received: (
            calls.append("config-map") or ("ocean-crew", True)
            if received is core_client else None
        ),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_upgrade",
        lambda core, apps, storage: (
            calls.append("upgrade-csi") or storage_plan
        ) if (
            core is core_client
            and apps is apps_client
            and storage is storage_client
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_storage_until_stable",
        lambda core, apps, storage, plan: (
            calls.append("reconcile-stable") or plan
        )
        if (
            core is core_client
            and apps is apps_client
            and storage is storage_client
            and plan is storage_plan
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda core, provisioner_replicas=1,
        nodeplugin_tolerations=None: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ) if (
            core is core_client
            and nodeplugin_tolerations
            == storage_plan["nodeplugin_tolerations"]
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "wait_for_csi_provisioner_rollout",
        lambda apps: calls.append("provisioner-ready")
        if apps is apps_client else None,
    )
    monkeypatch.setattr(
        operator,
        "crd_watch",
        lambda core, k8s, apps, resource_version=None: calls.append(
            "crd-watch"
        )
        if (
            core is core_client
            and k8s is api_client
            and apps is apps_client
            and resource_version == "117"
        ) else None,
    )

    operator.main()

    assert calls.index("config-map") < calls.index("upgrade-csi")
    assert calls.index("upgrade-csi") < calls.index("reconcile-stable")
    assert calls.index("reconcile-stable") < calls.index("deploy-csi-1")
    assert calls.index("deploy-csi-1") < calls.index("provisioner-ready")
    assert calls.index("provisioner-ready") < calls.index("crd-watch")
    assert calls.index("deploy-csi-1") < calls.index("crd-watch")


def test_fresh_start_reconciles_precreated_crs_before_serving(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    core_client = object()
    api_client = object()
    apps_client = object()
    storage_client = object()
    tolerations = [{"key": "bellagio", "operator": "Exists"}]
    storage_plan = _storage_plan(
        items=[_storage_custom_object(name="bellagio-precreated")],
        tolerations=tolerations,
        resource_version="fresh-8",
    )
    monkeypatch.setattr(
        operator.config,
        "load_incluster_config",
        lambda: None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CoreV1Api",
        lambda: core_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "ApiClient",
        lambda: api_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "AppsV1Api",
        lambda _client: apps_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: storage_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "deploy_config_map",
        lambda _core: ("ocean-crew", False),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_upgrade",
        lambda core, apps, storage: (
            calls.append("fence-and-gate") or storage_plan
        ) if (
            core is core_client
            and apps is apps_client
            and storage is storage_client
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_storage_until_stable",
        lambda core, apps, storage, plan: (
            calls.append("reconcile-precreated") or plan
        ) if (
            core is core_client
            and apps is apps_client
            and storage is storage_client
            and plan is storage_plan
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda core, provisioner_replicas=1,
        nodeplugin_tolerations=None: calls.append("serve")
        if (
            core is core_client
            and provisioner_replicas == 1
            and nodeplugin_tolerations == tolerations
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "wait_for_csi_provisioner_rollout",
        lambda apps: calls.append("provisioner-ready")
        if apps is apps_client else None,
    )
    monkeypatch.setattr(
        operator,
        "crd_watch",
        lambda core, k8s, apps, resource_version=None: calls.append("watch")
        if (
            core is core_client
            and k8s is api_client
            and apps is apps_client
            and resource_version == "fresh-8"
        ) else None,
    )
    monkeypatch.setattr(
        operator,
        "clear_operator_ready",
        lambda: calls.append("bellagio-unready"),
    )
    monkeypatch.setattr(
        operator,
        "mark_operator_ready",
        lambda: calls.append("bellagio-ready"),
    )

    operator.main()

    assert calls == [
        "bellagio-unready",
        "fence-and-gate",
        "reconcile-precreated",
        "serve",
        "provisioner-ready",
        "bellagio-ready",
        "watch",
        "bellagio-unready",
    ]


def test_operator_readiness_marker_is_created_and_cleared(
        monkeypatch, tmp_path):
    operator = _load_operator(monkeypatch)
    marker = tmp_path / "bellagio-operator-ready"
    monkeypatch.setattr(operator, "OPERATOR_READY_FILE", str(marker))

    operator.mark_operator_ready()

    assert marker.is_file()
    assert marker.stat().st_mode & 0o777 == 0o600

    operator.clear_operator_ready()
    operator.clear_operator_ready()

    assert not marker.exists()


def test_operator_failure_clears_stale_readiness(monkeypatch, tmp_path):
    operator = _load_operator(monkeypatch)
    marker = tmp_path / "bellagio-stale-ready"
    marker.write_text("Ocean's Eleven", encoding="utf-8")
    monkeypatch.setattr(operator, "OPERATOR_READY_FILE", str(marker))

    def fail_after_fence():
        assert not marker.exists()
        raise TimeoutError("Bellagio provisioner rollout timed out")

    monkeypatch.setattr(operator, "_run_operator", fail_after_fence)

    with pytest.raises(TimeoutError, match="provisioner rollout"):
        operator.main()

    assert not marker.exists()


def test_server_upgrade_failure_keeps_provisioner_fenced(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    core_client = object()
    api_client = object()
    apps_client = object()
    storage_client = object()
    monkeypatch.setattr(
        operator.config,
        "load_incluster_config",
        lambda: None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CoreV1Api",
        lambda: core_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "ApiClient",
        lambda: api_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "AppsV1Api",
        lambda _client: apps_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: storage_client if received is api_client else None,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "deploy_config_map",
        lambda _core: ("ocean-crew", True),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_upgrade",
        lambda _core, _apps, _storage: (
            calls.append("upgrade-csi-fenced") or _storage_plan()
        ),
    )

    def fail_storage_upgrade(
            _core, _apps, _storage, storage_plan=None):
        assert storage_plan is not None
        calls.append("upgrade-storage")
        raise TimeoutError("Bellagio server rollout timed out")

    monkeypatch.setattr(operator, "upgrade_storage_pods", fail_storage_upgrade)
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )

    with pytest.raises(TimeoutError, match="Bellagio server rollout"):
        operator.main()

    assert calls == ["upgrade-csi-fenced", "upgrade-storage"]


def test_initial_reconciliation_failure_keeps_provisioner_fenced(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    core_client = object()
    api_client = object()
    apps_client = object()
    storage_client = object()
    initial_item = _storage_custom_object(
        name="mirage-external",
        pool_type="External",
    )
    storage_plan = _storage_plan(items=[initial_item])
    monkeypatch.setattr(
        operator.config,
        "load_incluster_config",
        lambda: None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CoreV1Api",
        lambda: core_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "ApiClient",
        lambda: api_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "AppsV1Api",
        lambda _client: apps_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: storage_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "deploy_config_map",
        lambda _core: ("ocean-crew", True),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_upgrade",
        lambda _core, _apps, _storage: (
            calls.append("upgrade-csi-fenced") or storage_plan
        ),
    )
    monkeypatch.setattr(
        operator,
        "upgrade_storage_pods",
        lambda *_args, **_kwargs: calls.append("upgrade-storage"),
    )

    def fail_initial_reconciliation(
            _core, items, _apps, provisioner_fenced=False):
        assert items == [initial_item]
        assert provisioner_fenced is True
        calls.append("reconcile-initial")
        raise RuntimeError("Mirage External reconciliation failed")

    monkeypatch.setattr(
        operator,
        "reconcile_initial_storage",
        fail_initial_reconciliation,
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )

    with pytest.raises(RuntimeError, match="External reconciliation failed"):
        operator.main()

    assert calls == [
        "upgrade-csi-fenced",
        "upgrade-storage",
        "reconcile-initial",
    ]


def test_storage_api_failure_before_mutation_does_not_change_provisioner(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    record = {
        "type": "Replica1",
        "pvReclaimPolicy": "delete",
        "volume_id": HOSTING_VOLUME_ID,
        "bricks": [],
    }
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={
            f"{POOL_NAME}.info": json.dumps(record),
        }),
    )
    api_client = object()
    apps_client = object()

    class StorageClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            raise RuntimeError("casino storage API is unavailable")

    storage_client = StorageClient()
    monkeypatch.setattr(
        operator.config,
        "load_incluster_config",
        lambda: None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CoreV1Api",
        lambda: core_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "ApiClient",
        lambda: api_client,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "AppsV1Api",
        lambda received: apps_client if received is api_client else None,
        raising=False,
    )
    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: storage_client if received is api_client else None,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "deploy_config_map",
        lambda _core: ("ocean-crew", True),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce-provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1, **_kwargs: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda *_args: calls.append("deploy-server"),
    )

    with pytest.raises(RuntimeError, match="storage API is unavailable"):
        operator.main()

    assert calls == []


@pytest.mark.parametrize(
    ("pool_policy", "kubernetes_policy"),
    [
        ("delete", "Delete"),
        ("archive", "Delete"),
        ("retain", "Retain"),
    ],
)
def test_storage_class_reclaim_policy_mapping(
        monkeypatch, pool_policy, kubernetes_policy):
    operator = _load_operator(monkeypatch)

    assert operator.storage_class_reclaim_policy(pool_policy) == (
        kubernetes_policy
    )


def test_external_modified_reconciles_reclaim_policy_without_restart(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "External",
        "pvReclaimPolicy": "delete",
        "single_pv_per_pool": False,
        "gluster_hosts": "bellagio.example.invalid",
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "log-level=WARNING",
        "mount_identity": MOUNT_IDENTITY,
    }
    existing["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(existing)
    )
    persistent_volume = _persistent_volume(
        single_pv_per_pool="false",
        reclaim_policy="Delete",
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": existing},
        [persistent_volume],
    )
    original_patch = core_client.patch_persistent_volume

    def patch_persistent_volume(name, body):
        original_patch(name, body)
        persistent_volume.spec.persistent_volume_reclaim_policy = (
            body["spec"]["persistentVolumeReclaimPolicy"]
        )

    core_client.patch_persistent_volume = patch_persistent_volume
    storage_class = SimpleNamespace(
        metadata=SimpleNamespace(
            name=f"kadalu.{POOL_NAME}",
            uid="uid-storage-class-bellagio-external",
            resource_version="117",
            annotations={},
        ),
        provisioner="kadalu",
        parameters={
            "hostvol_type": "External",
            "gluster_hosts": "bellagio.example.invalid",
            "gluster_volname": "the-benedict-vault",
            "gluster_options": "log-level=WARNING",
            "single_pv_per_pool": "False",
        },
        reclaim_policy="Delete",
    )
    storage_api = FakeStorageV1Api([storage_class])
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )
    rendered = []
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **kwargs: rendered.append((
            filename,
            kwargs["reclaim_policy"],
        )),
    )
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )
    monkeypatch.setattr(operator, "add_tolerations", lambda *_args: None)
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "External",
            "pvReclaimPolicy": "retain",
            "single_pv_per_pool": False,
            "details": {
                "gluster_host": "bellagio.example.invalid",
                "gluster_volname": "the-benedict-vault",
                "gluster_options": "log-level=WARNING",
            },
        },
    }

    operator.handle_modified(
        core_client,
        obj,
        provisioner_fenced=True,
    )

    record = _saved_record(core_client)
    assert obj["spec"]["volume_id"] == HOSTING_VOLUME_ID
    assert record["volume_id"] == HOSTING_VOLUME_ID
    assert record["single_pv_per_pool"] is False
    assert record["mount_identity"] == MOUNT_IDENTITY
    assert record["pvReclaimPolicy"] == "retain"
    _assert_mount_metadata(operator, record)
    assert core_client.pv_patches == [(
        VOLUME_ID,
        {"spec": {"persistentVolumeReclaimPolicy": "Retain"}},
    )]
    assert core_client.events[:2] == [
        ("persistent-volume", VOLUME_ID),
        ("config-map", operator.KADALU_CONFIG_MAP),
    ]
    storage_class_path = str(
        Path(operator.MANIFESTS_DIR) / "external-storageclass.yaml"
    )
    assert rendered == [(storage_class_path, "Retain")]
    assert len(storage_api.deletes) == 1
    deleted_name, delete_options = storage_api.deletes[0]
    assert deleted_name == f"kadalu.{POOL_NAME}"
    assert delete_options.preconditions.uid == storage_class.metadata.uid
    assert delete_options.preconditions.resource_version == "117"
    assert commands == [(
        operator.KUBECTL_CMD,
        operator.CREATE_CMD,
        "-f",
        storage_class_path,
    )]


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("volume_id", OTHER_HOSTING_VOLUME_ID),
        ("single_pv_per_pool", True),
    ],
    ids=["volume-identity", "single-pv-mode"],
)
def test_external_modified_rejects_immutable_pool_changes(
        monkeypatch, changed_field, changed_value):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "External",
        "pvReclaimPolicy": "delete",
        "single_pv_per_pool": False,
        "gluster_hosts": "bellagio.example.invalid",
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "",
        "mount_identity": MOUNT_IDENTITY,
    }
    existing["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(existing)
    )
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: pytest.fail(
            "immutable change rendered a StorageClass"
        ),
    )
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "External",
            "pvReclaimPolicy": "retain",
            "single_pv_per_pool": False,
            "details": {
                "gluster_host": "bellagio.example.invalid",
                "gluster_volname": "the-benedict-vault",
            },
        },
    }
    obj["spec"][changed_field] = changed_value

    operator.handle_modified(core_client, obj)

    assert _saved_record(core_client) == existing
    assert not core_client.patches
    assert not core_client.pv_patches


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("gluster_host", "mirage.example.invalid"),
        ("gluster_volname", "the-mirage-decoy-vault"),
        ("gluster_options", "volfile-server=mirage.example.invalid"),
    ],
    ids=["hosts", "volume", "mount-options"],
)
def test_external_modified_rejects_backend_retarget(
        monkeypatch, changed_field, changed_value):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "External",
        "pvReclaimPolicy": "delete",
        "single_pv_per_pool": False,
        "gluster_hosts": "bellagio.example.invalid",
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "log-level=WARNING",
        "mount_identity": MOUNT_IDENTITY,
    }
    existing["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(existing)
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": existing},
        [_persistent_volume(single_pv_per_pool="false")],
    )
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: pytest.fail(
            "backend retarget rendered a StorageClass"
        ),
    )
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "External",
            "pvReclaimPolicy": "retain",
            "single_pv_per_pool": False,
            "details": {
                "gluster_host": "bellagio.example.invalid",
                "gluster_volname": "the-benedict-vault",
                "gluster_options": "log-level=WARNING",
            },
        },
    }
    obj["spec"]["details"][changed_field] = changed_value

    operator.handle_modified(core_client, obj)

    assert _saved_record(core_client) == existing
    assert not core_client.patches
    assert not core_client.pv_patches


def test_external_modified_accepts_reordered_backend_hosts(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "type": "External",
        "single_pv_per_pool": False,
        "gluster_hosts": (
            "bellagio-one.example.invalid,bellagio-two.example.invalid"
        ),
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "log-level=WARNING",
    }
    assert operator.apply_pool_mount_metadata(existing, None) is True
    reordered = {
        **existing,
        "gluster_hosts": (
            "bellagio-two.example.invalid,bellagio-one.example.invalid"
        ),
    }

    assert operator.apply_pool_mount_metadata(
        reordered,
        json.dumps(existing),
    ) is True
    assert reordered["mount_identity"] == existing["mount_identity"]
    assert (
        reordered["mount_config_fingerprint"]
        == existing["mount_config_fingerprint"]
    )


@pytest.mark.parametrize(
    "storage_class_name",
    [None, "default"],
    ids=["legacy-fallback", "custom-name"],
)
def test_external_handler_accepts_reordered_class_backend_hosts(
        monkeypatch, storage_class_name):
    operator = _load_operator(monkeypatch)
    obj = _external_pool_object(storage_class_name=storage_class_name)
    obj["spec"]["details"]["gluster_hosts"] = [
        "bellagio-one.example.invalid",
        "bellagio-two.example.invalid",
    ]
    existing = {
        "volname": POOL_NAME,
        "volume_id": HOSTING_VOLUME_ID,
        "storage_uid": obj["metadata"]["uid"],
        "type": "External",
        "pvReclaimPolicy": "delete",
        "single_pv_per_pool": False,
        "gluster_hosts": (
            "bellagio-two.example.invalid,bellagio-one.example.invalid"
        ),
        "gluster_volname": "the-benedict-vault",
        "gluster_options": "log-level=WARNING",
        "mount_identity": MOUNT_IDENTITY,
    }
    if storage_class_name is not None:
        existing["storageClassName"] = storage_class_name
    existing["mount_config_fingerprint"] = (
        operator.mount_config_fingerprint(existing)
    )
    storage_class = _owned_storage_class(operator, obj, durable=True)
    storage_class.parameters = {
        **storage_class.parameters,
        "gluster_hosts": existing["gluster_hosts"],
    }
    storage_api = FakeStorageV1Api([storage_class])
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": existing})
    rendered = []
    commands = []
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **values: rendered.append((filename, values)),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    assert operator.handle_modified(
        core_client,
        obj,
        storage_api=storage_api,
    ) is True

    record = _saved_record(core_client)
    assert record["gluster_hosts"] == (
        "bellagio-one.example.invalid,bellagio-two.example.invalid"
    )
    assert record["storageClassName"] == (
        storage_class_name or f"kadalu.{POOL_NAME}"
    )
    assert len(rendered) == 1
    assert storage_api.deletes == []
    assert commands == []


def test_storage_upgrade_plan_unions_active_and_orphan_tolerations(monkeypatch):
    operator = _load_operator(monkeypatch)
    active_tolerations = [{
        "key": "bellagio-active",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    orphan_tolerations = [SimpleNamespace(
        key="mirage-orphan",
        operator="Exists",
        effect="NoExecute",
        value=None,
        toleration_seconds=300,
    )]
    external_tolerations = [{
        **active_tolerations[0],
    }, {
        "key": "ocean-external",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    orphan_name = "mirage-pool"
    external_name = "ocean-external-pool"
    records = {
        f"{POOL_NAME}.info": _stored_native_pool(),
        f"{orphan_name}.info": _stored_native_pool(orphan_name),
        f"{external_name}.info": {
            "volname": external_name,
            "type": "External",
            "volume_id": _pool_volume_id(external_name),
            "gluster_hosts": "casino.example.invalid",
            "gluster_volname": "casino-vault",
            "tolerations": external_tolerations,
        },
    }
    deletion_uid = "uid-mirage-deleting"
    records[f"{orphan_name}.info"]["storage_uid"] = deletion_uid
    core_client = FakeCoreV1Client(records)

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(name, _namespace):
            assert name == f"server-{orphan_name}-0"
            return _server_statefulset(tolerations=orphan_tolerations)

    storage_list = {
        "items": [
            _storage_custom_object(tolerations=active_tolerations),
            _storage_custom_object(
                name=orphan_name,
                tolerations=[{"key": "ignored-deleting"}],
                deleting=True,
            ),
        ],
        "metadata": {"resourceVersion": "117"},
    }
    storage_list["items"][1]["metadata"]["uid"] = deletion_uid

    plan = operator.prepare_storage_upgrade(
        core_client,
        AppsClient(),
        storage_list,
    )

    assert plan["resource_version"] == "117"
    assert plan["nodeplugin_tolerations"] == [
        active_tolerations[0],
        {
            "effect": "NoExecute",
            "key": "mirage-orphan",
            "operator": "Exists",
            "tolerationSeconds": 300,
        },
        external_tolerations[1],
    ]
    assert [
        obj["metadata"]["name"] for obj in plan["upgrade_objects"]
    ] == [POOL_NAME]


def test_storage_upgrade_rejects_class_name_change_before_nodeplugin_mutation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(
        storage_class_name="bellagio-vault-class",
    )
    storage_obj = _storage_custom_object()
    storage_obj["spec"].update({
        "storageClassName": "default",
        "volume_id": record["volume_id"],
    })

    with pytest.raises(RuntimeError, match="immutable StorageClass"):
        operator.reconcile_nodeplugin_from_storage(
            FakeCoreV1Client({f"{POOL_NAME}.info": record}),
            MutationGuardClient(),
            object(),
            {
                "items": [storage_obj],
                "metadata": {"resourceVersion": "117-class-change"},
            },
        )


def test_storage_upgrade_rejects_foreign_custom_class_before_mutation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    storage_obj = _storage_custom_object()
    storage_obj["metadata"]["uid"] = "uid-bellagio-startup"
    storage_obj["spec"]["storageClassName"] = "default"
    foreign = SimpleNamespace(
        metadata=SimpleNamespace(name="default", annotations={}),
        provisioner="casino.invalid/decoy-provisioner",
        parameters={},
        reclaim_policy="Retain",
    )
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: SimpleNamespace(
            list_storage_class=lambda: SimpleNamespace(items=[foreign]),
        ),
        raising=False,
    )

    with pytest.raises(RuntimeError, match="not owned"):
        operator.reconcile_nodeplugin_from_storage(
            FakeCoreV1Client({}),
            MutationGuardClient(),
            object(),
            {
                "items": [storage_obj],
                "metadata": {"resourceVersion": "117-class-collision"},
            },
        )


def test_storage_upgrade_rejects_stale_class_uid_before_server_mutation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    storage_obj = _storage_custom_object()
    storage_obj["metadata"].update({
        "uid": "uid-current-bellagio",
        "creationTimestamp": "2026-08-15T10:00:00Z",
    })
    stale_obj = deepcopy(storage_obj)
    stale_obj["metadata"]["uid"] = "uid-former-bellagio"
    storage_class = _owned_storage_class(
        operator,
        stale_obj,
        durable=True,
        volume_id=record["volume_id"],
    )
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: SimpleNamespace(
            list_storage_class=lambda: SimpleNamespace(
                items=[storage_class]
            ),
        ),
        raising=False,
    )

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(_name, _namespace):
            return _server_statefulset(
                creation_timestamp="2026-08-15T11:00:00Z",
            )

        @staticmethod
        def patch_namespaced_daemon_set(*_args):
            pytest.fail("stale StorageClass reached nodeplugin mutation")

    with pytest.raises(RuntimeError, match="owned by another"):
        operator.reconcile_nodeplugin_from_storage(
            FakeCoreV1Client({f"{POOL_NAME}.info": record}),
            AppsClient(),
            object(),
            {
                "items": [storage_obj],
                "metadata": {"resourceVersion": "117-stale-class"},
            },
        )


def test_storage_upgrade_rejects_two_fresh_pools_sharing_custom_class(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    first = _storage_custom_object(name="bellagio-vault")
    first["spec"]["storageClassName"] = "default"
    second = _storage_custom_object(name="mirage-vault")
    second["spec"]["storageClassName"] = "default"

    with pytest.raises(RuntimeError, match="same StorageClass"):
        operator.reconcile_nodeplugin_from_storage(
            FakeCoreV1Client({}),
            MutationGuardClient(),
            object(),
            {
                "items": [first, second],
                "metadata": {"resourceVersion": "117-shared-class"},
            },
        )


def test_storage_upgrade_rejects_orphan_and_fresh_pool_sharing_class(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    orphan_name = "mirage-vault"
    orphan = _stored_native_pool(
        orphan_name,
        storage_class_name="default",
    )
    fresh = _storage_custom_object(name="bellagio-vault")
    fresh["spec"]["storageClassName"] = "default"

    with pytest.raises(RuntimeError, match="same StorageClass"):
        operator.reconcile_nodeplugin_from_storage(
            FakeCoreV1Client({f"{orphan_name}.info": orphan}),
            MutationGuardClient(),
            object(),
            {
                "items": [fresh],
                "metadata": {"resourceVersion": "117-orphan-class"},
            },
        )


def test_owned_pool_rejects_same_name_cr_with_different_uid(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record["storage_uid"] = "uid-original-ocean"
    storage_obj = _storage_custom_object()
    storage_obj["metadata"].update({
        "uid": "uid-recreated-benedict",
        "creationTimestamp": "2026-08-15T12:00:00Z",
    })
    storage_obj["spec"]["volume_id"] = record["volume_id"]

    with pytest.raises(RuntimeError, match="does not own"):
        operator.prepare_storage_upgrade(
            FakeCoreV1Client({f"{POOL_NAME}.info": record}),
            object(),
            {
                "items": [storage_obj],
                "metadata": {"resourceVersion": "116-owned"},
            },
        )


def test_legacy_pool_binds_uid_when_cr_predates_retained_server(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record["volume_id"] = HOSTING_VOLUME_ID
    record["bricks"][0].update({
        "host_brick_path": "/srv/bellagio-vault",
        "kube_hostname": "bellagio-storage-one",
    })
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    obj = _native_pool_object()
    obj["metadata"].update({
        "uid": "uid-original-ocean",
        "creationTimestamp": "2025-01-01T00:00:00Z",
    })
    obj["spec"].pop("volume_id")
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            creation_timestamp="2025-01-02T00:00:00Z",
        ),
    )
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    monkeypatch.setattr(operator, "deploy_server_pods", lambda *_args: None)

    assert operator.handle_added(core_client, obj, apps_client) is True
    assert _saved_record(core_client)["storage_uid"] == "uid-original-ocean"


def test_legacy_pool_rejects_cr_newer_than_retained_server(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    obj = _native_pool_object()
    obj["metadata"].update({
        "uid": "uid-recreated-benedict",
        "creationTimestamp": "2025-01-03T00:00:00Z",
    })
    obj["spec"].pop("volume_id")
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            creation_timestamp="2025-01-02T00:00:00Z",
        ),
    )
    monkeypatch.setattr(
        operator,
        "deploy_storage_class",
        lambda *_args: pytest.fail("recreated CR deployed a StorageClass"),
    )

    assert operator.handle_added(core_client, obj, apps_client) is False
    assert not core_client.patches


def test_legacy_pool_explicit_volume_identity_allows_recovery(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    storage_obj = _storage_custom_object()
    storage_obj["metadata"].update({
        "uid": "uid-recovery-ocean",
        "creationTimestamp": "2026-08-15T12:00:00Z",
    })
    storage_obj["spec"]["volume_id"] = record["volume_id"]

    plan = operator.prepare_storage_upgrade(
        FakeCoreV1Client({f"{POOL_NAME}.info": record}),
        object(),
        {
            "items": [storage_obj],
            "metadata": {"resourceVersion": "116-recovery"},
        },
    )

    assert [
        item["metadata"]["name"] for item in plan["upgrade_objects"]
    ] == [POOL_NAME]


def test_storage_upgrade_plan_tracks_missing_cr_as_deletion_orphan(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = {
        "volname": "mirage-external",
        "type": "External",
        "volume_id": _pool_volume_id("mirage-external"),
        "gluster_hosts": "mirage.example.invalid",
        "gluster_volname": "mirage-vault",
        "tolerations": [],
    }
    core_client = FakeCoreV1Client({"mirage-external.info": record})
    storage_api = FakeStorageV1Api([
        _legacy_storage_class(operator, record),
    ])

    plan = operator.prepare_storage_upgrade(
        core_client,
        object(),
        {"items": [], "metadata": {"resourceVersion": "117-orphan"}},
        storage_api=storage_api,
    )

    assert plan["deletion_orphans"] == [{
        "object": {
            "metadata": {"name": "mirage-external"},
            "spec": {"type": "External"},
        },
        "record": record,
        "legacy_storage_uid": f"orphan:{record['volume_id']}",
    }]


def test_uidless_deleting_pool_with_foreign_class_is_quarantined(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    deleting = _native_pool_object()
    deleting["metadata"].update({
        "uid": "uid-rusty-ryan-deleting",
        "deletionTimestamp": "2026-08-15T04:11:00Z",
    })
    deleting["spec"]["volume_id"] = record["volume_id"]
    conflicting_class = _legacy_storage_class(operator, record)
    conflicting_class.metadata.annotations = {
        operator.STORAGE_CLASS_NAMESPACE_ANNOTATION: operator.NAMESPACE,
        operator.STORAGE_CLASS_NAME_ANNOTATION: POOL_NAME,
        operator.STORAGE_CLASS_UID_ANNOTATION: "uid-benedict-foreign-class",
    }
    storage_api = FakeStorageV1Api([conflicting_class])
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )
    storage_client = SimpleNamespace(
        get_namespaced_custom_object=lambda *_args: deleting,
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {
            "items": [deleting],
            "metadata": {"resourceVersion": "117-deleting"},
        },
        storage_api=storage_api,
    )

    assert plan["deletion_orphans"][0]["legacy_quarantine"] is True
    assert "legacy_storage_uid" not in plan["deletion_orphans"][0]
    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        provisioner_fenced=True,
        storage_client=storage_client,
    ) is True
    quarantined = _saved_record(core_client)
    assert quarantined["provisioning_disabled"] is True
    assert quarantined[
        operator.DELETION_QUARANTINE_RECORD_FIELD
    ] is True
    assert not storage_api.deletes


def test_uidless_legacy_orphan_is_sealed_before_cleanup(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    class_name = f"kadalu.{POOL_NAME}"
    persistent_volume = _persistent_volume(
        volume_id="pvc-bellagio-legacy-loot",
        hostvol=POOL_NAME,
        storage_class_name=class_name,
        single_pv_per_pool="false",
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        persistent_volumes=[persistent_volume],
    )
    storage_api = FakeStorageV1Api([
        _legacy_storage_class(operator, record),
    ])
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )

    class MissingStorage(Exception):
        status = 404

    storage_client = SimpleNamespace(
        get_namespaced_custom_object=lambda *_args: (_ for _ in ()).throw(
            MissingStorage()
        ),
    )
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("nonempty orphan servers were deleted"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: pytest.fail("nonempty orphan record was deleted"),
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "118-orphan"}},
        storage_api=storage_api,
    )
    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        provisioner_fenced=True,
        storage_client=storage_client,
    ) is True

    sealed = _saved_record(core_client)
    assert sealed["storage_uid"] == f"orphan:{record['volume_id']}"
    assert sealed["provisioning_disabled"] is True
    assert sealed[operator.CLEANUP_ONLY_RECORD_FIELD] is True
    assert storage_api.deletes[0][0] == class_name
    delete_options = storage_api.deletes[0][1]
    assert delete_options.preconditions.uid == (
        f"uid-storage-class-{class_name}"
    )
    assert delete_options.preconditions.resource_version == "117"
    assert len(core_client.patches) == 1
    assert len(core_client.replacements) == 1
    assert core_client.replacements[0][2].metadata.resource_version == "101"

    replacement = _native_pool_object()
    replacement["metadata"]["uid"] = "uid-benedict-replacement"
    replacement["spec"]["volume_id"] = record["volume_id"]
    with pytest.raises(RuntimeError, match="does not own"):
        operator._validate_storage_record_owner(replacement, sealed)


@pytest.mark.parametrize(
    ("owner_kind", "valid_replacement"),
    [
        ("synthetic", True),
        ("synthetic", False),
        ("deleting-cr", True),
        ("deleting-cr", False),
    ],
)
def test_cleanup_only_orphan_quarantines_same_name_cr_in_plan(
        monkeypatch, owner_kind, valid_replacement):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    storage_uid = (
        f"orphan:{record['volume_id']}"
        if owner_kind == "synthetic"
        else "uid-ocean-deleting"
    )
    record.update({
        "storage_uid": storage_uid,
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    persistent_volume = _persistent_volume(
        volume_id="pvc-bellagio-retained-loot",
        hostvol=POOL_NAME,
        storage_class_name=f"kadalu.{POOL_NAME}",
        single_pv_per_pool="false",
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        persistent_volumes=[persistent_volume],
    )
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )
    replacement = (
        _native_pool_object()
        if valid_replacement
        else {"metadata": {"name": POOL_NAME}}
    )
    replacement["metadata"]["uid"] = "uid-benedict-replacement"
    if valid_replacement:
        replacement["spec"]["volume_id"] = record["volume_id"]

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {
            "items": [replacement],
            "metadata": {"resourceVersion": "118-replacement"},
        },
        storage_api=FakeStorageV1Api(),
    )
    assert plan["invalid_items"] == [replacement]
    assert not plan["reconcile_items"]
    assert plan["deletion_orphans"] == [{
        "object": {
            "metadata": {"name": POOL_NAME},
            "spec": {"type": "Replica1"},
        },
        "record": record,
    }]
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("retained orphan servers were deleted"),
    )
    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        provisioner_fenced=True,
    ) is True
    assert not core_client.patches


def test_uidless_legacy_orphan_finishes_cleanup_at_zero_pvs(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    storage_api = FakeStorageV1Api([
        _legacy_storage_class(operator, record),
    ])
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )

    class MissingStorage(Exception):
        status = 404

    storage_client = SimpleNamespace(
        get_namespaced_custom_object=lambda *_args: (_ for _ in ()).throw(
            MissingStorage()
        ),
    )
    cleanup = []
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: cleanup.append("servers"),
    )
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: cleanup.append("service-manifest"),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: cleanup.append("service-delete"),
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "119-orphan"}},
        storage_api=storage_api,
    )
    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        provisioner_fenced=True,
        storage_client=storage_client,
    ) is True

    assert storage_api.deletes
    assert cleanup == ["servers", "service-manifest", "service-delete"]
    assert core_client.config_map.data[f"{POOL_NAME}.info"] is None
    assert len(core_client.patches) == 2


def test_uidless_orphan_without_exact_class_is_tombstoned_only(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )

    class MissingStorage(Exception):
        status = 404

    storage_client = SimpleNamespace(
        get_namespaced_custom_object=lambda *_args: (_ for _ in ()).throw(
            MissingStorage()
        ),
    )
    fence_events = []
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: fence_events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: fence_events.append(("resume", apps)),
    )
    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "120-orphan"}},
        storage_api=FakeStorageV1Api(),
    )
    assert plan["deletion_orphans"][0]["legacy_quarantine"] is True
    assert "legacy_storage_uid" not in plan["deletion_orphans"][0]

    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        storage_client=storage_client,
    ) is True
    tombstoned = _saved_record(core_client)
    assert tombstoned["provisioning_disabled"] is True
    assert "storage_uid" not in tombstoned
    assert tombstoned[operator.DELETION_QUARANTINE_RECORD_FIELD] is True
    assert len(core_client.patches) == 1
    assert fence_events == [
        ("fence", apps_client),
        ("resume", apps_client),
    ]

    replacement = _native_pool_object()
    replacement["metadata"]["uid"] = "uid-benedict-quarantined"
    replacement_plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {
            "items": [replacement],
            "metadata": {"resourceVersion": "122-replacement"},
        },
        storage_api=FakeStorageV1Api(),
    )
    assert replacement_plan["invalid_items"] == [replacement]
    assert not replacement_plan["reconcile_items"]
    assert replacement_plan["deletion_orphans"][0][
        "legacy_quarantine"
    ] is True

    retry_plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "121-orphan"}},
        storage_api=FakeStorageV1Api(),
    )
    assert retry_plan["deletion_orphans"][0]["legacy_quarantine"] is True
    assert operator.reconcile_deletion_orphans(
        core_client,
        retry_plan["deletion_orphans"],
        apps_client,
        storage_client=storage_client,
    ) is True
    assert fence_events == [
        ("fence", apps_client),
        ("resume", apps_client),
    ]


def test_uidless_legacy_orphan_seals_same_name_cr_race_and_resumes(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    persistent_volume = _persistent_volume(
        volume_id="pvc-bellagio-race-loot",
        hostvol=POOL_NAME,
        storage_class_name=f"kadalu.{POOL_NAME}",
        single_pv_per_pool="false",
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        persistent_volumes=[persistent_volume],
    )
    storage_api = FakeStorageV1Api([
        _legacy_storage_class(operator, record),
    ])
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )
    replacement = _native_pool_object()
    replacement["metadata"]["uid"] = "uid-benedict-racing"
    storage_client = SimpleNamespace(
        get_namespaced_custom_object=lambda *_args: replacement,
    )
    fence_events = []
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: fence_events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: fence_events.append(("resume", apps)),
    )
    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "121-orphan"}},
        storage_api=storage_api,
    )

    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        storage_client=storage_client,
    ) is True
    sealed = _saved_record(core_client)
    assert sealed["storage_uid"] == f"orphan:{record['volume_id']}"
    assert sealed["provisioning_disabled"] is True
    assert sealed[operator.CLEANUP_ONLY_RECORD_FIELD] is True
    assert storage_api.deletes
    assert fence_events == [
        ("fence", apps_client),
        ("resume", apps_client),
    ]

    replacement_plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {
            "items": [replacement],
            "metadata": {"resourceVersion": "122-replacement"},
        },
        storage_api=storage_api,
    )
    assert replacement_plan["invalid_items"] == [replacement]
    assert not replacement_plan["reconcile_items"]
    assert replacement_plan["deletion_orphans"]


def test_cleanup_only_class_conflict_is_quarantined_without_global_fence(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    record.update({
        "storage_uid": "uid-ocean-deleting",
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    conflicting_class = _legacy_storage_class(operator, record)
    conflicting_class.metadata.annotations = {
        operator.STORAGE_CLASS_NAMESPACE_ANNOTATION: operator.NAMESPACE,
        operator.STORAGE_CLASS_NAME_ANNOTATION: POOL_NAME,
        operator.STORAGE_CLASS_UID_ANNOTATION: "uid-benedict-class",
    }
    storage_api = FakeStorageV1Api([conflicting_class])
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )
    fence_events = []
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: fence_events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: fence_events.append(("resume", apps)),
    )
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "122-orphan"}},
        storage_api=storage_api,
    )
    assert plan["deletion_orphans"][0]["cleanup_quarantine"] is True
    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
    ) is True
    assert not storage_api.deletes
    assert not core_client.patches
    assert fence_events == []


def test_delete_server_pods_renders_complete_legacy_template_inputs(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    tolerations = [{
        "key": "bellagio-cleanup-crew",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    record = _stored_native_pool(tolerations=tolerations)
    record["bricks"][0].pop("brick_device_dir")
    rendered = []
    commands = []
    monkeypatch.setattr(
        operator,
        "template",
        lambda filename, **values: rendered.append((filename, values)),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )

    operator.delete_server_pods(
        record,
        {"metadata": {"name": POOL_NAME}},
    )

    assert len(rendered) == 1
    _filename, values = rendered[0]
    assert values["brick_device_dir"] == ""
    assert values["tolerations"] == tolerations
    assert values["verbose"] == operator.VERBOSE
    assert commands == [(
        operator.KUBECTL_CMD,
        operator.DELETE_CMD,
        "-f",
        rendered[0][0],
        "--ignore-not-found=true",
    )]


def test_deleted_nonempty_orphan_retires_class_but_remains_serviceable(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    events = []
    original_patch = core_client.patch_namespaced_config_map

    def patch_config_map(*args):
        events.append("tombstone")
        return original_patch(*args)

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        patch_config_map,
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: events.append("pv-count") or 3,
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: events.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: pytest.fail("nonempty pool metadata was deleted"),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("nonempty pool servers were deleted"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=apps_client,
    ) is True
    assert events == [
        ("fence", apps_client),
        "tombstone",
        "storage-class",
        "pv-count",
        ("resume", apps_client),
    ]
    saved = _saved_record(core_client)
    assert saved["provisioning_disabled"] is True
    assert saved[operator.CLEANUP_ONLY_RECORD_FIELD] is True


def test_deleted_zero_pv_orphan_is_fully_cleaned(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    cleanup = []
    monkeypatch.setattr(operator, "get_num_pvs", lambda _core, _record: 0)
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: cleanup.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: cleanup.append("config-map"),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: cleanup.append("servers"),
    )
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: cleanup.append("service-manifest"),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: cleanup.append("service-delete"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        provisioner_fenced=True,
    ) is True
    assert cleanup == [
        "storage-class",
        "servers",
        "service-manifest",
        "service-delete",
        "config-map",
    ]
    assert core_client.patches


def test_deleted_cleanup_retries_ignore_already_absent_objects(monkeypatch):
    operator = _load_operator(monkeypatch)
    commands = []
    record = _stored_native_pool()
    record["provisioning_disabled"] = True
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    monkeypatch.setattr(operator, "get_num_pvs", lambda _core, _record: 0)
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *args: commands.append(args),
    )
    monkeypatch.setattr(operator, "delete_config_map", lambda *_args: None)

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        provisioner_fenced=True,
    ) is True
    assert commands
    assert all("--ignore-not-found=true" in command for command in commands)


def test_deleted_unknown_pv_count_fails_closed(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record["provisioning_disabled"] = True
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    monkeypatch.setattr(operator, "get_num_pvs", lambda _core, _record: -1)
    monkeypatch.setattr(operator, "delete_storage_class", lambda *_args: None)

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        provisioner_fenced=True,
    ) is False


def test_zero_pv_runtime_delete_fences_rechecks_and_resumes(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    original_patch = core_client.patch_namespaced_config_map

    def patch_config_map(*args):
        events.append("tombstone")
        return original_patch(*args)

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        patch_config_map,
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: events.append("pv-count") or 0,
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: events.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: events.append("servers"),
    )
    monkeypatch.setattr(
        operator,
        "template",
        lambda *_args, **_kwargs: events.append("service-manifest"),
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: events.append("service-delete"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: events.append("config-map"),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=apps_client,
    ) is True
    assert events == [
        ("fence", apps_client),
        "tombstone",
        "storage-class",
        "pv-count",
        "servers",
        "service-manifest",
        "service-delete",
        "config-map",
        ("resume", apps_client),
    ]


def test_runtime_delete_preserves_pool_if_pv_exists_after_tombstone(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    original_patch = core_client.patch_namespaced_config_map

    def patch_config_map(*args):
        events.append("tombstone")
        return original_patch(*args)

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        patch_config_map,
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: events.append("pv-count") or 1,
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: events.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("nonempty pool servers were deleted"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: pytest.fail("nonempty pool metadata was deleted"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=apps_client,
    ) is True
    assert events == [
        ("fence", apps_client),
        "tombstone",
        "storage-class",
        "pv-count",
        ("resume", apps_client),
    ]


def test_existing_tombstone_migrates_cleanup_marker_under_fence(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    record["provisioning_disabled"] = True
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    original_patch = core_client.patch_namespaced_config_map

    def migrate_tombstone(*args):
        events.append("cleanup-marker")
        return original_patch(*args)

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        migrate_tombstone,
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: events.append("pv-count") or 2,
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: events.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("nonempty pool servers were deleted"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: pytest.fail("nonempty pool metadata was deleted"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=apps_client,
    ) is True
    assert events == [
        ("fence", apps_client),
        "cleanup-marker",
        "storage-class",
        "pv-count",
        ("resume", apps_client),
    ]
    assert _saved_record(core_client)[
        operator.CLEANUP_ONLY_RECORD_FIELD
    ] is True


def test_existing_tombstone_blocks_replacement_until_marker_migration(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    _deletion_object(record)
    record["provisioning_disabled"] = True
    persistent_volume = _persistent_volume(
        volume_id="pvc-bellagio-tombstone-loot",
        hostvol=POOL_NAME,
        storage_class_name=f"kadalu.{POOL_NAME}",
        single_pv_per_pool="false",
    )
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        persistent_volumes=[persistent_volume],
    )
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )
    replacement = _native_pool_object()
    replacement["metadata"]["uid"] = "uid-benedict-too-early"

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {
            "items": [replacement],
            "metadata": {"resourceVersion": "123-replacement"},
        },
        storage_api=FakeStorageV1Api(),
    )

    assert plan["invalid_items"] == [replacement]
    assert not plan["reconcile_items"]
    assert plan["deletion_orphans"] == [{
        "object": {
            "metadata": {"name": POOL_NAME},
            "spec": {"type": "Replica1"},
        },
        "record": record,
    }]
    monkeypatch.setattr(operator, "delete_storage_class", lambda *_args: None)
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("retained pool servers were deleted"),
    )

    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        provisioner_fenced=True,
    ) is True
    saved = _saved_record(core_client)
    assert saved["provisioning_disabled"] is True
    assert saved[operator.CLEANUP_ONLY_RECORD_FIELD] is True


def test_existing_tombstone_with_foreign_class_migrates_to_local_quarantine(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    _deletion_object(record)
    record["provisioning_disabled"] = True
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    conflicting_class = _legacy_storage_class(operator, record)
    conflicting_class.metadata.annotations = {
        operator.STORAGE_CLASS_NAMESPACE_ANNOTATION: operator.NAMESPACE,
        operator.STORAGE_CLASS_NAME_ANNOTATION: POOL_NAME,
        operator.STORAGE_CLASS_UID_ANNOTATION: "uid-benedict-foreign-class",
    }
    storage_api = FakeStorageV1Api([conflicting_class])
    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: _server_statefulset(
            tolerations=[],
        ),
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "124-orphan"}},
        storage_api=storage_api,
    )

    assert plan["deletion_orphans"][0]["cleanup_quarantine"] is True
    assert operator.reconcile_deletion_orphans(
        core_client,
        plan["deletion_orphans"],
        apps_client,
        provisioner_fenced=True,
    ) is True
    saved = _saved_record(core_client)
    assert saved["provisioning_disabled"] is True
    assert saved[operator.CLEANUP_ONLY_RECORD_FIELD] is True
    assert not storage_api.deletes

    retry_plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "125-orphan"}},
        storage_api=storage_api,
    )
    assert retry_plan["deletion_orphans"][0]["cleanup_quarantine"] is True


def test_cleanup_only_nonempty_retry_does_not_fence(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    record.update({
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: events.append("pv-count") or 2,
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: pytest.fail("nonempty retry fenced the provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda *_args: pytest.fail("unfenced retry resumed the provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: pytest.fail("nonempty retry retired the class"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=object(),
    ) is True
    assert events == ["pv-count"]


def test_cleanup_only_zero_precheck_refences_and_recounts(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    record.update({
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    pv_counts = iter([0, 1])
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: (
            events.append("pv-count") or next(pv_counts)
        ),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: events.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("fenced recount ignored a new PV"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: pytest.fail("fenced recount deleted live metadata"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=apps_client,
    ) is True
    assert events == [
        "pv-count",
        ("fence", apps_client),
        "storage-class",
        "pv-count",
        ("resume", apps_client),
    ]


def test_tombstoned_delete_counts_custom_class_pv_by_hostvol(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record["provisioning_disabled"] = True
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client(
        {f"{POOL_NAME}.info": record},
        [_persistent_volume(
            storage_class_name="casino-imported-vaults",
            single_pv_per_pool="false",
        )],
    )
    retired = []
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: retired.append("storage-class"),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: pytest.fail("hostvol-owned PV was ignored"),
    )
    monkeypatch.setattr(
        operator,
        "delete_config_map",
        lambda *_args: pytest.fail("hostvol-owned PV metadata was deleted"),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        provisioner_fenced=True,
    ) is True
    assert retired == ["storage-class"]


def test_runtime_delete_resumes_other_pools_after_cleanup_failure(monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    original_patch = core_client.patch_namespaced_config_map

    def patch_config_map(*args):
        events.append("tombstone")
        return original_patch(*args)

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        patch_config_map,
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: events.append("pv-count") or 0,
    )
    monkeypatch.setattr(operator, "delete_storage_class", lambda *_args: None)
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_server_pods",
        lambda *_args: (_ for _ in ()).throw(
            RuntimeError("Bellagio API unavailable")
        ),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )

    with pytest.raises(RuntimeError, match="Bellagio API"):
        operator.handle_deleted(
            core_client,
            deletion_obj,
            storage_info_data=record,
            apps_v1_client=apps_client,
        )
    assert events == [
        ("fence", apps_client),
        "tombstone",
        "pv-count",
        ("resume", apps_client),
    ]


def test_runtime_delete_tombstone_failure_keeps_provisioner_fenced(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()

    def fail_tombstone(_name, _namespace, config_map):
        events.append("tombstone-failed")
        config_map.data[f"{POOL_NAME}.info"] = json.dumps(record)
        raise RuntimeError("Bellagio tombstone write failed")

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        fail_tombstone,
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: pytest.fail("class retired before durable tombstone"),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )

    with pytest.raises(RuntimeError, match="tombstone write failed"):
        operator.handle_deleted(
            core_client,
            deletion_obj,
            storage_info_data=record,
            apps_v1_client=apps_client,
        )

    assert events == [("fence", apps_client), "tombstone-failed"]
    assert "provisioning_disabled" not in _saved_record(core_client)


def test_runtime_delete_defers_class_failure_after_durable_tombstone(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    events = []
    record = _stored_native_pool()
    deletion_obj = _deletion_object(record)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    apps_client = object()
    original_patch = core_client.patch_namespaced_config_map

    def persist_tombstone(*args):
        events.append("tombstone")
        return original_patch(*args)

    monkeypatch.setattr(
        core_client,
        "patch_namespaced_config_map",
        persist_tombstone,
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args: (
            events.append("storage-class-failed")
            or (_ for _ in ()).throw(
                RuntimeError("Bellagio class retirement failed")
            )
        ),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )

    assert operator.handle_deleted(
        core_client,
        deletion_obj,
        storage_info_data=record,
        apps_v1_client=apps_client,
    ) is True

    assert events == [
        ("fence", apps_client),
        "tombstone",
        "storage-class-failed",
        ("resume", apps_client),
    ]
    saved = _saved_record(core_client)
    assert saved["provisioning_disabled"] is True
    assert saved[operator.CLEANUP_ONLY_RECORD_FIELD] is True


def test_deleted_absent_metadata_is_idempotent_after_completed_cleanup(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, _record: pytest.fail("absent deletion queried PVs"),
    )
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: SimpleNamespace(
            list_storage_class=lambda: SimpleNamespace(items=[]),
        ),
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *_args: pytest.fail("absent StorageClass was deleted"),
    )

    assert operator.handle_deleted(
        core_client,
        {"metadata": {"name": POOL_NAME}},
    ) is True


def test_stale_deleted_event_cannot_remove_recreated_storage_class(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    new_obj = _native_pool_object()
    new_obj["metadata"]["uid"] = "uid-new-bellagio"
    storage_class = _owned_storage_class(
        operator,
        new_obj,
        durable=True,
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[storage_class]),
    )
    monkeypatch.setattr(
        operator.client,
        "StorageV1Api",
        lambda: storage_api,
        raising=False,
    )
    commands = []
    monkeypatch.setattr(
        operator,
        "lib_execute",
        lambda *command: commands.append(command),
    )
    core_client = FakeCoreV1Client({})

    assert operator.handle_deleted(
        core_client,
        {
            "metadata": {
                "name": POOL_NAME,
                "uid": "uid-old-bellagio",
            },
        },
    ) is False
    assert commands == []


def test_stale_deleted_event_cannot_adopt_recreated_pool_record(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    record["storage_uid"] = "uid-new-bellagio"
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args, **_kwargs: pytest.fail(
            "stale event retired the replacement StorageClass"
        ),
    )
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: pytest.fail("stale event reached the CSI fence"),
    )

    assert operator.handle_deleted(
        core_client,
        {
            "metadata": {
                "name": POOL_NAME,
                "uid": "uid-old-bellagio",
            },
        },
        apps_v1_client=object(),
    ) is False
    assert not core_client.patches


def test_fenced_delete_rejects_replaced_record_snapshot(monkeypatch):
    operator = _load_operator(monkeypatch)
    old_record = _stored_native_pool()
    deletion_obj = _deletion_object(old_record)
    new_record = _stored_native_pool()
    new_record["storage_uid"] = "uid-new-bellagio"
    new_record["volume_id"] = OTHER_HOSTING_VOLUME_ID
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": new_record})
    events = []
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda _core, apps: events.append(("fence", apps)),
    )
    monkeypatch.setattr(
        operator,
        "resume_csi_provisioner",
        lambda apps: events.append(("resume", apps)),
    )
    monkeypatch.setattr(
        operator,
        "delete_storage_class",
        lambda *_args, **_kwargs: pytest.fail(
            "stale snapshot retired the replacement StorageClass"
        ),
    )
    apps_client = object()

    with pytest.raises(RuntimeError, match="does not own|changed"):
        operator.handle_deleted(
            core_client,
            deletion_obj,
            storage_info_data=old_record,
            apps_v1_client=apps_client,
        )

    assert events == [("fence", apps_client)]
    assert not core_client.patches


def test_deleted_legacy_record_without_owner_proof_fails_closed(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})

    assert operator.handle_deleted(
        core_client,
        {
            "metadata": {
                "name": POOL_NAME,
                "uid": "uid-bellagio-deletion",
            },
        },
        apps_v1_client=object(),
    ) is False
    assert not core_client.patches


def test_deleted_metadata_read_failure_propagates(monkeypatch):
    operator = _load_operator(monkeypatch)

    class MetadataReadFailed(Exception):
        pass

    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: (_ for _ in ()).throw(
            MetadataReadFailed("casino API unavailable")
        ),
    )

    with pytest.raises(MetadataReadFailed, match="casino API"):
        operator.handle_deleted(
            core_client,
            {"metadata": {"name": POOL_NAME}},
        )


def test_fenced_storage_reconciliation_repeats_after_cr_mutation(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    first = _storage_plan(
        items=[_storage_custom_object(name="bellagio-old")],
        resource_version="7",
    )
    changed = _storage_plan(
        items=[_storage_custom_object(name="mirage-new")],
        resource_version="8",
    )
    stable = _storage_plan(
        items=[_storage_custom_object(name="mirage-new")],
        # LIST resourceVersion is a cluster-wide snapshot boundary and can
        # advance even when no Kadalustorage object changed.
        resource_version="9",
    )
    prepared = iter([changed, stable])
    reconciled = []
    gated = []
    monkeypatch.setattr(
        operator,
        "reconcile_storage_plan",
        lambda _core, _apps, _storage, plan: reconciled.append(
            plan["resource_version"]
        ),
        raising=False,
    )
    monkeypatch.setattr(
        operator,
        "list_storage_resources",
        lambda _storage: object(),
    )
    monkeypatch.setattr(
        operator,
        "prepare_storage_upgrade",
        lambda *_args: next(prepared),
    )

    def gate_current(_core, _apps, _storage, plan, current_tolerations=None):
        gated.append((plan["resource_version"], current_tolerations))
        return plan

    monkeypatch.setattr(
        operator,
        "gate_nodeplugin_until_current",
        gate_current,
        raising=False,
    )

    result = operator.reconcile_storage_until_stable(
        object(),
        object(),
        object(),
        first,
    )

    assert result is stable
    assert reconciled == ["7", "8"]
    assert gated == [("8", []), ("9", [])]


def test_watch_replans_after_cleanup_unblocks_same_name_cr(monkeypatch):
    operator = _load_operator(monkeypatch)
    replacement = _storage_custom_object()
    replacement["metadata"]["uid"] = "uid-benedict-replacement"
    record = _stored_native_pool()
    record.update({
        "storage_uid": "uid-ocean-deleting",
        "provisioning_disabled": True,
        operator.CLEANUP_ONLY_RECORD_FIELD: True,
    })
    orphan = {
        "object": {"metadata": {"name": POOL_NAME}},
        "record": record,
    }
    blocked = _storage_plan(
        items=[replacement],
        deletion_orphans=[orphan],
        invalid_items=[replacement],
        resource_version="90",
    )
    blocked["reconcile_items"] = []
    unblocked = _storage_plan(
        items=[replacement],
        resource_version="91",
    )
    reconciled_items = []
    deletion_plans = []
    relists = []
    monkeypatch.setattr(
        operator,
        "reconcile_initial_storage",
        lambda _core, items, *_args, **_kwargs: (
            reconciled_items.append(list(items)) or True
        ),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_planned_deletion_orphans",
        lambda _core, _apps, _storage, plan, **_kwargs: (
            deletion_plans.append(plan) or True
        ),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: relists.append(True) or unblocked,
    )

    result = operator.reconcile_watch_storage_until_stable(
        object(),
        object(),
        object(),
        blocked,
    )

    assert result is unblocked
    assert reconciled_items == [[], [replacement]]
    assert deletion_plans == [blocked, unblocked]
    assert relists == [True]


def test_storage_plan_fingerprint_ignores_list_order_and_status_churn(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    left = _storage_plan(items=[
        {
            "metadata": {
                "name": "mirage",
                "uid": "uid-mirage",
                "resourceVersion": "80",
            },
            "spec": {"type": "Replica1"},
            "status": {"phase": "Reconciling"},
        },
        {
            "metadata": {
                "name": "bellagio",
                "uid": "uid-bellagio",
                "resourceVersion": "79",
            },
            "spec": {"type": "Replica3"},
        },
    ])
    right = _storage_plan(items=[
        {
            "metadata": {
                "name": "bellagio",
                "uid": "uid-bellagio",
                "resourceVersion": "91",
            },
            "spec": {"type": "Replica3"},
            "status": {"phase": "Ready"},
        },
        {
            "metadata": {
                "name": "mirage",
                "uid": "uid-mirage",
                "resourceVersion": "92",
            },
            "spec": {"type": "Replica1"},
        },
    ])

    assert (
        operator._storage_plan_fingerprint(left)
        == operator._storage_plan_fingerprint(right)
    )


def test_storage_plan_fingerprint_tracks_legacy_orphan_disposition(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    obj = {"metadata": {"name": POOL_NAME}, "spec": {"type": "Replica1"}}
    quarantined = _storage_plan(deletion_orphans=[{
        "object": obj,
        "record": record,
        "legacy_quarantine": True,
    }])
    cleanup_owned = _storage_plan(deletion_orphans=[{
        "object": obj,
        "record": record,
        "legacy_storage_uid": f"orphan:{record['volume_id']}",
    }])

    assert (
        operator._storage_plan_fingerprint(quarantined)
        != operator._storage_plan_fingerprint(cleanup_owned)
    )


@pytest.mark.parametrize(
    "brick_indices",
    [
        [0, 0],
        [-1, 0],
        [0, 2],
    ],
    ids=["duplicate", "negative", "non-contiguous"],
)
def test_storage_upgrade_preflight_rejects_unsafe_legacy_brick_indices(
        monkeypatch, brick_indices):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(bricks=2)
    for brick, brick_index in zip(record["bricks"], brick_indices):
        brick["brick_index"] = brick_index
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    storage_list = {
        "items": [_storage_custom_object()],
        "metadata": {"resourceVersion": "118"},
    }

    with pytest.raises(RuntimeError, match="brick indices"):
        operator.prepare_storage_upgrade(
            core_client,
            object(),
            storage_list,
        )


def test_orphan_preflight_rejects_inconsistent_live_tolerations(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(bricks=2)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})

    class AppsClient:
        @staticmethod
        def read_namespaced_stateful_set(name, _namespace):
            key = "bellagio-one" if name.endswith("-0") else "bellagio-two"
            return _server_statefulset(tolerations=[SimpleNamespace(
                key=key,
                operator="Exists",
                effect="NoSchedule",
                value=None,
                toleration_seconds=None,
            )])

    with pytest.raises(RuntimeError, match="inconsistent.*tolerations"):
        operator.prepare_storage_upgrade(
            core_client,
            AppsClient(),
            {"items": [], "metadata": {"resourceVersion": "119"}},
        )


def test_orphan_preflight_uses_persisted_tolerations_when_servers_are_absent(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    tolerations = [{
        "key": "bellagio-fallback",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    record = _stored_native_pool(tolerations=tolerations)
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})

    class MissingStatefulSet(Exception):
        status = 404

    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: (_ for _ in ()).throw(
            MissingStatefulSet()
        ),
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "120"}},
    )

    assert plan["nodeplugin_tolerations"] == tolerations
    assert not plan["upgrade_objects"]


def test_orphan_preflight_accepts_explicit_empty_persisted_tolerations(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool(tolerations=[])
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})

    class MissingStatefulSet(Exception):
        status = 404

    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: (_ for _ in ()).throw(
            MissingStatefulSet()
        ),
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "120-empty"}},
    )

    assert not plan["nodeplugin_tolerations"]
    assert not plan["upgrade_objects"]


def test_zero_pv_orphan_without_servers_can_finish_cleanup(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})
    current_tolerations = [{
        "key": "bellagio-current",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]

    class MissingStatefulSet(Exception):
        status = 404

    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: (_ for _ in ()).throw(
            MissingStatefulSet()
        ),
        read_namespaced_daemon_set=lambda *_args: _nodeplugin_daemonset(
            tolerations=current_tolerations,
        ),
    )
    monkeypatch.setattr(operator, "get_num_pvs", lambda _core, _record: 0)

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "120-cleanup"}},
    )

    assert plan["nodeplugin_tolerations"] == current_tolerations
    assert not plan["upgrade_objects"]
    assert plan["deletion_orphans"][0]["record"] == record


def test_orphan_preflight_rejects_missing_toleration_source(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = _stored_native_pool()
    core_client = FakeCoreV1Client({f"{POOL_NAME}.info": record})

    class MissingStatefulSet(Exception):
        status = 404

    apps_client = SimpleNamespace(
        read_namespaced_stateful_set=lambda *_args: (_ for _ in ()).throw(
            MissingStatefulSet()
        ),
    )
    monkeypatch.setattr(operator, "get_num_pvs", lambda _core, _record: 1)

    with pytest.raises(RuntimeError, match="trustworthy toleration source"):
        operator.prepare_storage_upgrade(
            core_client,
            apps_client,
            {"items": [], "metadata": {"resourceVersion": "120-missing"}},
        )


def test_legacy_external_orphan_preserves_nodeplugin_tolerations(monkeypatch):
    operator = _load_operator(monkeypatch)
    tolerations = [{
        "key": "legacy-external-storage",
        "operator": "Exists",
        "effect": "NoSchedule",
    }]
    record = {
        "volname": "mirage-external",
        "type": "External",
        "volume_id": _pool_volume_id("mirage-external"),
        "gluster_hosts": "mirage.example.invalid",
        "gluster_volname": "mirage-vault",
    }
    core_client = FakeCoreV1Client({"mirage-external.info": record})
    calls = []
    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda name, namespace: (
            calls.append((name, namespace))
            or _nodeplugin_daemonset(tolerations=tolerations)
        ),
    )

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "120-external"}},
    )

    assert plan["nodeplugin_tolerations"] == tolerations
    assert not plan["upgrade_objects"]
    assert calls == [(operator.NODE_PLUGIN, operator.NAMESPACE)]


def test_zero_pv_external_orphan_can_finish_without_nodeplugin(monkeypatch):
    operator = _load_operator(monkeypatch)
    record = {
        "volname": "mirage-external",
        "type": "External",
        "volume_id": _pool_volume_id("mirage-external"),
        "gluster_hosts": "mirage.example.invalid",
        "gluster_volname": "mirage-vault",
    }
    core_client = FakeCoreV1Client({"mirage-external.info": record})

    class MissingDaemonSet(Exception):
        status = 404

    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda *_args: (_ for _ in ()).throw(
            MissingDaemonSet()
        ),
    )
    monkeypatch.setattr(operator, "get_num_pvs", lambda _core, _record: 0)

    plan = operator.prepare_storage_upgrade(
        core_client,
        apps_client,
        {"items": [], "metadata": {"resourceVersion": "121-cleanup"}},
    )

    assert not plan["nodeplugin_tolerations"]
    assert not plan["upgrade_objects"]
    assert plan["deletion_orphans"][0]["record"] == record


def test_zero_pv_external_fallback_is_not_reused_for_nonempty_orphan(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    zero_name = "bellagio-empty-external"
    live_name = "mirage-live-external"

    def record(name):
        return {
            "volname": name,
            "type": "External",
            "volume_id": _pool_volume_id(name),
            "gluster_hosts": f"{name}.example.invalid",
            "gluster_volname": f"{name}-vault",
        }

    core_client = FakeCoreV1Client({
        f"{zero_name}.info": record(zero_name),
        f"{live_name}.info": record(live_name),
    })

    class MissingDaemonSet(Exception):
        status = 404

    apps_client = SimpleNamespace(
        read_namespaced_daemon_set=lambda *_args: (_ for _ in ()).throw(
            MissingDaemonSet()
        ),
    )
    monkeypatch.setattr(
        operator,
        "get_num_pvs",
        lambda _core, stored: 0 if stored["volname"] == zero_name else 1,
    )

    with pytest.raises(RuntimeError, match="nodeplugin scheduling policy"):
        operator.prepare_storage_upgrade(
            core_client,
            apps_client,
            {"items": [], "metadata": {"resourceVersion": "122-mixed"}},
        )


def test_nodeplugin_toleration_reconciliation_propagates_patch_failure(
        monkeypatch):
    operator = _load_operator(monkeypatch)

    class PatchFailed(Exception):
        pass

    apps_client = SimpleNamespace(
        patch_namespaced_daemon_set=lambda *_args: (_ for _ in ()).throw(
            PatchFailed("casino API rejected the patch")
        ),
    )

    with pytest.raises(PatchFailed, match="casino API"):
        operator.reconcile_nodeplugin_tolerations(
            apps_client,
            [{"key": "bellagio", "operator": "Exists"}],
        )


def test_initial_storage_reconciliation_requires_explicit_success(monkeypatch):
    operator = _load_operator(monkeypatch)
    items = [
        _storage_custom_object(name="bellagio-one"),
        _storage_custom_object(name="bellagio-two", pool_type="External"),
    ]
    calls = []

    def handle_added(_core, obj, _apps, **_kwargs):
        calls.append(obj["metadata"]["name"])
        return obj["metadata"]["name"] == "bellagio-one"

    monkeypatch.setattr(operator, "handle_added", handle_added)

    with pytest.raises(RuntimeError, match="bellagio-two"):
        operator.reconcile_initial_storage(object(), items, object())

    assert calls == ["bellagio-one", "bellagio-two"]


def test_handle_added_propagates_external_reconciliation_result(monkeypatch):
    operator = _load_operator(monkeypatch)
    obj = {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "External",
            "details": {
                "gluster_host": "bellagio.example.invalid",
                "gluster_volname": "casino-vault",
            },
        },
    }
    core_client = FakeCoreV1Client({})
    monkeypatch.setattr(
        operator,
        "handle_external_storage_addition",
        lambda *_args: False,
    )

    assert operator.handle_added(core_client, obj) is False


def test_watch_stream_promotes_authoritative_snapshot_resource_version(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            pytest.fail("pre-reconciled watch unexpectedly listed resources")

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(method, *_args, **kwargs):
            assert method == custom_objects.list_namespaced_custom_object
            assert kwargs["resource_version"] == "999"
            return iter(())

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda received: custom_objects if received is k8s_client else None,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: _storage_plan(resource_version="999"),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda *_args: True,
        raising=False,
    )

    assert operator.watch_stream(
        object(),
        k8s_client,
        object(),
        resource_version="121",
    ) == "999"


def test_watch_quarantines_invalid_event_and_continues_to_valid_pool(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()
    invalid = {
        "metadata": {"name": "empty-bellagio", "resourceVersion": "8"},
    }
    valid = _storage_custom_object(name="mirage-valid")
    valid["metadata"]["resourceVersion"] = "9"
    handled = []

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            return None

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **_kwargs):
            return iter((
                {"type": "ADDED", "object": invalid},
                {"type": "ADDED", "object": valid},
            ))

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda _core, obj, _apps, **_kwargs: handled.append(obj) or True,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: next(storage_plans),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda *_args, **_kwargs: True,
    )

    storage_plans = iter([
        _storage_plan(resource_version="7"),
        _storage_plan(items=[valid], resource_version="12"),
    ])

    assert operator.watch_stream(
        object(),
        k8s_client,
        object(),
        resource_version="7",
    ) == "12"
    assert handled == [valid]


def test_watch_handles_deleted_event_without_spec(monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()
    deleted = {
        "metadata": {
            "name": "departed-bellagio",
            "resourceVersion": "18",
        },
    }
    handled = []

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            return None

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **_kwargs):
            return iter(({"type": "DELETED", "object": deleted},))

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "handle_deleted",
        lambda _core, obj, apps_v1_client=None: handled.append(obj) or True,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: next(storage_plans),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda *_args, **_kwargs: True,
    )

    storage_plans = iter([
        _storage_plan(resource_version="17"),
        _storage_plan(resource_version="21"),
    ])

    assert operator.watch_stream(
        object(),
        k8s_client,
        object(),
        resource_version="17",
    ) == "21"
    assert handled == [deleted]


def test_watch_restarts_after_newer_snapshot_before_buffered_events(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()
    added = _storage_custom_object(name="bellagio")
    added["metadata"]["resourceVersion"] = "8"
    modified = _storage_custom_object(
        name="mirage",
        pool_type="Replica3",
    )
    modified["metadata"]["resourceVersion"] = "9"
    events = [
        {"type": "ADDED", "object": added},
        {"type": "MODIFIED", "object": modified},
    ]
    stream_cursors = []
    handled = []

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            return None

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **kwargs):
            stream_cursors.append(kwargs["resource_version"])
            return iter(events)

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda *_args, **_kwargs: handled.append("added") or True,
    )
    monkeypatch.setattr(
        operator,
        "handle_modified",
        lambda *_args, **_kwargs: handled.append("modified") or True,
    )
    storage_plans = iter([
        _storage_plan(resource_version="7"),
        _storage_plan(resource_version="12"),
    ])
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: next(storage_plans),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda *_args: True,
        raising=False,
    )

    assert operator.watch_stream(
        object(),
        k8s_client,
        object(),
        resource_version="7",
    ) == "12"
    assert stream_cursors == ["7"]
    assert handled == ["added"]


def test_watch_replays_unacked_event_after_aggregate_failure(monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()
    added = _storage_custom_object(name="bellagio")
    added["metadata"]["resourceVersion"] = "8"
    event = {
        "type": "ADDED",
        "object": added,
    }
    stream_cursors = []
    aggregate_calls = 0

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            return None

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **kwargs):
            stream_cursors.append(kwargs["resource_version"])
            return iter([event])

    class AggregateFailed(Exception):
        pass

    def reconcile_union(*_args, **_kwargs):
        nonlocal aggregate_calls
        aggregate_calls += 1
        if aggregate_calls == 2:
            raise AggregateFailed("casino API failed")
        return _storage_plan(
            resource_version="12" if aggregate_calls == 4 else "7"
        )

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        reconcile_union,
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda *_args: True,
        raising=False,
    )

    with pytest.raises(AggregateFailed, match="casino API"):
        operator.watch_stream(
            object(),
            k8s_client,
            object(),
            resource_version="7",
        )

    assert operator.watch_stream(
        object(),
        k8s_client,
        object(),
        resource_version="7",
    ) == "12"
    assert stream_cursors == ["7", "7"]


def test_watch_timeout_retries_nonempty_deletion_orphans(monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()
    orphan = {
        "object": {"metadata": {"name": "bellagio-orphan"}},
        "record": _stored_native_pool("bellagio-orphan"),
    }
    deletion_calls = []
    stream_kwargs = []

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            return None

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **kwargs):
            stream_kwargs.append(kwargs)
            return iter(())

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _client: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: _storage_plan(
            deletion_orphans=[orphan],
            resource_version="12",
        ),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda core, orphans, apps: (
            deletion_calls.append((core, orphans, apps)) or True
        ),
        raising=False,
    )
    core_client = object()
    apps_client = object()

    assert operator.watch_stream(
        core_client,
        k8s_client,
        apps_client,
        resource_version="7",
    ) == "12"
    assert deletion_calls == [(core_client, [orphan], apps_client)]
    assert stream_kwargs == [{
        "resource_version": "12",
        "timeout_seconds": operator.WATCH_TIMEOUT_SECONDS,
        "allow_watch_bookmarks": True,
    }]


def test_watch_stream_propagates_runtime_union_reconciliation_failure(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()
    added = {
        "metadata": {"name": "bellagio-vault", "resourceVersion": "122"},
        "spec": {"type": "Replica1"},
    }

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            pytest.fail("pre-reconciled watch unexpectedly listed resources")

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **_kwargs):
            return iter([{"type": "ADDED", "object": added}])

    class PatchFailed(Exception):
        pass

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _received: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "handle_added",
        lambda *_args, **_kwargs: True,
    )
    aggregate_calls = 0

    def reconcile_union(*_args, **_kwargs):
        nonlocal aggregate_calls
        aggregate_calls += 1
        if aggregate_calls == 2:
            raise PatchFailed("Bellagio nodeplugin patch failed")
        return _storage_plan(resource_version="121")

    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        reconcile_union,
        raising=False,
    )

    with pytest.raises(PatchFailed, match="nodeplugin patch failed"):
        operator.watch_stream(
            object(),
            k8s_client,
            object(),
            resource_version="121",
        )


def test_watch_stream_raises_expired_error_object_for_safe_relist(monkeypatch):
    operator = _load_operator(monkeypatch)
    k8s_client = object()

    class CustomObjectsClient:
        @staticmethod
        def list_namespaced_custom_object(*_args, **_kwargs):
            pytest.fail("pre-reconciled watch unexpectedly listed resources")

    custom_objects = CustomObjectsClient()

    class FakeWatch:
        @staticmethod
        def stream(_method, *_args, **_kwargs):
            return iter([{
                "type": "ERROR",
                "object": {
                    "kind": "Status",
                    "code": 410,
                    "reason": "Expired",
                    "message": "too old resource version: 121",
                },
            }])

    monkeypatch.setattr(
        operator.client,
        "CustomObjectsApi",
        lambda _received: custom_objects,
        raising=False,
    )
    monkeypatch.setattr(operator.watch, "Watch", FakeWatch, raising=False)
    monkeypatch.setattr(
        operator,
        "reconcile_nodeplugin_from_storage",
        lambda *_args, **_kwargs: _storage_plan(resource_version="999"),
    )
    monkeypatch.setattr(
        operator,
        "reconcile_deletion_orphans",
        lambda *_args: True,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="too old resource version") as err:
        operator.watch_stream(
            object(),
            k8s_client,
            object(),
            resource_version="121",
        )

    assert err.value.status == 410


def test_crd_watch_relists_after_expired_resource_version(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []

    class ResourceVersionExpired(Exception):
        status = 410

    class WatchComplete(Exception):
        pass

    def watch_stream(_core, _k8s, _apps, resource_version=None):
        calls.append(resource_version)
        if len(calls) == 1:
            raise ResourceVersionExpired()
        raise WatchComplete()

    monkeypatch.setattr(operator, "watch_stream", watch_stream)

    with pytest.raises(WatchComplete):
        operator.crd_watch(
            object(),
            object(),
            object(),
            resource_version="122",
        )

    assert calls == ["122", None]
