"""Operator tests for durable ownership of whole-pool CSI volumes."""

import importlib
import json
import sys
import types
import uuid
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
    fake_kubernetes.client = SimpleNamespace()
    fake_kubernetes.config = SimpleNamespace()
    fake_kubernetes.watch = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "kubernetes", fake_kubernetes)
    sys.modules.pop("kadalu_operator.main", None)
    return importlib.import_module("kadalu_operator.main")


class FakeCoreV1Client:
    """Minimal CoreV1 API surface used by claim migration and config writes."""

    def __init__(self, records, persistent_volumes=(), pods=()):
        self.config_map = SimpleNamespace(data={
            key: json.dumps(value)
            for key, value in records.items()
        })
        self.persistent_volumes = list(persistent_volumes)
        self.pods = list(pods)
        self.patches = []
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

    def patch_persistent_volume(self, name, body):
        self.pv_patches.append((name, body))
        self.events.append(("persistent-volume", name))


def _persistent_volume(
        *,
        volume_id=VOLUME_ID,
        driver="kadalu",
        hostvol=POOL_NAME,
        single_pv_per_pool="true",
        path=None,
        pvtype=None,
        claim_namespace=None,
        claim_name=None,
        reclaim_policy="Delete"):
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
        metadata=SimpleNamespace(name=volume_id),
        spec=SimpleNamespace(
            csi=SimpleNamespace(
                driver=driver,
                volume_attributes=attributes,
                volume_handle=volume_id,
            ),
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


def _server_pod(volume_id=HOSTING_VOLUME_ID, *, ordinal=0):
    env = [] if volume_id is None else [SimpleNamespace(
        name="VOLUME_ID",
        value=volume_id,
    )]
    return SimpleNamespace(
        metadata=SimpleNamespace(name=f"server-{POOL_NAME}-0-{ordinal}"),
        spec=SimpleNamespace(containers=[SimpleNamespace(env=env)]),
    )


def _nodeplugin_daemonset(*, current=True):
    desired = 2
    return SimpleNamespace(
        metadata=SimpleNamespace(generation=4),
        status=SimpleNamespace(
            desired_number_scheduled=desired,
            updated_number_scheduled=desired if current else 1,
            number_ready=desired,
            observed_generation=4,
        ),
    )


def _legacy_pool_record():
    return {
        "volname": POOL_NAME,
        "type": "Replica1",
        "single_pv_per_pool": True,
        "bricks": [],
    }


def _native_pool_object(*, single_pv_per_pool=False, policy="delete"):
    return {
        "metadata": {"name": POOL_NAME},
        "spec": {
            "type": "Replica1",
            "volume_id": "bellagio-hosting-volume",
            "single_pv_per_pool": single_pv_per_pool,
            "pvReclaimPolicy": policy,
            "storage": [{
                "node_id": "node-eleven-ocean",
                "node": "bellagio-storage-one",
                "path": "/srv/bellagio-vault",
            }],
        },
    }


def _saved_record(client, pool_name=POOL_NAME):
    return json.loads(client.config_map.data[f"{pool_name}.info"])


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

    operator.handle_external_storage_addition(client, obj, object())

    record = _saved_record(client)
    assert record["single_pv_per_pool"] is True
    assert record["single_pv_claim_version"] == 1
    _assert_mount_metadata(operator, record)


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
        operator.handle_external_storage_addition(client, obj, object())
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
        "volume_id": "bellagio-existing-hosting-volume",
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
        lambda _client, obj: received.append(obj["spec"]["volume_id"]),
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

    assert received == ["bellagio-existing-hosting-volume"]


def test_added_existing_server_reconciles_offline_config_change(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = {
        "volname": POOL_NAME,
        "volume_id": "bellagio-hosting-volume",
        "type": "Replica1",
        "single_pv_per_pool": False,
        "pvReclaimPolicy": "delete",
        "bricks": [],
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
        lambda obj, _client: deployed_classes.append(obj["metadata"]["name"]),
    )
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda deployed: deployed_servers.append(deployed["metadata"]["name"]),
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

    operator.handle_added(client, obj)

    assert _saved_record(client)["pvReclaimPolicy"] == "archive"
    assert deployed_classes == [POOL_NAME]
    assert deployed_servers == [POOL_NAME]
    assert rendered_services == [
        str(Path(operator.MANIFESTS_DIR) / "services.yaml")
    ]
    assert commands == [(
        operator.KUBECTL_CMD,
        operator.APPLY_CMD,
        "-f",
        str(Path(operator.MANIFESTS_DIR) / "services.yaml"),
    )]


def test_missing_native_record_recovers_consistent_server_volume_id(
        monkeypatch):
    operator = _load_operator(monkeypatch)
    client = FakeCoreV1Client({})
    client.list_namespaced_pod = lambda _namespace: SimpleNamespace(items=[
        _server_pod(ordinal=0),
        _server_pod(ordinal=1),
    ])
    monkeypatch.setattr(operator, "deploy_storage_class", lambda *_args: None)
    deployed_servers = []
    monkeypatch.setattr(
        operator,
        "deploy_server_pods",
        lambda deployed: deployed_servers.append(deployed["metadata"]["name"]),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    obj = _native_pool_object()
    obj["spec"].pop("volume_id")

    operator.handle_added(client, obj)

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
        lambda deployed: deployed_servers.append(deployed["metadata"]["name"]),
    )
    monkeypatch.setattr(operator, "template", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(operator, "lib_execute", lambda *_args: None)
    obj = _native_pool_object()
    obj["spec"].pop("volume_id")

    operator.handle_added(client, obj)

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
        "volume_id": "bellagio-hosting-volume",
        "type": "Replica1",
        "single_pv_per_pool": False,
        "pvReclaimPolicy": (
            "delete" if existing_policy == "Delete" else "retain"
        ),
        "bricks": [],
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


def test_storage_class_policy_change_replaces_immutable_object(monkeypatch):
    operator = _load_operator(monkeypatch)
    existing = SimpleNamespace(
        metadata=SimpleNamespace(name=f"kadalu.{POOL_NAME}"),
        reclaim_policy="Delete",
    )
    storage_api = SimpleNamespace(
        list_storage_class=lambda: SimpleNamespace(items=[existing]),
    )
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
    )

    assert commands == [
        (
            operator.KUBECTL_CMD,
            operator.DELETE_CMD,
            "storageclass",
            f"kadalu.{POOL_NAME}",
            "--ignore-not-found=true",
            "--wait=true",
        ),
        (
            operator.KUBECTL_CMD,
            operator.APPLY_CMD,
            "-f",
            "/tmp/bellagio-storageclass.yaml",
        ),
    ]


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


def test_deploy_csi_rejects_multiple_serving_controllers(monkeypatch):
    operator = _load_operator(monkeypatch)

    with pytest.raises(ValueError, match="zero .* or one"):
        operator.deploy_csi_pods(object(), provisioner_replicas=2)


def test_subvolume_only_upgrade_does_not_delete_nodeplugin(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
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
        lambda _core, provisioner_replicas=1: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monkeypatch.setattr(operator.time, "sleep", lambda _seconds: None)

    operator.deploy_csi_upgrade(core_client, apps_client)

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
        "migrate",
        "deploy-csi-1",
    ]


def test_legacy_block_pv_aborts_before_mutation_and_keeps_controller_fenced(
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
    monkeypatch.setattr(
        operator,
        "quiesce_csi_provisioner",
        lambda *_args: calls.append("quiesce-provisioner"),
    )
    monkeypatch.setattr(
        operator,
        "deploy_csi_pods",
        lambda _core, provisioner_replicas=1: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monkeypatch.setattr(
        operator,
        "migrate_legacy_single_pv_claims",
        lambda *_args: calls.append("migrate"),
    )

    with pytest.raises(RuntimeError, match="PersistentVolumes still exist"):
        operator.deploy_csi_upgrade(core_client, apps_client)

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
    ]
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
        lambda _core, provisioner_replicas=1: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )
    monotonic_values = iter([0, operator.CSI_QUIESCE_TIMEOUT_SECONDS + 1])
    monkeypatch.setattr(
        operator.time,
        "monotonic",
        lambda: next(monotonic_values),
    )

    with pytest.raises(TimeoutError, match="drain or unpublish"):
        operator.deploy_csi_upgrade(core_client, apps_client)

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
        "migrate",
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
        lambda _core, provisioner_replicas=1: calls.append(
            f"deploy-csi-{provisioner_replicas}"
        ),
    )

    operator.deploy_csi_upgrade(core_client, apps_client)

    assert calls == [
        "quiesce-provisioner",
        "deploy-csi-0",
        "migrate",
        "deploy-csi-1",
    ]


def test_upgrade_quiesces_before_migration_and_csi_rollout(monkeypatch):
    operator = _load_operator(monkeypatch)
    calls = []
    core_client = SimpleNamespace(
        read_namespaced_config_map=lambda *_args: SimpleNamespace(data={}),
    )
    api_client = object()
    apps_client = object()
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
        lambda core, apps: calls.append("upgrade-csi")
        if core is core_client and apps is apps_client else None,
    )
    monkeypatch.setattr(
        operator,
        "upgrade_storage_pods",
        lambda received: calls.append("upgrade-storage")
        if received is core_client else None,
    )
    monkeypatch.setattr(operator, "crd_watch", lambda *_args: None)

    operator.main()

    assert calls.index("config-map") < calls.index("upgrade-csi")
    assert calls.index("upgrade-csi") < calls.index("upgrade-storage")


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
        metadata=SimpleNamespace(name=f"kadalu.{POOL_NAME}"),
        reclaim_policy="Delete",
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

    operator.handle_modified(core_client, obj)

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
    assert commands == [
        (
            operator.KUBECTL_CMD,
            operator.DELETE_CMD,
            "storageclass",
            f"kadalu.{POOL_NAME}",
            "--ignore-not-found=true",
            "--wait=true",
        ),
        (
            operator.KUBECTL_CMD,
            operator.APPLY_CMD,
            "-f",
            storage_class_path,
        ),
    ]


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
