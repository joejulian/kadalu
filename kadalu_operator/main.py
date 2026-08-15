"""
KaDalu Operator: Once started, deploys required CSI drivers,
bootstraps the ConfigMap and waits for the CRD update to create
Server pods
"""
import hashlib
import json
import logging
import os
import re
import time
import uuid

import urllib3
from jinja2 import Template

from kadalulib import CommandException
from kadalulib import execute as lib_execute
from kadalulib import (is_host_reachable, logf, logging_setup,
                       send_analytics_tracker, get_single_pv_per_pool)
from kubernetes import client, config, watch
from urllib3.exceptions import NewConnectionError, ProtocolError
from utils import CommandError
from utils import execute as utils_execute

NAMESPACE = os.environ.get("KADALU_NAMESPACE", "kadalu")
VERSION = os.environ.get("KADALU_VERSION", "latest")
K8S_DIST = os.environ.get("K8S_DIST", "kubernetes")
IMAGES_HUB = os.environ.get("IMAGES_HUB", "ghcr.io")
CSI_SIDECAR_REGISTRY = os.environ.get(
    "CSI_SIDECAR_REGISTRY", "registry.k8s.io"
)
BUSYBOX_IMAGE = os.environ.get(
    "BUSYBOX_IMAGE",
    "docker.io/library/busybox:1.37.0@sha256:"
    "9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0",
)
KUBELET_DIR = os.environ.get("KUBELET_DIR")
VERBOSE = os.environ.get("VERBOSE", "no")
TEMPLATES_DIR = os.environ.get("KADALU_TEMPLATES_DIR", "/kadalu/templates")
MANIFESTS_DIR = os.environ.get("KADALU_MANIFESTS_DIR", "/tmp/kadalu-manifests")
KUBECTL_CMD = "/usr/bin/kubectl"
KADALU_CONFIG_MAP = "kadalu-info"
CSI_POD_PREFIX = "csi-"
STORAGE_CLASS_NAME_PREFIX = "kadalu."
# TODO: Add ThinArbiter
VALID_HOSTING_VOLUME_TYPES = ["Replica1", "Replica2", "Replica3",
                              "Disperse", "External", "Arbiter"]
VALID_PV_RECLAIM_POLICY_TYPES = ["delete", "archive", "retain"]
VOLUME_TYPE_REPLICA_1 = "Replica1"
VOLUME_TYPE_REPLICA_2 = "Replica2"
VOLUME_TYPE_REPLICA_3 = "Replica3"
VOLUME_TYPE_EXTERNAL = "External"
VOLUME_TYPE_DISPERSE = "Disperse"
VOLUME_TYPE_ARBITER = "Arbiter"

CREATE_CMD = "create"
APPLY_CMD = "apply"
DELETE_CMD = "delete"
PATCH_CMD = "patch"

NODE_PLUGIN = "kadalu-csi-nodeplugin"
CSI_PROVISIONER = "kadalu-csi-provisioner"
CSI_DRIVER_NAME = "kadalu"
SINGLE_PV_CLAIM_VERSION = 1
CSI_QUIESCE_TIMEOUT_SECONDS = 120
BLOCK_PV_TYPES = {"virtblock", "rawblock"}


def template(filename, **kwargs):
    """Substitute the template with provided fields"""
    template_filename = os.path.join(
        TEMPLATES_DIR,
        os.path.basename(filename) + ".j2",
    )
    with open(template_filename, encoding="utf-8") as template_file:
        content = template_file.read()

    if kwargs.get("render", False):
        return Template(content).render(**kwargs)

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    return Template(content).stream(**kwargs).dump(filename)


# TODO: Validate given options using kadalu/volgen API
def options_validation(options):
    """ Validate Pool Options """

    for option in options:
        if option.get("key", None) is None and \
           option.get("value", None) is None:
            logging.error(logf("Key/Value not specified for Storage Pool Options"))
            return False

    return True


def bricks_validation(bricks):
    """Validate Brick path and node options"""
    ret = True
    for idx, brick in enumerate(bricks):
        if not ret:
            break

        if brick.get("pvc", None) is not None:
            continue

        if brick.get("path", None) is None and \
           brick.get("device", None) is None:
            logging.error(logf("Storage path/device not specified",
                               number=idx+1))
            ret = False

        if brick.get("node", None) is None:
            logging.error(logf("Storage node not specified", number=idx+1))
            ret = False

    return ret


def validate_ext_details(obj):
    """Validate external Volume details"""
    cluster = obj["spec"].get("details", None)
    if not cluster:
        logging.error(logf("External Cluster details not given."))
        return False

    valid = 0
    ghosts = []
    gport = 24007
    if cluster.get('gluster_hosts', None):
        valid += 1
        hosts = cluster.get('gluster_hosts')
        ghosts.extend(hosts)
    if cluster.get('gluster_host', None):
        valid += 1
        ghosts.append(cluster.get('gluster_host'))
    if cluster.get('gluster_volname', None):
        valid += 1
    if cluster.get('gluster_port', None):
        gport = cluster.get('gluster_port', 24007)

    if valid < 2:
        logging.error(logf("No 'host' and 'volname' details provided."))
        return False

    if not is_host_reachable(ghosts, gport):
        logging.error(logf("gluster server not reachable: on %s:%d" %
                           (ghosts, gport)))
        #  Noticed that there may be glitches in n/w during this time.
        #  Not good to fail the validation, instead, just log here, so
        #  we are aware this is a possible reason.
        #return False

    logging.debug(logf("External Storage %s successfully validated" % \
                       obj["metadata"].get("name", "<unknown>")))
    return True


# pylint: disable=too-many-return-statements
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=too-many-locals
def validate_volume_request(obj):
    """Validate the Volume request for Replica options, number of bricks etc"""
    if not obj.get("spec", None):
        logging.error("Storage 'spec' not specified")
        return False

    pv_reclaim_policy = obj["spec"].get("pvReclaimPolicy", "delete")
    if pv_reclaim_policy not in VALID_PV_RECLAIM_POLICY_TYPES:
        logging.error("PV Reclaim Policy not valid")
        return False

    if (
            get_single_pv_per_pool(obj["spec"])
            and pv_reclaim_policy == "archive"):
        logging.error(
            "PV Reclaim Policy 'archive' is not supported for "
            "single_pv_per_pool"
        )
        return False

    voltype = obj["spec"].get("type", None)
    if voltype is None:
        logging.error("Storage type not specified")
        return False

    if voltype not in VALID_HOSTING_VOLUME_TYPES:
        logging.error(logf("Invalid Storage type",
                           valid_types=",".join(VALID_HOSTING_VOLUME_TYPES),
                           provided_type=voltype))
        return False

    if voltype == VOLUME_TYPE_EXTERNAL:
        return validate_ext_details(obj)

    options = obj["spec"].get("options", [])
    if not options_validation(options):
        return False

    bricks = obj["spec"].get("storage", [])
    if not bricks_validation(bricks):
        return False

    decommissioned = ""
    subvol_bricks_count = 1
    if voltype == VOLUME_TYPE_REPLICA_2:
        subvol_bricks_count = 2
    elif voltype == VOLUME_TYPE_REPLICA_3:
        subvol_bricks_count = 3

    if voltype == VOLUME_TYPE_DISPERSE:
        disperse_config = obj["spec"].get("disperse", None)
        if disperse_config is None:
            logging.error("Disperse Volume data and redundancy "
                          "count is not specified")
            return False

        data_bricks = disperse_config.get("data", 0)
        redundancy_bricks = disperse_config.get("redundancy", 0)
        if data_bricks == 0 or redundancy_bricks == 0:
            logging.error("Disperse Volume data or redundancy "
                          "count is not specified")
            return False

        subvol_bricks_count = data_bricks + redundancy_bricks
        # redundancy must be greater than 0, and the total number
        # of bricks must be greater than 2 * redundancy. This
        # means that a dispersed volume must have a minimum of 3 bricks.
        if subvol_bricks_count <= (2 * redundancy_bricks):
            logging.error("Invalid redundancy for the Disperse Volume")
            return False

        # stripe_size = (bricks_count - redundancy) * 512
        # Using combinations of #Bricks/redundancy that give a power
        # of two for the stripe size will make the disperse volume
        # perform better in most workloads because it's more typical
        # to write information in blocks that are multiple of two
        # https://docs.gluster.org/en/latest/Administrator-Guide
        #    /Setting-Up-Volumes/#creating-dispersed-volumes
        if data_bricks % 2 != 0:
            logging.error("Disperse Configuration is not Optimal")
            return False

    if len(bricks) % subvol_bricks_count != 0:
        logging.error("Invalid number of storage directories/devices"
                      " specified")
        return False

    if subvol_bricks_count > 1:
        for i in range(0, int(len(bricks) / subvol_bricks_count)):
            decommissioned = ""
            for k in range(0, subvol_bricks_count):
                brick_idx = (i * subvol_bricks_count) + k
                brick = bricks[brick_idx]
                decom = brick.get("decommissioned", "")
                if k == 0:
                    decommissioned = decom
                    continue
                if decom != decommissioned:
                    logging.error(logf(
                        "All of distribute subvolume should be marked decommissioned",
                        brick=brick, brick_index=brick_idx))
                    return False

    # If we are here, decommissioned option is properly given.

    if voltype == VOLUME_TYPE_REPLICA_2:
        tiebreaker = obj["spec"].get("tiebreaker", None)
        if tiebreaker and (not tiebreaker.get("node", None) or
                           not tiebreaker.get("path", None)):
            logging.error(logf("'tiebreaker' provided for replica2 "
                               "config is not valid"))
            return False

    if voltype == VOLUME_TYPE_ARBITER:
        # Arbiter volume require atleast 2 data bricks and atmost 1 arbiter brick,
        # Per each distribute group.
        subvol_bricks_count = 2 + 1
        if len(bricks) < 3 or len(bricks) % subvol_bricks_count != 0:
            logging.error("Invalid number of storage directories/devices"
                          " specified for volume type 'Arbiter'")
            return False

    logging.debug(logf("Storage %s successfully validated" % \
                       obj["metadata"].get("name", "<unknown>")))
    return True


def get_brick_device_dir(brick):
    """If custom file is passed as brick device then the
    parent directory needs to be mounted as is
    in server container"""
    brick_device_dir = ""
    logging.info(repr(brick))
    brickdev = brick.get("device", "")
    logging.info(brickdev)
    if brickdev != "" and not brickdev.startswith("/dev/"):
        brick_device_dir = os.path.dirname(brickdev)

    return brick_device_dir


def get_brick_hostname(volname, idx, suffix=True):
    """Brick hostname is <statefulset-name>-<ordinal>.<service-name>
    statefulset name is the one which is visible when the
    `get pods` command is run, so the format used for that name
    is "server-<volname>-<idx>". Escape dots from the
    hostname from the input otherwise will become invalid name.
    Service is created with name as Volume name. For example,
    brick_hostname will be "server-spool1-0-0.spool1" and
    server pod name will be "server-spool1-0"
    """
    dns_friendly_volname = _dns_friendly_volname(volname)
    hostname = "server-%s-%d" % (dns_friendly_volname, idx)
    if suffix:
        return "%s-0.%s" % (hostname, volname)

    return hostname


def _dns_friendly_volname(volname):
    """Return the volume-name fragment used by server StatefulSets."""
    tmp_vol = volname.replace("-", "_")
    return re.sub(r'\W+', '', tmp_vol).replace("_", "-")


def is_server_pod_for_volume(pod_name, volname):
    """Return whether a pod is one of the volume's brick StatefulSet pods."""
    volume_fragment = re.escape(_dns_friendly_volname(volname))
    return re.fullmatch(
        rf"server-{volume_fragment}-\d+-\d+",
        pod_name,
    ) is not None


def recover_server_volume_id(pods, volname):
    """Recover one hosting UUID from every existing brick server pod."""
    matching_pods = [
        pod for pod in pods
        if is_server_pod_for_volume(pod.metadata.name, volname)
    ]
    if not matching_pods:
        return None

    recovered = set()
    for pod in matching_pods:
        pod_values = set()
        for container in getattr(pod.spec, "containers", None) or []:
            for variable in getattr(container, "env", None) or []:
                if getattr(variable, "name", None) == "VOLUME_ID":
                    value = getattr(variable, "value", None)
                    if value:
                        pod_values.add(value)
        if len(pod_values) != 1:
            raise ValueError(
                f"Server pod {pod.metadata.name} has no unique VOLUME_ID"
            )
        recovered.update(pod_values)

    if len(recovered) != 1:
        raise ValueError("Existing server pods disagree on VOLUME_ID")
    volume_id = next(iter(recovered))
    try:
        if str(uuid.UUID(volume_id)) != volume_id:
            raise ValueError
    except (AttributeError, TypeError, ValueError) as err:
        raise ValueError(
            "Existing server pods contain an invalid VOLUME_ID"
        ) from err
    return volume_id


def _canonical_gluster_hosts(hosts):
    """Normalize Gluster endpoints without making their order identity."""
    if isinstance(hosts, str):
        hosts = hosts.split(",")
    if not isinstance(hosts, (list, tuple)):
        return []
    return sorted({str(host).strip() for host in hosts if str(host).strip()})


def mount_config_fingerprint(data):
    """Return a stable digest of the backend configuration used by a mount."""
    canonical = {
        "schema": 1,
        "type": data.get("type"),
        "volname": data.get("volname"),
        "volume_id": data.get("volume_id"),
        "single_pv_per_pool": get_single_pv_per_pool(data),
    }
    if data.get("type") == VOLUME_TYPE_EXTERNAL:
        canonical.update({
            "gluster_hosts": _canonical_gluster_hosts(
                data.get("gluster_hosts", "")
            ),
            "gluster_volname": data.get("gluster_volname"),
            "gluster_options": data.get("gluster_options", ""),
        })
    else:
        brick_fields = (
            "brick_device",
            "brick_index",
            "brick_path",
            "decommissioned",
            "host_brick_path",
            "kube_hostname",
            "node",
            "node_id",
            "pvc_name",
        )
        canonical.update({
            "bricks": [
                {
                    field: brick.get(field)
                    for field in brick_fields
                }
                for brick in data.get("bricks", [])
            ],
            "disperse": data.get("disperse", {}),
            "options": data.get("options", {}),
            "tiebreaker": data.get("tiebreaker", {}),
        })

    serialized = json.dumps(
        canonical,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _external_backend_identity(data):
    """Return the immutable connection identity for an external pool."""
    return {
        "gluster_hosts": _canonical_gluster_hosts(
            data.get("gluster_hosts", "")
        ),
        "gluster_volname": data.get("gluster_volname"),
        "gluster_options": data.get("gluster_options", ""),
    }


def _decode_pool_record(existing_serialized):
    """Decode one existing pool record, returning a reason on failure."""
    try:
        existing = json.loads(existing_serialized)
    except (TypeError, ValueError) as err:
        logging.error(logf(
            "Existing storage pool metadata is invalid",
            error=err,
        ))
        return None
    if not isinstance(existing, dict):
        logging.error("Existing storage pool metadata is not an object")
        return None
    return existing


def _preserve_single_pv_claim_metadata(
        data, existing, recovered_legacy_owner=None):
    """Apply immutable ownership metadata from a decoded pool record."""
    data.pop("single_pv_claim_version", None)
    data.pop("legacy_single_pv_volume_id", None)
    if existing is None:
        if get_single_pv_per_pool(data):
            if recovered_legacy_owner is None:
                data["single_pv_claim_version"] = SINGLE_PV_CLAIM_VERSION
            else:
                data["legacy_single_pv_volume_id"] = recovered_legacy_owner
        return True

    existing_mode = get_single_pv_per_pool(existing)
    requested_mode = get_single_pv_per_pool(data)
    if requested_mode != existing_mode:
        logging.error(logf(
            "Rejected immutable single_pv_per_pool change",
            volname=data.get("volname"),
            existing=existing_mode,
            requested=requested_mode,
        ))
        return False
    if not existing_mode:
        return True
    if (
            existing.get("single_pv_claim_version")
            == SINGLE_PV_CLAIM_VERSION):
        data["single_pv_claim_version"] = SINGLE_PV_CLAIM_VERSION
        return True
    legacy_owner = existing.get("legacy_single_pv_volume_id")
    if legacy_owner:
        data["legacy_single_pv_volume_id"] = legacy_owner
    return True


def preserve_single_pv_claim_metadata(data, existing_serialized):
    """Keep ownership generation stable when reconciling an existing pool."""
    if existing_serialized is None:
        return _preserve_single_pv_claim_metadata(data, None)
    existing = _decode_pool_record(existing_serialized)
    if existing is None:
        return False
    return _preserve_single_pv_claim_metadata(data, existing)


def _canonical_mount_identity(value):
    """Return the canonical UUID form of a stored mount identity."""
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        return None


def _apply_pool_mount_metadata(
        data, existing, recovered_legacy_owner=None,
        legacy_mount_migration=False):
    """Preserve one pool incarnation and fingerprint its current backend."""
    legacy_fingerprint = None
    if existing is not None:
        legacy_fingerprint = existing.get(
            "legacy_mount_config_fingerprint"
        )
    # This marker is operator-owned. Fresh pools must never inherit one from
    # request data, while reconciles must preserve an actual migration marker.
    data.pop("legacy_mount_config_fingerprint", None)
    if legacy_fingerprint is not None:
        data["legacy_mount_config_fingerprint"] = legacy_fingerprint

    if not _preserve_single_pv_claim_metadata(
            data, existing, recovered_legacy_owner):
        return False

    if existing is not None:
        for field in ("type", "volname", "volume_id"):
            existing_value = existing.get(field)
            if (
                    existing_value is not None
                    and data.get(field) != existing_value):
                logging.error(logf(
                    "Rejected immutable storage pool identity change",
                    field=field,
                    existing=existing_value,
                    requested=data.get(field),
                ))
                return False
        if (
                existing.get("type") == VOLUME_TYPE_EXTERNAL
                and _external_backend_identity(data)
                != _external_backend_identity(existing)):
            logging.error(logf(
                "Rejected immutable external storage backend change",
                volname=data.get("volname"),
            ))
            return False

    mount_identity = None
    if existing is not None and "mount_identity" in existing:
        mount_identity = _canonical_mount_identity(
            existing.get("mount_identity")
        )
        if mount_identity is None:
            logging.error(logf(
                "Rejected invalid storage pool mount identity",
                volname=data.get("volname"),
            ))
            return False
    if mount_identity is None:
        mount_identity = str(uuid.uuid4())

    if legacy_mount_migration:
        if existing is None or "mount_identity" in existing:
            logging.error("Legacy mount migration requires an identity-less pool")
            return False
        data["legacy_mount_config_fingerprint"] = mount_config_fingerprint(data)

    data["mount_identity"] = mount_identity
    data["mount_config_fingerprint"] = mount_config_fingerprint(data)
    return True


def apply_pool_mount_metadata(
        data, existing_serialized, recovered_legacy_owner=None):
    """Prepare safe ownership and mount identity for a ConfigMap write."""
    if existing_serialized is None:
        return _apply_pool_mount_metadata(
            data,
            None,
            recovered_legacy_owner,
        )
    existing = _decode_pool_record(existing_serialized)
    if existing is None:
        return False
    return _apply_pool_mount_metadata(data, existing)


def discover_single_pv_owners(core_v1_client):
    """Return existing whole-pool Kadalu PV owners grouped by hosting pool."""
    claims_by_hostvol = {}
    persistent_volumes = core_v1_client.list_persistent_volume().items or []
    for persistent_volume in persistent_volumes:
        spec = getattr(persistent_volume, "spec", None)
        csi_source = getattr(spec, "csi", None)
        if (
                csi_source is None
                or getattr(csi_source, "driver", None) != CSI_DRIVER_NAME):
            continue
        attributes = getattr(csi_source, "volume_attributes", None) or {}
        if (
                not isinstance(attributes, dict)
                or not get_single_pv_per_pool(attributes)
                or attributes.get("path")):
            continue
        hostvol = attributes.get("hostvol")
        volume_id = getattr(csi_source, "volume_handle", None)
        if hostvol and volume_id:
            claims_by_hostvol.setdefault(hostvol, set()).add(volume_id)
    return claims_by_hostvol


def _pool_persistent_volumes(core_v1_client, volname):
    """Yield Kadalu PVs provisioned from one hosting pool."""
    persistent_volumes = core_v1_client.list_persistent_volume().items or []
    for persistent_volume in persistent_volumes:
        spec = getattr(persistent_volume, "spec", None)
        csi_source = getattr(spec, "csi", None)
        attributes = getattr(csi_source, "volume_attributes", None) or {}
        if (
                csi_source is not None
                and getattr(csi_source, "driver", None) == CSI_DRIVER_NAME
                and isinstance(attributes, dict)
                and attributes.get("hostvol") == volname):
            yield persistent_volume


def reconcile_pool_pv_reclaim_policy(
        core_v1_client, volname, pool_policy, before_config_write):
    """Patch existing PV policy on the data-safe side of a config write."""
    reclaim_policy = storage_class_reclaim_policy(pool_policy)
    if (
            before_config_write is not None
            and (reclaim_policy == "Retain") != before_config_write):
        return

    for persistent_volume in _pool_persistent_volumes(
            core_v1_client, volname):
        spec = persistent_volume.spec
        current_policy = getattr(
            spec,
            "persistent_volume_reclaim_policy",
            None,
        ) or "Delete"
        if current_policy == reclaim_policy:
            continue
        name = persistent_volume.metadata.name
        core_v1_client.patch_persistent_volume(
            name,
            {"spec": {"persistentVolumeReclaimPolicy": reclaim_policy}},
        )
        logging.info(logf(
            "Updated PersistentVolume reclaim policy",
            name=name,
            hostvol=volname,
            policy=reclaim_policy,
        ))


def recovered_single_pv_owner(core_v1_client, data):
    """Recover one legacy owner when a pool ConfigMap record is absent."""
    if not get_single_pv_per_pool(data):
        return None, True

    owners = discover_single_pv_owners(core_v1_client).get(
        data.get("volname"),
        set(),
    )
    if len(owners) > 1:
        logging.error(logf(
            "Refusing to recreate ambiguous single-PV pool metadata",
            volname=data.get("volname"),
            matching_persistent_volumes=len(owners),
        ))
        return None, False
    return (next(iter(owners)) if owners else None), True


def apply_pool_mount_metadata_for_write(
        core_v1_client, data, existing_serialized):
    """Prepare a ConfigMap write, recovering ownership if its record vanished."""
    recovered_owner = None
    if existing_serialized is None:
        recovered_owner, valid = recovered_single_pv_owner(
            core_v1_client,
            data,
        )
        if not valid:
            return False
    return apply_pool_mount_metadata(
        data,
        existing_serialized,
        recovered_owner,
    )


def _kadalu_block_volume(persistent_volume):
    """Return the PV and pool identities for a Kadalu block volume."""
    spec = getattr(persistent_volume, "spec", None)
    csi_source = getattr(spec, "csi", None)
    if (
            csi_source is None
            or getattr(csi_source, "driver", None) != CSI_DRIVER_NAME):
        return None

    attributes = getattr(csi_source, "volume_attributes", None) or {}
    if (
            not isinstance(attributes, dict)
            or attributes.get("pvtype") not in BLOCK_PV_TYPES):
        return None
    return persistent_volume.metadata.name, attributes.get("hostvol")


def _legacy_mount_pools(configmap_records):
    """Return identity-less hosting pools from validated ConfigMap records."""
    legacy_pools = set()
    for key, serialized in (configmap_records or {}).items():
        if not key.endswith(".info"):
            continue
        record = _decode_pool_record(serialized)
        if record is None:
            raise ValueError(f"Invalid storage pool metadata in {key}")
        if "mount_identity" not in record:
            legacy_pools.add(key[:-len(".info")])
    return legacy_pools


def ensure_kadalu_block_volumes_absent(core_v1_client, reason):
    """Reject a mount-boundary change while any block PV still exists."""
    block_volumes = []
    for persistent_volume in (
            core_v1_client.list_persistent_volume().items or []):
        block_volume = _kadalu_block_volume(persistent_volume)
        if block_volume is not None:
            block_volumes.append(block_volume)

    if not block_volumes:
        return
    details = ", ".join(
        f"{pv_name} (pool {pool_name or 'unknown'})"
        for pv_name, pool_name in sorted(set(block_volumes))
    )
    raise RuntimeError(
        f"Cannot {reason} while these Kadalu block PersistentVolumes still "
        f"exist: {details}. Keep the CSI provisioner quiesced until the block "
        "volumes can be removed; stopping workloads alone is not sufficient "
        "because an old block target may lack recovery state."
    )


def legacy_mount_migration_required(configmap_records):
    """Return whether any hosting pool still lacks a mount identity."""
    return bool(_legacy_mount_pools(configmap_records))


def migrate_legacy_single_pv_claims(core_v1_client):
    """Prepare legacy ownership and mount identity before CSI starts."""
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)
    if legacy_mount_migration_required(configmap_data.data):
        ensure_kadalu_block_volumes_absent(
            core_v1_client,
            "migrate legacy mount identities",
        )
    claims_by_hostvol = discover_single_pv_owners(core_v1_client)

    changed = False
    for key, serialized in (configmap_data.data or {}).items():
        if not key.endswith(".info"):
            continue
        existing = _decode_pool_record(serialized)
        if existing is None:
            raise ValueError(f"Invalid storage pool metadata in {key}")
        data = dict(existing)
        if not _apply_pool_mount_metadata(
                data,
                existing,
                legacy_mount_migration="mount_identity" not in existing):
            raise ValueError(f"Unable to preserve storage pool metadata in {key}")

        if (
                get_single_pv_per_pool(data)
                and data.get("single_pv_claim_version")
                != SINGLE_PV_CLAIM_VERSION):
            volname = key[:-len(".info")]
            owners = claims_by_hostvol.get(volname, set())
            legacy_owner = data.get("legacy_single_pv_volume_id")
            if legacy_owner:
                if owners and owners != {legacy_owner}:
                    logging.warning(logf(
                        "Preserving legacy single-PV owner despite "
                        "conflicting PersistentVolumes",
                        volname=volname,
                        owner=legacy_owner,
                        matching_persistent_volumes=len(owners),
                    ))
            elif len(owners) == 1:
                data["legacy_single_pv_volume_id"] = next(iter(owners))
            else:
                logging.warning(logf(
                    "Legacy single-PV pool ownership is ambiguous",
                    volname=volname,
                    matching_persistent_volumes=len(owners),
                ))

        if data != existing:
            configmap_data.data[key] = json.dumps(data)
            changed = True

    if changed:
        core_v1_client.patch_namespaced_config_map(
            KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
        logging.info(
            "Prepared legacy pool ownership and mount identity before CSI rollout"
        )


def upgrade_storage_pods(core_v1_client):
    """
    Upgrade the Storage pods after operator pod upgrade
    """
    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)

    for key in configmap_data.data:
        if ".info" not in key:
            continue

        volname = key.replace('.info', '')
        data = json.loads(configmap_data.data[key])

        logging.info(logf("config map", volname=volname, data=data))
        if data['type'] == VOLUME_TYPE_EXTERNAL:
            # nothing to be done for upgrade, say we are good.
            logging.debug(logf(
                "volume type external, nothing to upgrade",
                volname=volname,
                data=data))
            continue

        if data['type'] == VOLUME_TYPE_REPLICA_1:
            # No promise of high availability, upgrade
            logging.debug(logf(
                "volume type Replica1, calling upgrade",
                volname=volname,
                data=data))
            # TODO: call upgrade

        # Replica 2 and Replica 3 needs to check for self-heal
        # count 0 before going ahead with upgrade.

        # glfsheal volname --file-path=/template/file info-summary
        obj = {}
        obj["metadata"] = {}
        obj["spec"] = {}
        obj["metadata"]["name"] = volname
        obj["spec"]["type"] = data['type']
        obj["spec"]["pvReclaimPolicy"] = data.get("pvReclaimPolicy", "delete")
        obj["spec"]["volume_id"] = data["volume_id"]
        obj["spec"]["storage"] = []

        # Need this loop so below array can be constructed in the proper order
        for val in data["bricks"]:
            obj["spec"]["storage"].append({})

        # Set Node ID for each storage device from configmap
        for val in data["bricks"]:
            idx = val["brick_index"]

            obj["spec"]["storage"][idx]["node_id"] = val["node_id"]
            obj["spec"]["storage"][idx]["path"] = val["host_brick_path"]
            obj["spec"]["storage"][idx]["node"] = val["kube_hostname"]
            obj["spec"]["storage"][idx]["device"] = val["brick_device"]
            obj["spec"]["storage"][idx]["pvc"] = val["pvc_name"]

        # TODO: call upgrade_pods_with_heal_check() here
        deploy_server_pods(obj)


def update_config_map(core_v1_client, obj):
    """
    Volinfo of new hosting Volume is generated and updated to ConfigMap
    """
    volname = obj["metadata"]["name"]
    voltype = obj["spec"]["type"]
    pv_reclaim_policy = obj["spec"].get("pvReclaimPolicy", "delete")
    volume_id = obj["spec"]["volume_id"]
    disperse_config = obj["spec"].get("disperse", {})

    data = {
        "namespace": NAMESPACE,
        "kadalu_version": VERSION,
        "volname": volname,
        "volume_id": volume_id,
        "single_pv_per_pool": get_single_pv_per_pool(obj["spec"]),
        "type": voltype,
        "pvReclaimPolicy" : pv_reclaim_policy,
        "bricks": [],
        "disperse": {
            "data": disperse_config.get("data", 0),
            "redundancy": disperse_config.get("redundancy", 0)
        },
        "options": {}
    }
    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)
    volinfo_file = "%s.info" % volname
    existing_serialized = configmap_data.data.get(volinfo_file)

    # Add options entry as key:value
    if obj["spec"].get("options", None):
        options = obj["spec"]["options"]
        for option in options:
            data["options"].update({
                option.get("key"): option.get("value")
            })

    # For each brick, add brick path and node id
    bricks = obj["spec"]["storage"]
    for idx, storage in enumerate(bricks):
        data["bricks"].append({
            "brick_path": "/bricks/%s/data/brick" % volname,
            "kube_hostname": storage.get("node", ""),
            "node": get_brick_hostname(volname, idx),
            "node_id": storage["node_id"],
            "host_brick_path": storage.get("path", ""),
            "brick_device": storage.get("device", ""),
            "pvc_name": storage.get("pvc", ""),
            "brick_device_dir": get_brick_device_dir(storage),
            "decommissioned": storage.get("decommissioned", ""),
            "brick_index": idx
        })

    if voltype == VOLUME_TYPE_REPLICA_2:
        tiebreaker = obj["spec"].get("tiebreaker", None)
        if not tiebreaker:
            data["tiebreaker"] = {}
        else:
            if not tiebreaker.get("port", None):
                tiebreaker["port"] = 24007

            data["tiebreaker"] = tiebreaker

    if not apply_pool_mount_metadata_for_write(
            core_v1_client, data, existing_serialized):
        return False

    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=True,
    )
    configmap_data.data[volinfo_file] = json.dumps(data)

    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=False,
    )
    logging.info(logf("Updated configmap", name=KADALU_CONFIG_MAP,
                      volname=volname))
    return True


def deploy_server_pods(obj):
    """
    Deploy server pods depending on type of Hosting
    Volume and other options specified
    """
    # Deploy server pod
    volname = obj["metadata"]["name"]
    voltype = obj["spec"]["type"]
    pv_reclaim_policy = obj["spec"].get("pvReclaimPolicy", "delete")
    tolerations = obj["spec"].get("tolerations")
    docker_user = os.environ.get("DOCKER_USER", "joejulian")

    shd_required = False
    if voltype in (VOLUME_TYPE_REPLICA_3, VOLUME_TYPE_REPLICA_2,
                   VOLUME_TYPE_DISPERSE):
        shd_required = True

    template_args = {
        "namespace": NAMESPACE,
        "kadalu_version": VERSION,
        "images_hub": IMAGES_HUB,
        "docker_user": docker_user,
        "volname": volname,
        "voltype": voltype,
        "pvReclaimPolicy": pv_reclaim_policy,
        "volume_id": obj["spec"]["volume_id"],
        "shd_required": shd_required
    }

    # One StatefulSet per Brick
    for idx, storage in enumerate(obj["spec"]["storage"]):
        template_args["host_brick_path"] = storage.get("path", "")
        template_args["kube_hostname"] = storage.get("node", "")
        # TODO: Understand the need, and usage of suffix
        template_args["serverpod_name"] = get_brick_hostname(
            volname,
            idx,
            suffix=False
        )
        template_args["brick_path"] = "/bricks/%s/data/brick" % volname
        template_args["brick_index"] = idx
        template_args["brick_device"] = storage.get("device", "")
        template_args["pvc_name"] = storage.get("pvc", "")
        template_args["brick_device_dir"] = get_brick_device_dir(storage)
        template_args["brick_node_id"] = storage["node_id"]
        template_args["k8s_dist"] = K8S_DIST
        template_args["verbose"] = VERBOSE
        template_args["tolerations"] = tolerations

        filename = os.path.join(MANIFESTS_DIR, "server.yaml")
        template(filename, **template_args)
        lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
        logging.info(logf("Deployed Server pod",
                          volname=volname,
                          manifest=filename,
                          node=storage.get("node", "")))
    add_tolerations("daemonset", NODE_PLUGIN, tolerations)


def handle_external_storage_addition(core_v1_client, obj, storage_api=None):
    """Deploy service(One service per Volume)"""
    volname = obj["metadata"]["name"]
    details = obj["spec"]["details"]
    pv_reclaim_policy = obj["spec"].get("pvReclaimPolicy", "delete")
    tolerations = obj["spec"].get("tolerations")

    hosts = []
    ghost = details.get("gluster_host", None)
    ghosts = details.get("gluster_hosts", None)
    if ghost:
        hosts.append(ghost)
    if ghosts:
        hosts.extend(ghosts)

    data = {
        "volname": volname,
        "volume_id": obj["spec"]["volume_id"],
        "type": VOLUME_TYPE_EXTERNAL,
        "pvReclaimPolicy": pv_reclaim_policy,
        # CRD would set 'native' but just being cautious
        "single_pv_per_pool": get_single_pv_per_pool(obj["spec"]),
        "gluster_hosts": ",".join(hosts),
        "gluster_volname": details["gluster_volname"],
        "gluster_options": details.get("gluster_options", ""),
    }
    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)
    volinfo_file = "%s.info" % volname
    if not apply_pool_mount_metadata_for_write(
        core_v1_client,
        data,
        configmap_data.data.get(volinfo_file),
    ):
        return False
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=True,
    )
    configmap_data.data[volinfo_file] = json.dumps(data)

    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=False,
    )
    logging.info(logf("Updated configmap", name=KADALU_CONFIG_MAP,
                      volname=volname))
    filename = os.path.join(MANIFESTS_DIR, "external-storageclass.yaml")
    template(
        filename,
        **data,
        reclaim_policy=storage_class_reclaim_policy(pv_reclaim_policy),
    )
    if storage_api is None:
        storage_api = client.StorageV1Api()
    reconcile_storage_class_manifest(
        storage_api,
        filename,
        "kadalu." + volname,
        storage_class_reclaim_policy(pv_reclaim_policy),
    )
    # Close the provisioning race around immutable StorageClass replacement.
    # The directional pass above protects data; this pass catches a PV created
    # from the old class while reconciliation was in progress.
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=None,
    )
    logging.info(logf("Deployed External StorageClass", volname=volname, manifest=filename))
    add_tolerations("daemonset", NODE_PLUGIN, tolerations)
    return True


def handle_added(core_v1_client, obj):
    """
    New Volume is requested. Update the configMap and deploy
    """

    if not validate_volume_request(obj):
        # TODO: Delete Custom resource
        logging.debug(logf(
            "validation of volume request failed",
            yaml=obj
        ))
        return

    volname = obj["metadata"]["name"]
    pods = core_v1_client.list_namespaced_pod(NAMESPACE)
    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)
    volinfo_file = "%s.info" % volname
    existing_serialized = configmap_data.data.get(volinfo_file)
    existing = None

    if existing_serialized is not None:
        # Volume already exists
        logging.warning(logf(
            "Updating existing config map",
            storagename=volname
        ))
        existing = _decode_pool_record(existing_serialized)
        if existing is None:
            return
        requested_mode = get_single_pv_per_pool(obj["spec"])
        if requested_mode != get_single_pv_per_pool(existing):
            logging.error(logf(
                "Rejected immutable single_pv_per_pool change",
                volname=volname,
                existing=get_single_pv_per_pool(existing),
                requested=requested_mode,
            ))
            return

    existing_volume_id = existing.get("volume_id") if existing else None
    requested_volume_id = obj["spec"].get("volume_id")
    if existing is not None:
        if not existing_volume_id:
            logging.error(logf(
                "Existing storage pool has no volume identity",
                volname=volname,
            ))
            return
        if (
                requested_volume_id is not None
                and requested_volume_id != existing_volume_id):
            logging.error(logf(
                "Rejected immutable storage pool volume identity change",
                volname=volname,
                existing=existing_volume_id,
                requested=requested_volume_id,
            ))
            return
        obj["spec"]["volume_id"] = existing_volume_id
        logging.info(logf(
            "Reusing existing volume id",
            volume_id=existing_volume_id,
        ))
    elif requested_volume_id is None:
        try:
            recovered_volume_id = recover_server_volume_id(pods.items, volname)
        except ValueError as err:
            logging.error(logf(
                "Refusing to reconstruct storage pool identity",
                volname=volname,
                error=err,
            ))
            return
        if recovered_volume_id is not None:
            obj["spec"]["volume_id"] = recovered_volume_id
            logging.info(logf(
                "Recovered volume id from existing server pods",
                volname=volname,
                volume_id=recovered_volume_id,
            ))
        elif any(_pool_persistent_volumes(core_v1_client, volname)):
            logging.error(logf(
                "Refusing to reconstruct storage pool without its volume id",
                volname=volname,
            ))
            return
        else:
            obj["spec"]["volume_id"] = str(uuid.uuid4())
    else:
        logging.info(logf(
            "Applying existing volume id",
            volume_id=requested_volume_id,
        ))

    voltype = obj["spec"]["type"]
    if voltype == VOLUME_TYPE_EXTERNAL:
        handle_external_storage_addition(core_v1_client, obj)
        return

    # Generate Node ID for each storage device.
    for idx, _ in enumerate(obj["spec"]["storage"]):
        obj["spec"]["storage"][idx]["node_id"] = "node-%d" % idx

    if not update_config_map(core_v1_client, obj):
        return
    deploy_storage_class(obj, core_v1_client)

    # Applying an unchanged StatefulSet is non-disruptive, while always
    # reconciling it ensures storage placement, devices, tolerations, and other
    # CR changes made while the operator was down reach existing servers.
    deploy_server_pods(obj)

    filename = os.path.join(MANIFESTS_DIR, "services.yaml")
    template(filename, namespace=NAMESPACE, volname=volname)
    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
    logging.info(logf("Deployed Service", volname=volname, manifest=filename))


def handle_modified(core_v1_client, obj):
    """
    Handle when Volume option is updated or Volume
    state is changed to maintenance
    """
    # TODO: Handle Volume maintenance mode

    volname = obj["metadata"]["name"]

    voltype = obj["spec"]["type"]
    if voltype == VOLUME_TYPE_EXTERNAL:
        if not validate_volume_request(obj):
            logging.debug(logf(
                "validation of volume request failed",
                yaml=obj
            ))
            return

        configmap_data = core_v1_client.read_namespaced_config_map(
            KADALU_CONFIG_MAP, NAMESPACE)
        existing_serialized = configmap_data.data.get(f"{volname}.info")
        if existing_serialized is None:
            logging.warning(logf(
                "Volume config not found",
                storagename=volname
            ))
            handle_added(core_v1_client, obj)
            return

        existing = _decode_pool_record(existing_serialized)
        if existing is None:
            return
        existing_volume_id = existing.get("volume_id")
        if not existing_volume_id:
            logging.error(logf(
                "Existing storage pool has no volume identity",
                volname=volname,
            ))
            return
        requested_volume_id = obj["spec"].get("volume_id")
        if (
                requested_volume_id is not None
                and requested_volume_id != existing_volume_id):
            logging.error(logf(
                "Rejected immutable storage pool volume identity change",
                volname=volname,
                existing=existing_volume_id,
                requested=requested_volume_id,
            ))
            return

        obj["spec"]["volume_id"] = existing_volume_id
        handle_external_storage_addition(core_v1_client, obj)
        return

    if not validate_volume_request(obj):
        logging.debug(logf(
            "validation of volume request failed",
            yaml=obj
        ))
        return

    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)

    if not configmap_data.data.get("%s.info" % volname, None):
        logging.warning(logf(
            "Volume config not found",
            storagename=volname
        ))
        # Volume doesn't exist yet, so create it
        handle_added(core_v1_client, obj)
        return

    # Volume ID (uuid) is already generated, re-use
    cfgmap = _decode_pool_record(configmap_data.data[volname + ".info"])
    if cfgmap is None:
        return
    # Get volume-id from config map
    obj["spec"]["volume_id"] = cfgmap["volume_id"]

    # Set Node ID for each storage device from configmap
    for idx, _ in enumerate(obj["spec"]["storage"]):
        obj["spec"]["storage"][idx]["node_id"] = "node-%d" % idx

    # Add new entry in the existing config map
    if not update_config_map(core_v1_client, obj):
        return
    deploy_storage_class(obj, core_v1_client)
    deploy_server_pods(obj)

    filename = os.path.join(MANIFESTS_DIR, "services.yaml")
    template(filename, namespace=NAMESPACE, volname=volname)
    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
    logging.info(logf("Deployed Service", volname=volname, manifest=filename))


def handle_deleted(core_v1_client, obj):
    """
    If number of pvs provisioned from that volume
    is zero - Delete the respective server pods
    If number of pvs is not zero, wait or periodically
    check for num_pvs. Delete Server pods only when pvs becomes zero.
    """

    volname = obj["metadata"]["name"]

    storage_info_data = get_configmap_data(volname)

    logging.info(logf("Delete requested", volname=volname))

    if storage_info_data is None:
        logging.error(logf(
            "Storage delete failed. Storage metadata is unavailable",
            storage=volname,
        ))
        return

    pv_count = get_num_pvs(storage_info_data)

    if pv_count == -1:
        logging.error(
            logf("Storage delete failed. Failed to get PV count",
                 number_of_pvs=pv_count,
                 storage=volname))
        return

    if pv_count != 0:

        logging.warning(
            logf("Storage delete failed. Storage is not empty",
                 number_of_pvs=pv_count,
                 storage=volname))

    elif pv_count == 0:

        hostvol_type = storage_info_data.get("type")

        # We can't delete external volume but cleanup StorageClass and Configmap
        # Delete Configmap and Storage class for both Native & External
        delete_storage_class(volname, hostvol_type)
        delete_config_map(core_v1_client, obj)

        if hostvol_type != "External":

            delete_server_pods(storage_info_data, obj)
            filename = os.path.join(MANIFESTS_DIR, "services.yaml")
            template(filename, namespace=NAMESPACE, volname=volname)
            lib_execute(KUBECTL_CMD, DELETE_CMD, "-f", filename)
            logging.info(
                logf("Deleted Service", volname=volname, manifest=filename))

    return


def get_configmap_data(volname):
    """
    Get storage info data from kadalu configmap
    """

    cmd = [KUBECTL_CMD, "get", "configmap", "kadalu-info",
           "-n", NAMESPACE, "-ojson"]

    try:
        resp = utils_execute(cmd)
        config_data = json.loads(resp.stdout)

        data = config_data['data']
        storage_name = "%s.info" % volname
        storage_info_data = data[storage_name]

        # Return data in 'dict' format
        return json.loads(storage_info_data)

    except CommandError as err:
        logging.error(logf(
            "Failed to get details from configmap",
            error=err
        ))
        return None


def get_num_pvs(storage_info_data):
    """
    Get number of PVs provisioned from
    volume requested for deletion
    through configmap.
    """

    volname = storage_info_data['volname']
    volname = "kadalu." + volname
    jpath = ('jsonpath=\'{range .items[?(@.spec.storageClassName=="%s")]}'
                '{.spec.storageClassName}{"\\n"}{end}\'' % volname)
    cmd = ["kubectl", "get", "pv", "-o", jpath]

    try:
        resp = utils_execute(cmd)
        pvs = resp.stdout.strip("'").split()
        return len(pvs)

    except CommandError as msg:
        logging.error(
            logf("Failed to get size details of the "
                 "storage \"%s\"" % volname,
                 error=msg))
        # Return error as its -1
        return -1


def delete_server_pods(storage_info_data, obj):
    """
    Delete server pods depending on type of Hosting
    Volume and other options specified
    """

    volname = obj["metadata"]["name"]
    voltype = storage_info_data['type']
    volumeid = storage_info_data['volume_id']

    docker_user = os.environ.get("DOCKER_USER", "joejulian")

    shd_required = False
    if voltype in (VOLUME_TYPE_REPLICA_3, VOLUME_TYPE_REPLICA_2):
        shd_required = True

    template_args = {
        "namespace": NAMESPACE,
        "kadalu_version": VERSION,
        "docker_user": docker_user,
        "images_hub": IMAGES_HUB,
        "volname": volname,
        "voltype": voltype,
        "volume_id": volumeid,
        "shd_required": shd_required
    }

    bricks = storage_info_data['bricks']

    # Traverse all bricks from configmap
    for brick in bricks:

        idx = brick['brick_index']
        template_args["host_brick_path"] = brick['host_brick_path']
        template_args["kube_hostname"] = brick['kube_hostname']
        template_args["serverpod_name"] = get_brick_hostname(
            volname,
            idx,
            suffix=False
        )
        template_args["brick_path"] = "/bricks/%s/data/brick" % volname
        template_args["brick_index"] = idx
        template_args["brick_device"] = brick['brick_device']
        template_args["pvc_name"] = brick['pvc_name']
        template_args["brick_device_dir"] = brick['brick_device_dir']
        template_args["brick_node_id"] = brick['node_id']
        template_args["k8s_dist"] = K8S_DIST

        filename = os.path.join(MANIFESTS_DIR, "server.yaml")
        template(filename, **template_args)
        lib_execute(KUBECTL_CMD, DELETE_CMD, "-f", filename)
        logging.info(logf(
            "Deleted Server pod",
            volname=volname,
            manifest=filename,
            node=brick['node']
        ))


def delete_config_map(core_v1_client, obj):
    """
    Volinfo of existing Volume is generated and ConfigMap is deleted
    """

    volname = obj["metadata"]["name"]

    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)

    volinfo_file = "%s.info" % volname
    configmap_data.data[volinfo_file] = None

    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
    logging.info(logf(
        "Deleted configmap",
        name=KADALU_CONFIG_MAP,
        volname=volname
    ))


def delete_storage_class(hostvol_name, _):
    """
    Deletes deployed External and Custom StorageClass
    """

    sc_name = "kadalu." + hostvol_name
    lib_execute(KUBECTL_CMD, DELETE_CMD, "sc", sc_name)
    logging.info(logf(
        "Deleted Storage class",
        volname=hostvol_name
    ))


def watch_stream(core_v1_client, k8s_client):
    """
    Watches kubernetes event stream for kadalustorages in Kadalu namespace
    """
    crds = client.CustomObjectsApi(k8s_client)
    k8s_watch = watch.Watch()
    initial_list = crds.list_namespaced_custom_object(
        "kadalu-operator.storage",
        "v1alpha1",
        NAMESPACE,
        "kadalustorages",
    )

    for item in initial_list.get("items"):
        handle_added(core_v1_client, item)

    metadata = initial_list.get("metadata")
    resource_version = metadata['resourceVersion']

    for event in k8s_watch.stream(crds.list_namespaced_custom_object,
                                  "kadalu-operator.storage",
                                  "v1alpha1",
                                  NAMESPACE,
                                  "kadalustorages",
                                  resource_version=resource_version):
        obj = event["object"]
        operation = event['type']
        spec = obj.get("spec")
        if not spec:
            continue
        metadata = obj.get("metadata")
        resource_version = metadata['resourceVersion']
        logging.debug(logf("Event", operation=operation, object=repr(obj)))
        if operation == "ADDED":
            handle_added(core_v1_client, obj)
        elif operation == "MODIFIED":
            handle_modified(core_v1_client, obj)
        elif operation == "DELETED":
            handle_deleted(core_v1_client, obj)


def crd_watch(core_v1_client, k8s_client):
    """
    Watches the CRD to provision new PV Hosting Volumes
    """
    while True:
        try:
            watch_stream(core_v1_client, k8s_client)
        except (ProtocolError, NewConnectionError):
            # It might so happen that this'll be logged for every hit in k8s
            # event stream in kadalu namespace and better to log at debug level
            logging.debug(
                logf(
                    "Watch connection broken and restarting watch on the stream"
                ))
            time.sleep(30)


def quiesce_csi_provisioner(
        core_v1_client,
        apps_v1_client,
        timeout_seconds=CSI_QUIESCE_TIMEOUT_SECONDS,
        poll_interval=1):
    """Stop the old controller before ownership metadata is migrated."""
    try:
        apps_v1_client.patch_namespaced_stateful_set(
            CSI_PROVISIONER,
            NAMESPACE,
            {"spec": {"replicas": 0}},
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        if getattr(err, "status", None) not in (404, "404"):
            raise
        logging.info("No existing CSI provisioner StatefulSet to quiesce")

    deadline = time.monotonic() + timeout_seconds
    while True:
        pods = core_v1_client.list_namespaced_pod(NAMESPACE)
        active = [
            pod.metadata.name
            for pod in pods.items
            if pod.metadata.name.startswith(CSI_PROVISIONER + "-")
        ]
        if not active:
            logging.info("CSI provisioner is quiesced")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Timed out waiting for the old CSI provisioner to stop: "
                + ", ".join(active)
            )
        time.sleep(poll_interval)


def csi_nodeplugin_rollout_complete(daemon_set):
    """Return whether every desired node runs the observed DaemonSet revision."""
    if daemon_set is None:
        return False
    metadata = getattr(daemon_set, "metadata", None)
    status = getattr(daemon_set, "status", None)
    if status is None:
        return False

    desired = getattr(status, "desired_number_scheduled", None)
    updated = getattr(status, "updated_number_scheduled", None)
    ready = getattr(status, "number_ready", None)
    if desired is None or updated is None or ready is None:
        return False

    generation = getattr(metadata, "generation", None)
    observed = getattr(status, "observed_generation", None)
    if generation is not None and (
            observed is None or observed < generation):
        return False
    return updated == desired and ready == desired


def read_csi_nodeplugin(apps_v1_client):
    """Read the nodeplugin DaemonSet, treating absence as not rolled out."""
    try:
        return apps_v1_client.read_namespaced_daemon_set(
            NODE_PLUGIN,
            NAMESPACE,
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        if getattr(err, "status", None) not in (404, "404"):
            raise
        return None


def wait_for_csi_nodeplugin_rollout(
        apps_v1_client,
        timeout_seconds=CSI_QUIESCE_TIMEOUT_SECONDS,
        poll_interval=1):
    """Wait for an explicitly advanced OnDelete nodeplugin rollout."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        daemon_set = read_csi_nodeplugin(apps_v1_client)
        if csi_nodeplugin_rollout_complete(daemon_set):
            logging.info("CSI nodeplugin rollout is complete")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Timed out waiting for the CSI nodeplugin OnDelete rollout. "
                "The CSI provisioner remains at zero replicas. For each node: "
                "drain or unpublish its Kadalu volumes, delete that node's old "
                "kadalu-csi-nodeplugin pod, and wait for its replacement to "
                "become Ready before continuing with the next node. Restart "
                "the operator after every desired nodeplugin pod is updated."
            )
        time.sleep(poll_interval)


def deploy_csi_pods(core_v1_client, provisioner_replicas=1):
    """
    Look for CSI pods, if any one CSI pod found then
    that means it is deployed
    """
    if provisioner_replicas not in (0, 1):
        raise ValueError(
            "CSI provisioner replicas must be zero (upgrade fence) or one"
        )

    pods = core_v1_client.list_namespaced_pod(
        NAMESPACE)
    for pod in pods.items:
        if pod.metadata.name.startswith(CSI_POD_PREFIX):
            logging.info("Updating already deployed CSI pods")

    # storage.k8s.io/v1 has been served since Kubernetes 1.18 and is the only
    # CSIDriver API available on the supported Kubernetes 1.36 baseline.
    filename = os.path.join(MANIFESTS_DIR, "csi-driver-object-v1.yaml")
    template(filename, namespace=NAMESPACE, kadalu_version=VERSION)
    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)

    filename = os.path.join(MANIFESTS_DIR, "csi.yaml")
    docker_user = os.environ.get("DOCKER_USER", "joejulian")
    template(filename, namespace=NAMESPACE, kadalu_version=VERSION,
             docker_user=docker_user, k8s_dist=K8S_DIST,
             images_hub=IMAGES_HUB,
             csi_sidecar_registry=CSI_SIDECAR_REGISTRY,
             busybox_image=BUSYBOX_IMAGE,
             kubelet_dir=KUBELET_DIR, verbose=VERBOSE,
             provisioner_replicas=provisioner_replicas)

    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
    logging.info(logf("Deployed CSI Pods", manifest=filename))


def deploy_csi_upgrade(core_v1_client, apps_v1_client):
    """Keep provisioning fenced until metadata and every node are current."""
    quiesce_csi_provisioner(core_v1_client, apps_v1_client)
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    migration_required = legacy_mount_migration_required(configmap_data.data)

    # Publish the desired OnDelete template without restarting live clients or
    # allowing the controller to create block volumes during the transition.
    deploy_csi_pods(core_v1_client, provisioner_replicas=0)
    if migration_required:
        ensure_kadalu_block_volumes_absent(
            core_v1_client,
            "migrate legacy mount identities",
        )

    migrate_legacy_single_pv_claims(core_v1_client)
    wait_for_csi_nodeplugin_rollout(apps_v1_client)
    deploy_csi_pods(core_v1_client, provisioner_replicas=1)


def deploy_config_map(core_v1_client):
    """Deploys the template configmap if not exists"""

    configmaps = core_v1_client.list_namespaced_config_map(
        NAMESPACE)
    uid = uuid.uuid4()
    upgrade = False
    for item in configmaps.items:
        if item.metadata.name == KADALU_CONFIG_MAP:
            logging.info(logf(
                "Found existing configmap. Updating",
                name=item.metadata.name
            ))

            # Don't overwrite UID info.
            configmap_data = core_v1_client.read_namespaced_config_map(
                KADALU_CONFIG_MAP, NAMESPACE)
            if configmap_data.data.get("uid", None):
                uid = configmap_data.data["uid"]
                upgrade = True
            # Keep the config details required to be preserved.

    # Deploy Config map
    filename = os.path.join(MANIFESTS_DIR, "configmap.yaml")
    template(filename,
             namespace=NAMESPACE,
             kadalu_version=VERSION,
             uid=uid)

    if not upgrade:
        lib_execute(KUBECTL_CMD, CREATE_CMD, "-f", filename)
    logging.info(logf("ConfigMap Deployed", manifest=filename, uid=uid, upgrade=upgrade))
    return uid, upgrade


def storage_class_reclaim_policy(pv_reclaim_policy):
    """Map Kadalu data handling to Kubernetes PV lifecycle semantics."""
    if pv_reclaim_policy == "retain":
        return "Retain"
    return "Delete"


def reconcile_storage_class_manifest(
        storage_api, filename, name, reclaim_policy):
    """Apply a StorageClass, replacing it when immutable policy changed."""
    installed = {
        item.metadata.name: item
        for item in storage_api.list_storage_class().items
    }
    existing = installed.get(name)
    existing_policy = (
        getattr(existing, "reclaim_policy", None)
        if existing is not None
        else None
    )
    # Kubernetes defaults an omitted StorageClass policy to Delete, so legacy
    # classes which did not render this field do not need replacement.
    existing_policy = existing_policy or "Delete"
    if existing is not None and existing_policy != reclaim_policy:
        logging.info(logf(
            "Replacing StorageClass to change immutable reclaim policy",
            name=name,
            existing=existing_policy,
            requested=reclaim_policy,
        ))
        # Removing a StorageClass does not remove its PVs or PVCs. Waiting for
        # the exact class to disappear makes the following create deterministic;
        # if apply fails, a later reconciliation can recreate it from this file.
        lib_execute(
            KUBECTL_CMD,
            DELETE_CMD,
            "storageclass",
            name,
            "--ignore-not-found=true",
            "--wait=true",
        )
    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)


def deploy_storage_class(obj, core_v1_client=None, storage_api=None):
    """Deploys the default and custom storage class for KaDalu if not exists"""

    # Deploy defalut Storage Class
    if storage_api is None:
        storage_api = client.StorageV1Api()
    sc_names = []
    for tmpl in os.listdir(TEMPLATES_DIR):
        if tmpl.startswith("storageclass-") and tmpl.endswith(".j2"):
            sc_names.append(
                tmpl.replace("storageclass-", "").replace(".yaml.j2", "")
            )

    for sc_name in sc_names:
        filename = os.path.join(MANIFESTS_DIR, "storageclass-%s.yaml" % sc_name)
        storage_class_name = "kadalu." + obj["metadata"]["name"]
        reclaim_policy = storage_class_reclaim_policy(
            obj["spec"].get("pvReclaimPolicy", "delete")
        )

        template(filename, namespace=NAMESPACE, kadalu_version=VERSION,
                 hostvol_name=obj["metadata"]["name"],
                 single_pv_per_pool=get_single_pv_per_pool(obj["spec"]),
                 reclaim_policy=reclaim_policy)
        reconcile_storage_class_manifest(
            storage_api,
            filename,
            storage_class_name,
            reclaim_policy,
        )
        if core_v1_client is not None:
            reconcile_pool_pv_reclaim_policy(
                core_v1_client,
                obj["metadata"]["name"],
                obj["spec"].get("pvReclaimPolicy", "delete"),
                before_config_write=None,
            )
        logging.info(logf("Deployed StorageClass", manifest=filename))

def add_tolerations(resource, name, tolerations):
    """Adds tolerations to kubernetes resource/name object"""
    if tolerations is None:
        return
    patch = {"spec": {"template": {"spec": {"tolerations": tolerations}}}}
    try:
        lib_execute(KUBECTL_CMD, PATCH_CMD, resource, name, "-n", NAMESPACE,
                    "-p", json.dumps(patch), "--type=merge")
    except CommandException as err:
        errmsg = f"Unable to patch {resource}/{name} with tolerations \
        {str(tolerations)}"
        logging.error(logf(errmsg, error=err))
    logging.info(logf("Added tolerations", resource=resource, name=name,
        tolerations=str(tolerations)))
    return

def main():
    """Main"""
    config.load_incluster_config()

    core_v1_client = client.CoreV1Api()
    k8s_client = client.ApiClient()
    apps_v1_client = client.AppsV1Api(k8s_client)

    # ConfigMap
    uid, upgrade = deploy_config_map(core_v1_client)

    if upgrade:
        # Keep provisioning disabled until legacy metadata is safe and every
        # explicitly advanced OnDelete nodeplugin is running this release.
        deploy_csi_upgrade(core_v1_client, apps_v1_client)
    else:
        migrate_legacy_single_pv_claims(core_v1_client)
        deploy_csi_pods(core_v1_client)

    if upgrade:
        logging.info(logf("Upgrading to ", version=VERSION))
        upgrade_storage_pods(core_v1_client)

    # Send Analytics Tracker
    # The information from this analytics is available for
    # developers to understand and build project in a better
    # way
    send_analytics_tracker("operator", uid)

    # Watch CRD
    crd_watch(core_v1_client, k8s_client)


if __name__ == "__main__":
    logging_setup()

    # This not advised in general, but in kadalu's operator, it is OK to
    # ignore these warnings as we know to make calls only inside of
    # kubernetes cluster
    urllib3.disable_warnings()

    main()
