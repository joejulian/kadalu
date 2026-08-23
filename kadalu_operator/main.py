"""
KaDalu Operator: Once started, deploys required CSI drivers,
bootstraps the ConfigMap and waits for the CRD update to create
Server pods
"""
import hashlib
import ipaddress
import json
import logging
import os
import re
import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone

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
OPERATOR_READY_FILE = "/tmp/operator-ready"
KUBECTL_CMD = "/usr/bin/kubectl"
KADALU_CONFIG_MAP = "kadalu-info"
CSI_POD_PREFIX = "csi-"
STORAGE_CLASS_NAME_PREFIX = "kadalu."
STORAGE_CLASS_NAMESPACE_ANNOTATION = "kadalu.io/storage-namespace"
STORAGE_CLASS_NAME_ANNOTATION = "kadalu.io/storage-name"
STORAGE_CLASS_UID_ANNOTATION = "kadalu.io/storage-uid"
STORAGE_CLASS_VOLUME_ID_ANNOTATION = "kadalu.io/volume-id"
STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION = "kadalu.io/mount-identity"
STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION = (
    "kadalu.io/backend-fingerprint"
)
CLEANUP_ONLY_STORAGE_UID_PREFIX = "orphan:"
CLEANUP_ONLY_RECORD_FIELD = "deletion_cleanup_only"
DELETION_QUARANTINE_RECORD_FIELD = "deletion_quarantined"
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
CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS = int(os.environ.get(
    "KADALU_CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS",
    "3600",
))
CSI_PROVISIONER_ROLLOUT_TIMEOUT_SECONDS = int(os.environ.get(
    "KADALU_CSI_PROVISIONER_ROLLOUT_TIMEOUT_SECONDS",
    "300",
))
SERVER_ROLLOUT_TIMEOUT_SECONDS = int(os.environ.get(
    "KADALU_SERVER_ROLLOUT_TIMEOUT_SECONDS",
    "900",
))
STORAGE_CLASS_DELETE_TIMEOUT_SECONDS = int(os.environ.get(
    "KADALU_STORAGE_CLASS_DELETE_TIMEOUT_SECONDS",
    "60",
))
WATCH_TIMEOUT_SECONDS = int(os.environ.get(
    "KADALU_WATCH_TIMEOUT_SECONDS",
    "30",
))
HEAL_WAIT_TIMEOUT_SECONDS = int(os.environ.get(
    "KADALU_HEAL_WAIT_TIMEOUT_SECONDS",
    "900",
))
HEAL_WAIT_INTERVAL_SECONDS = 5
HEAL_GATED_VOLUME_TYPES = {
    VOLUME_TYPE_REPLICA_2,
    VOLUME_TYPE_REPLICA_3,
    VOLUME_TYPE_DISPERSE,
    VOLUME_TYPE_ARBITER,
}
BLOCK_PV_TYPES = {"virtblock", "rawblock"}
TOLERATION_OPERATORS = {"Equal", "Exists"}
TOLERATION_EFFECTS = {"", "NoSchedule", "PreferNoSchedule", "NoExecute"}
DNS_SUBDOMAIN_RE = re.compile(
    r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*"
)
DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")
LABEL_NAME_RE = re.compile(
    r"[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?"
)


class WatchStatusError(RuntimeError):
    """Represent a Kubernetes watch Status object as a raised exception."""

    def __init__(self, message, status):
        super().__init__(message)
        self.status = status


class MissingTolerationSourceError(RuntimeError):
    """A deletion-orphan has no persisted or live scheduling policy."""


def mark_operator_ready():
    """Publish readiness after fenced startup reconciliation succeeds."""
    ready_fd = os.open(
        OPERATOR_READY_FILE,
        os.O_CREAT | os.O_TRUNC | os.O_WRONLY,
        0o600,
    )
    os.close(ready_fd)


def clear_operator_ready():
    """Remove readiness left by an earlier reconciler process."""
    try:
        os.unlink(OPERATOR_READY_FILE)
    except FileNotFoundError:
        pass


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
# pylint: disable=too-many-boolean-expressions
def options_validation(options):
    """ Validate Pool Options """

    if not isinstance(options, list):
        logging.error("Storage Pool Options must be a list")
        return False
    for option in options:
        if not isinstance(option, dict):
            logging.error("Storage Pool Option must be an object")
            return False
        key = option.get("key")
        value = option.get("value")
        if (
                not isinstance(key, str)
                or not key
                or key != key.strip()
                or any(ord(char) < 32 or ord(char) == 127 for char in key)
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", key) is None
                or not isinstance(value, str)
                or any(ord(char) < 32 or ord(char) == 127 for char in value)):
            logging.error(logf("Key/Value not specified for Storage Pool Options"))
            return False

    return True


# pylint: disable=too-many-return-statements,too-many-branches
def bricks_validation(bricks):
    """Validate Brick path and node options"""
    if not isinstance(bricks, list) or not bricks:
        logging.error("At least one storage brick must be specified")
        return False
    identities = set()
    for idx, brick in enumerate(bricks):
        if not isinstance(brick, dict):
            logging.error(logf("Storage brick is invalid", number=idx + 1))
            return False

        backings = []
        for field in ("pvc", "path", "device"):
            value = brick.get(field)
            if value is None or value == "":
                continue
            if (
                    not isinstance(value, str)
                    or not value.strip()
                    or value != value.strip()
                    or any(ord(char) < 32 or ord(char) == 127
                           for char in value)):
                logging.error(logf(
                    "Storage brick backing is invalid",
                    number=idx + 1,
                    field=field,
                ))
                return False
            backings.append(field)

        if len(backings) != 1:
            logging.error(logf(
                "Exactly one non-empty pvc, path, or device must be "
                "specified for each storage brick",
                number=idx + 1,
            ))
            return False

        backing = backings[0]
        value = brick[backing]
        if backing == "pvc":
            if (
                    len(value) > 253
                    or DNS_SUBDOMAIN_RE.fullmatch(value) is None):
                logging.error(logf(
                    "Storage PVC name is invalid",
                    number=idx + 1,
                ))
                return False
        elif not os.path.isabs(value):
            logging.error(logf(
                "Storage path/device must be absolute",
                number=idx + 1,
                field=backing,
            ))
            return False
        elif (
                os.path.normpath(value) != value
                or (backing == "device" and not os.path.basename(value))):
            logging.error(logf(
                "Storage path/device is not canonical",
                number=idx + 1,
            ))
            return False

        node = brick.get("node")
        node_supplied = node is not None and node != ""
        if node_supplied and (
                not isinstance(node, str)
                or node != node.strip()
                or not _valid_label_value(node)):
            logging.error(logf(
                "Storage node is invalid",
                number=idx + 1,
            ))
            return False
        if backing != "pvc" and not node_supplied:
            logging.error(logf(
                "Storage node not specified",
                number=idx + 1,
            ))
            return False

        identity = (
            (backing, value)
            if backing == "pvc"
            else (backing, node, value)
        )
        if identity in identities:
            logging.error(logf(
                "Duplicate storage brick backing",
                number=idx + 1,
            ))
            return False
        identities.add(identity)

    return True


# pylint: disable=too-many-return-statements
def validate_ext_details(obj):
    """Validate external Volume details"""
    cluster = obj["spec"].get("details", None)
    if not isinstance(cluster, dict) or not cluster:
        logging.error(logf("External Cluster details not given."))
        return False

    ghosts = []
    hosts = cluster.get("gluster_hosts")
    if hosts is not None:
        if not isinstance(hosts, list):
            logging.error("External gluster_hosts must be a list")
            return False
        ghosts.extend(hosts)
    host = cluster.get("gluster_host")
    if host not in (None, ""):
        ghosts.append(host)
    if (
            not ghosts
            or any(
                not isinstance(item, str)
                or not item
                or item != item.strip()
                or not _valid_gluster_host(item)
                for item in ghosts
            )):
        logging.error("No valid external Gluster host was provided")
        return False

    gluster_volname = cluster.get("gluster_volname")
    if (
            not isinstance(gluster_volname, str)
            or not gluster_volname
            or gluster_volname != gluster_volname.strip()
            or not _valid_gluster_volume_path(gluster_volname)):
        logging.error(logf("No 'host' and 'volname' details provided."))
        return False

    gluster_options = cluster.get("gluster_options", "")
    if (
            not isinstance(gluster_options, str)
            or any(ord(char) < 32 or ord(char) == 127
                   for char in gluster_options)):
        logging.error("External Gluster options are invalid")
        return False

    gport = cluster.get("gluster_port", 24007)
    if (
            isinstance(gport, bool)
            or not isinstance(gport, int)
            or not 1 <= gport <= 65535):
        logging.error("External Gluster port is invalid")
        return False

    try:
        reachable = is_host_reachable(ghosts, gport)
    except (OSError, TypeError, UnicodeError, ValueError) as err:
        logging.error(logf(
            "Unable to check external Gluster hosts",
            error=err,
        ))
        reachable = False
    if not reachable:
        logging.error(logf("gluster server not reachable: on %s:%d" %
                           (ghosts, gport)))
        #  Noticed that there may be glitches in n/w during this time.
        #  Not good to fail the validation, instead, just log here, so
        #  we are aware this is a possible reason.
        #return False

    logging.debug(logf("External Storage %s successfully validated" % \
                       obj["metadata"].get("name", "<unknown>")))
    return True


def _valid_gluster_host(host):
    """Return whether a Gluster endpoint is an IP address or DNS name."""
    if "," in host or not host.isascii():
        return False
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    if len(host) > 253:
        return False
    labels = host.lower().rstrip(".").split(".")
    return bool(labels) and all(
        label
        and len(label) <= 63
        and DNS_LABEL_RE.fullmatch(label) is not None
        for label in labels
    )


def _valid_gluster_volume_path(value):
    """Validate a Gluster volume name with an optional subdirectory."""
    if len(value.encode("utf-8")) > 4096:
        return False
    components = value.split("/")
    if (
            not components
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,255}",
                                components[0])):
        return False
    return all(
        component not in ("", ".", "..")
        and len(component.encode("utf-8")) <= 255
        and not any(ord(char) < 32 or ord(char) == 127
                    for char in component)
        for component in components[1:]
    )


def storage_class_name(source):
    """Return the configured StorageClass name, preserving legacy naming."""
    if "metadata" in source:
        spec = source.get("spec") or {}
        metadata = source.get("metadata") or {}
        volname = metadata.get("name")
        configured = (
            spec.get("storageClassName")
            if isinstance(spec, dict)
            else None
        )
    else:
        volname = source.get("volname")
        configured = source.get("storageClassName")
    if configured is None:
        return f"{STORAGE_CLASS_NAME_PREFIX}{volname}"
    return configured


# pylint: disable=too-many-return-statements
def generated_resource_names_validation(obj):
    """Validate every Kubernetes name derived from a storage resource."""
    metadata = obj.get("metadata")
    if not isinstance(metadata, dict):
        logging.error("Storage metadata is invalid")
        return False
    volname = metadata.get("name")
    if (
            not isinstance(volname, str)
            or not volname
            or len(volname) > 63
            or DNS_LABEL_RE.fullmatch(volname) is None):
        logging.error("Storage name is invalid")
        return False

    requested_storage_class_name = storage_class_name(obj)
    if (
            not isinstance(requested_storage_class_name, str)
            or not requested_storage_class_name
            or len(requested_storage_class_name) > 253
            or DNS_SUBDOMAIN_RE.fullmatch(
                requested_storage_class_name
            ) is None):
        logging.error("StorageClass name is invalid")
        return False

    spec = obj["spec"]
    if spec.get("type") == VOLUME_TYPE_EXTERNAL:
        return True
    if len(volname) > 63:
        logging.error("Storage name cannot be used as a headless Service name")
        return False
    for index, _brick in enumerate(spec.get("storage") or []):
        pod_hostname = f"{get_brick_hostname(volname, index, suffix=False)}-0"
        if (
                len(pod_hostname) > 63
                or DNS_LABEL_RE.fullmatch(pod_hostname) is None):
            logging.error(logf(
                "Generated server Pod hostname is invalid",
                storage=volname,
                brick=index,
            ))
            return False
    return True


# pylint: disable=too-many-return-statements
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=too-many-locals
# pylint: disable=too-many-boolean-expressions
def validate_volume_request(obj):
    """Validate the Volume request for Replica options, number of bricks etc"""
    if not isinstance(obj, dict):
        logging.error("Storage resource is invalid")
        return False

    spec = obj.get("spec")
    if not isinstance(spec, dict) or not spec:
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

    if not generated_resource_names_validation(obj):
        return False

    volume_id = spec.get("volume_id")
    if volume_id is not None:
        try:
            valid_volume_id = str(uuid.UUID(volume_id)) == volume_id
        except (AttributeError, TypeError, ValueError):
            valid_volume_id = False
        if not valid_volume_id:
            logging.error("Storage volume_id must be a canonical UUID")
            return False

    try:
        normalized_tolerations = normalize_tolerations(
            spec.get("tolerations"),
            f"Kadalustorage {(obj.get('metadata') or {}).get('name', '<unknown>')}",
        )
    except RuntimeError as err:
        logging.error(logf(
            "Storage tolerations are invalid",
            error=err,
        ))
        return False
    if "tolerations" in spec:
        spec["tolerations"] = normalized_tolerations

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
        if not isinstance(disperse_config, dict):
            logging.error("Disperse Volume data and redundancy "
                          "count is not specified")
            return False

        data_bricks = disperse_config.get("data", 0)
        redundancy_bricks = disperse_config.get("redundancy", 0)
        if (
                isinstance(data_bricks, bool)
                or not isinstance(data_bricks, int)
                or data_bricks <= 0
                or isinstance(redundancy_bricks, bool)
                or not isinstance(redundancy_bricks, int)
                or redundancy_bricks <= 0):
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
        if tiebreaker:
            if not isinstance(tiebreaker, dict):
                logging.error("Replica2 tiebreaker must be an object")
                return False
            node = tiebreaker.get("node")
            path = tiebreaker.get("path")
            port = tiebreaker.get("port", 24007)
            if (
                    not isinstance(node, str)
                    or not node
                    or node != node.strip()
                    or not _valid_label_value(node)
                    or not isinstance(path, str)
                    or not path
                    or path != path.strip()
                    or any(ord(char) < 32 or ord(char) == 127
                           for char in path)
                    or not os.path.isabs(path)
                    or isinstance(port, bool)
                    or not isinstance(port, int)
                    or not 1 <= port <= 65535):
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


_K8S_FIELD_MISSING = object()


def _k8s_value(value, api_name, model_name=None, default=None):
    """Read one Kubernetes API field from a model or JSON mapping."""
    if value is None:
        return default
    if isinstance(value, dict):
        if api_name in value:
            return value[api_name]
        if model_name is not None and model_name in value:
            return value[model_name]
        return default
    return getattr(value, model_name or api_name, default)


def _k8s_named_items(items, name):
    """Return every item with an exact Kubernetes name field."""
    return [
        item for item in items or []
        if _k8s_value(item, "name") == name
    ]


def _server_container_and_environment(pod_spec, context):
    """Return the unique server container and its direct-value env map."""
    containers = _k8s_value(pod_spec, "containers", default=[]) or []
    server_containers = _k8s_named_items(containers, "server")
    if len(server_containers) != 1:
        raise ValueError(f"{context} has no unique server container")

    server = server_containers[0]
    environment = {}
    for variable in _k8s_value(server, "env", default=[]) or []:
        name = _k8s_value(variable, "name")
        if name in environment:
            raise ValueError(f"{context} has duplicate {name} environment")
        if _k8s_value(
                variable,
                "valueFrom",
                "value_from",
                _K8S_FIELD_MISSING,
        ) not in (_K8S_FIELD_MISSING, None):
            raise ValueError(f"{context} has indirect {name} environment")
        environment[name] = _k8s_value(
            variable,
            "value",
            default=_K8S_FIELD_MISSING,
        )
    return server, environment


def _required_hostname(pod_spec, context):
    """Return the one exact required hostname from a server Pod template."""
    affinity = _k8s_value(pod_spec, "affinity")
    if affinity is None:
        return None
    node_affinity = _k8s_value(
        affinity,
        "nodeAffinity",
        "node_affinity",
    )
    if node_affinity is None:
        return None
    required = _k8s_value(
        node_affinity,
        "requiredDuringSchedulingIgnoredDuringExecution",
        "required_during_scheduling_ignored_during_execution",
    )
    if required is None:
        return None
    terms = _k8s_value(
        required,
        "nodeSelectorTerms",
        "node_selector_terms",
        [],
    ) or []
    if len(terms) != 1:
        raise ValueError(f"{context} has ambiguous hostname affinity")
    expressions = _k8s_value(
        terms[0],
        "matchExpressions",
        "match_expressions",
        [],
    ) or []
    if len(expressions) != 1:
        raise ValueError(f"{context} has ambiguous hostname affinity")
    expression = expressions[0]
    values = _k8s_value(expression, "values", default=[]) or []
    if (
            _k8s_value(expression, "key") != "kubernetes.io/hostname"
            or _k8s_value(expression, "operator") != "In"
            or len(values) != 1
            or not values[0]):
        raise ValueError(f"{context} has invalid hostname affinity")
    return values[0]


def _validate_server_volume_mounts(
        pod_spec, server, storage, volname, context):
    """Prove the live server uses the exact requested brick backing."""
    volumes = _k8s_value(pod_spec, "volumes", default=[]) or []
    mountdir_volumes = _k8s_named_items(volumes, "glusterfsd-mountdir")
    if len(mountdir_volumes) != 1:
        raise ValueError(f"{context} has no unique brick volume")
    mountdir = mountdir_volumes[0]

    host_path = _k8s_value(mountdir, "hostPath", "host_path")
    pvc = _k8s_value(
        mountdir,
        "persistentVolumeClaim",
        "persistent_volume_claim",
    )
    empty_dir = _k8s_value(
        mountdir,
        "emptyDir",
        "empty_dir",
        _K8S_FIELD_MISSING,
    )
    source_count = sum((
        host_path is not None,
        pvc is not None,
        empty_dir not in (_K8S_FIELD_MISSING, None),
    ))
    if source_count != 1:
        raise ValueError(f"{context} has ambiguous brick backing")

    if storage.get("pvc", ""):
        if (
                pvc is None
                or _k8s_value(
                    pvc,
                    "claimName",
                    "claim_name",
                ) != storage["pvc"]):
            raise ValueError(f"{context} has a different brick PVC")
    elif storage.get("path", ""):
        if (
                host_path is None
                or _k8s_value(host_path, "path") != storage["path"]
                or _k8s_value(host_path, "type") != "Directory"):
            raise ValueError(f"{context} has a different brick path")
    elif empty_dir in (_K8S_FIELD_MISSING, None):
        raise ValueError(f"{context} has a different brick device backing")

    mounts = _k8s_value(
        server,
        "volumeMounts",
        "volume_mounts",
        [],
    ) or []
    mountdir_mounts = _k8s_named_items(mounts, "glusterfsd-mountdir")
    if (
            len(mountdir_mounts) != 1
            or _k8s_value(
                mountdir_mounts[0],
                "mountPath",
                "mount_path",
            ) != f"/bricks/{volname}"):
        raise ValueError(f"{context} has an invalid brick mount")

    device = storage.get("device", "")
    device_dir_volumes = _k8s_named_items(volumes, "brick-device-dir")
    device_dir_mounts = _k8s_named_items(mounts, "brick-device-dir")
    expected_device_dir = (
        os.path.dirname(device)
        if device and not device.startswith("/dev/")
        else ""
    )
    if expected_device_dir:
        if len(device_dir_volumes) != 1 or len(device_dir_mounts) != 1:
            raise ValueError(f"{context} has no unique device directory")
        device_host_path = _k8s_value(
            device_dir_volumes[0],
            "hostPath",
            "host_path",
        )
        if (
                device_host_path is None
                or _k8s_value(device_host_path, "path")
                != expected_device_dir
                or _k8s_value(device_host_path, "type") != "Directory"
                or _k8s_value(
                    device_dir_mounts[0],
                    "mountPath",
                    "mount_path",
                ) != "/brickdev"):
            raise ValueError(f"{context} has a different device directory")
    elif device_dir_volumes or device_dir_mounts:
        raise ValueError(f"{context} has an unexpected device directory")


def _validate_live_server(resource, obj, index, stateful_set):
    """Validate one live server Pod or StatefulSet template exactly."""
    volname = obj["metadata"]["name"]
    kind = "StatefulSet" if stateful_set else "Pod"
    expected_name = get_brick_hostname(volname, index, suffix=False)
    if not stateful_set:
        expected_name = f"{expected_name}-0"
    context = f"Server {kind} {expected_name}"

    metadata = _k8s_value(resource, "metadata")
    if _k8s_value(metadata, "name") != expected_name:
        raise ValueError(f"{context} has an inconsistent name")
    resource_spec = _k8s_value(resource, "spec")
    if resource_spec is None:
        raise ValueError(f"{context} has no spec")
    if stateful_set:
        if _k8s_value(
                resource_spec,
                "serviceName",
                "service_name",
        ) != volname:
            raise ValueError(f"{context} has a different service")
        replicas = _k8s_value(resource_spec, "replicas", default=1)
        if replicas != 1:
            raise ValueError(f"{context} has an unsafe replica count")
        pod_template = _k8s_value(resource_spec, "template")
        pod_spec = _k8s_value(pod_template, "spec")
    else:
        pod_spec = resource_spec
    if pod_spec is None:
        raise ValueError(f"{context} has no Pod template")

    server, environment = _server_container_and_environment(
        pod_spec,
        context,
    )
    storage = obj["spec"]["storage"][index]
    expected_environment = {
        "VOLUME": volname,
        "VOLUME_TYPE": obj["spec"]["type"],
        "BRICK_PATH": f"/bricks/{volname}/data/brick",
        "NODEID": f"node-{index}",
        "BRICK_INDEX": str(index),
        "BRICK_DEVICE": storage.get("device", ""),
    }
    for name, expected in expected_environment.items():
        if environment.get(name, _K8S_FIELD_MISSING) != expected:
            raise ValueError(f"{context} has a different {name}")

    volume_id = environment.get("VOLUME_ID", _K8S_FIELD_MISSING)
    try:
        canonical_volume_id = str(uuid.UUID(volume_id)) == volume_id
    except (AttributeError, TypeError, ValueError):
        canonical_volume_id = False
    if not canonical_volume_id:
        raise ValueError(f"{context} has an invalid VOLUME_ID")

    expected_node = storage.get("node", "")
    if _required_hostname(pod_spec, context) != (expected_node or None):
        raise ValueError(f"{context} has a different storage node")
    if expected_node:
        if environment.get("HOSTNAME") != expected_node:
            raise ValueError(f"{context} has a different HOSTNAME")
    elif "HOSTNAME" in environment:
        raise ValueError(f"{context} has an unexpected HOSTNAME")

    _validate_server_volume_mounts(
        pod_spec,
        server,
        storage,
        volname,
        context,
    )
    return volume_id


def _matching_server_workloads(resources, volname, stateful_set):
    """Return workload objects whose complete names belong to one pool."""
    fragment = re.escape(_dns_friendly_volname(volname))
    suffix = r"\d+" if stateful_set else r"\d+-\d+"
    pattern = re.compile(rf"server-{fragment}-{suffix}")
    return [
        resource for resource in resources or []
        if pattern.fullmatch(str(_k8s_value(
            _k8s_value(resource, "metadata"),
            "name",
            "",
        ))) is not None
    ]


def _validate_server_workload_set(resources, obj, stateful_set):
    """Validate a complete, unambiguous live workload set for one pool."""
    volname = obj["metadata"]["name"]
    matching = _matching_server_workloads(resources, volname, stateful_set)
    if not matching:
        return set()
    expected_names = {
        get_brick_hostname(volname, index, suffix=False)
        + ("" if stateful_set else "-0")
        for index, _storage in enumerate(obj["spec"]["storage"])
    }
    names = [
        _k8s_value(_k8s_value(item, "metadata"), "name")
        for item in matching
    ]
    if len(names) != len(set(names)) or set(names) != expected_names:
        kind = "StatefulSet" if stateful_set else "Pod"
        raise ValueError(
            f"Existing server {kind} evidence is incomplete or ambiguous"
        )

    recovered = set()
    resources_by_name = dict(zip(names, matching))
    for index, _storage in enumerate(obj["spec"]["storage"]):
        name = get_brick_hostname(volname, index, suffix=False)
        if not stateful_set:
            name = f"{name}-0"
        recovered.add(_validate_live_server(
            resources_by_name[name],
            obj,
            index,
            stateful_set,
        ))
    return recovered


def recover_native_pool_volume_id(obj, pods, apps_v1_client=None):
    """Prove native backend geometry and recover its unique hosting UUID."""
    recovered = _validate_server_workload_set(pods, obj, False)
    list_statefulsets = getattr(
        apps_v1_client,
        "list_namespaced_stateful_set",
        None,
    )
    if list_statefulsets is not None:
        response = list_statefulsets(NAMESPACE)
        statefulsets = _k8s_value(response, "items", default=[]) or []
        recovered.update(_validate_server_workload_set(
            statefulsets,
            obj,
            True,
        ))
    if len(recovered) > 1:
        raise ValueError("Existing server workloads disagree on VOLUME_ID")
    return next(iter(recovered)) if recovered else None


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


def _requested_external_backend_identity(spec):
    """Return the external backend identity represented by a CR spec."""
    details = spec.get("details") or {}
    hosts = []
    if details.get("gluster_host"):
        hosts.append(details["gluster_host"])
    if details.get("gluster_hosts"):
        hosts.extend(details["gluster_hosts"])
    return {
        "gluster_hosts": _canonical_gluster_hosts(hosts),
        "gluster_volname": details.get("gluster_volname"),
        "gluster_options": details.get("gluster_options", ""),
    }


def _canonical_native_geometry(pool_type, source):
    """Return immutable native volume geometry from a spec or pool record."""
    disperse = {}
    if pool_type == VOLUME_TYPE_DISPERSE:
        requested = source.get("disperse") or {}
        disperse = {
            "data": requested.get("data", 0),
            "redundancy": requested.get("redundancy", 0),
        }

    tiebreaker = {}
    if pool_type == VOLUME_TYPE_REPLICA_2:
        requested = source.get("tiebreaker") or {}
        if requested:
            tiebreaker = {
                "deployment": requested.get("deployment", ""),
                "node": requested.get("node", ""),
                "path": requested.get("path", ""),
                "port": requested.get("port") or 24007,
            }
    return {"disperse": disperse, "tiebreaker": tiebreaker}


def _requested_native_backend_identity(spec):
    """Return ordered immutable brick placement and geometry from a CR."""
    identity = {
        "bricks": [
            {
                "node": brick.get("node", ""),
                "path": brick.get("path", ""),
                "device": brick.get("device", ""),
                "pvc": brick.get("pvc", ""),
            }
            for brick in spec.get("storage", [])
        ],
    }
    identity.update(_canonical_native_geometry(spec.get("type"), spec))
    return identity


def _stored_native_backend_identity(data):
    """Return ordered immutable brick placement and geometry from a record."""
    bricks = sorted(
        data.get("bricks") or [],
        key=lambda brick: brick.get("brick_index", -1),
    )
    identity = {
        "bricks": [
            {
                "node": brick.get("kube_hostname", ""),
                "path": brick.get("host_brick_path", ""),
                "device": brick.get("brick_device", ""),
                "pvc": brick.get("pvc_name", ""),
            }
            for brick in bricks
        ],
    }
    identity.update(_canonical_native_geometry(data.get("type"), data))
    return identity


def storage_class_backend_fingerprint(source):
    """Fingerprint immutable backend identity for durable SC recovery."""
    spec = source.get("spec")
    if isinstance(spec, dict):
        metadata = source.get("metadata") or {}
        volname = metadata.get("name")
        voltype = spec.get("type")
        single_pv_per_pool = get_single_pv_per_pool(spec)
        backend = (
            _requested_external_backend_identity(spec)
            if voltype == VOLUME_TYPE_EXTERNAL
            else _requested_native_backend_identity(spec)
        )
    else:
        volname = source.get("volname")
        voltype = source.get("type")
        single_pv_per_pool = get_single_pv_per_pool(source)
        backend = (
            _external_backend_identity(source)
            if voltype == VOLUME_TYPE_EXTERNAL
            else _stored_native_backend_identity(source)
        )
    serialized = json.dumps(
        {
            "schema": 1,
            "namespace": NAMESPACE,
            "name": volname,
            "type": voltype,
            "single_pv_per_pool": single_pv_per_pool,
            "backend": backend,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _validate_requested_backend_identity(obj, existing):
    """Reject a CR that retargets an existing pool's durable backend."""
    spec = obj["spec"]
    volname = (obj.get("metadata") or {}).get("name", "<unknown>")
    if existing.get("type") == VOLUME_TYPE_EXTERNAL:
        matches = (
            _requested_external_backend_identity(spec)
            == _external_backend_identity(existing)
        )
        backend_type = "external"
    else:
        matches = (
            _requested_native_backend_identity(spec)
            == _stored_native_backend_identity(existing)
        )
        backend_type = "native"
    if not matches:
        raise RuntimeError(
            f"Kadalustorage {volname} requests an immutable {backend_type} "
            "storage backend change"
        )


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


def _kubernetes_timestamp(value):
    """Normalize Kubernetes model or JSON timestamps for safe comparison."""
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value
        )
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _legacy_cr_predates_retained_storage(
        obj, data, core_v1_client, apps_v1_client):
    """Prove a UID-less CR predates every retained object for its pool."""
    metadata = obj.get("metadata") or {}
    created = _kubernetes_timestamp(metadata.get("creationTimestamp"))
    if created is None or core_v1_client is None:
        return False

    retained = []
    unknown_timestamp = False
    if data.get("type") != VOLUME_TYPE_EXTERNAL and apps_v1_client is not None:
        for brick in data.get("bricks") or []:
            name = get_brick_hostname(
                metadata.get("name", data.get("volname", "")),
                brick.get("brick_index"),
                suffix=False,
            )
            try:
                stateful_set = apps_v1_client.read_namespaced_stateful_set(
                    name,
                    NAMESPACE,
                )
            except Exception as err:  # pylint: disable=broad-exception-caught
                if getattr(err, "status", None) not in (404, "404"):
                    raise
                continue
            timestamp = _kubernetes_timestamp(
                getattr(stateful_set.metadata, "creation_timestamp", None)
            )
            if timestamp is None:
                unknown_timestamp = True
            else:
                retained.append(timestamp)

    pvs = _pool_persistent_volumes(
        core_v1_client,
        metadata.get("name", data.get("volname", "")),
        storage_class_name(data),
    )
    for pv in pvs:
        timestamp = _kubernetes_timestamp(
            getattr(pv.metadata, "creation_timestamp", None)
        )
        if timestamp is None:
            unknown_timestamp = True
        else:
            retained.append(timestamp)

    if unknown_timestamp:
        return False
    if not retained:
        # With no retained StatefulSet or PV, there is no older workload for a
        # new CR incarnation to adopt.
        return True
    return all(created <= timestamp for timestamp in retained)


def _validate_storage_record_owner(
        obj, data, core_v1_client=None, apps_v1_client=None):
    """Reject implicit adoption by a different Kadalustorage incarnation."""
    metadata = obj.get("metadata") or {}
    spec = obj.get("spec") or {}
    current_uid = metadata.get("uid")
    if not current_uid:
        # Kubernetes API objects always have UIDs. Keeping synthetic/unit
        # objects usable does not weaken the live-cluster ownership check.
        return

    stored_uid = data.get("storage_uid")
    if stored_uid:
        if stored_uid != current_uid:
            raise RuntimeError(
                f"Kadalustorage {metadata.get('name')} does not own its "
                "existing storage pool record"
            )
        return

    stored_volume_id = data.get("volume_id")
    requested_volume_id = spec.get("volume_id")
    if (
            stored_volume_id
            and requested_volume_id == stored_volume_id):
        return
    if (
            requested_volume_id is None
            and _legacy_cr_predates_retained_storage(
                obj,
                data,
                core_v1_client,
                apps_v1_client,
            )):
        # Server-authored immutable timestamps prove this is the CR which
        # predates the retained pool, not a same-name recreation. The next
        # config write persists UID and makes future checks direct.
        return
    raise RuntimeError(
        f"Legacy storage pool {metadata.get('name')} requires its existing "
        "volume_id before ownership can be established"
    )


def _validate_runtime_storage_record(
        obj, data, core_v1_client=None, apps_v1_client=None):
    """Validate retained pool identity and ownership before reconciliation."""
    volname = (obj.get("metadata") or {}).get("name", "<unknown>")
    _validate_stored_pool_identity(volname, data)
    if data.get("type") != VOLUME_TYPE_EXTERNAL:
        _validate_stored_bricks(volname, data)
    _validate_storage_record_owner(
        obj,
        data,
        core_v1_client,
        apps_v1_client,
    )
    if storage_class_name(obj) != storage_class_name(data):
        raise RuntimeError(
            f"Kadalustorage {volname} requests an immutable StorageClass "
            "name change"
        )
    _validate_requested_backend_identity(obj, data)


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
        legacy_mount_migration=False, recovered_mount_identity=None):
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
        if storage_class_name(data) != storage_class_name(existing):
            logging.error(logf(
                "Rejected immutable StorageClass name change",
                volname=data.get("volname"),
                existing=storage_class_name(existing),
                requested=storage_class_name(data),
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
        if (
                existing.get("type") != VOLUME_TYPE_EXTERNAL
                and _stored_native_backend_identity(data)
                != _stored_native_backend_identity(existing)):
            logging.error(logf(
                "Rejected immutable native storage backend change",
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
    if mount_identity is None and recovered_mount_identity is not None:
        mount_identity = _canonical_mount_identity(recovered_mount_identity)
        if mount_identity != recovered_mount_identity:
            logging.error(logf(
                "Rejected invalid recovered storage pool mount identity",
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
        data, existing_serialized, recovered_legacy_owner=None,
        recovered_mount_identity=None):
    """Prepare safe ownership and mount identity for a ConfigMap write."""
    if existing_serialized is None:
        return _apply_pool_mount_metadata(
            data,
            None,
            recovered_legacy_owner,
            recovered_mount_identity=recovered_mount_identity,
        )
    existing = _decode_pool_record(existing_serialized)
    if existing is None:
        return False
    return _apply_pool_mount_metadata(
        data,
        existing,
        recovered_mount_identity=recovered_mount_identity,
    )


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


def _pool_persistent_volumes(
        core_v1_client, volname, class_name=None):
    """Yield every PV attributable to one Kadalu hosting pool."""
    if class_name is None:
        class_name = f"{STORAGE_CLASS_NAME_PREFIX}{volname}"
    persistent_volumes = core_v1_client.list_persistent_volume().items or []
    for persistent_volume in persistent_volumes:
        spec = getattr(persistent_volume, "spec", None)
        csi_source = getattr(spec, "csi", None)
        attributes = getattr(csi_source, "volume_attributes", None) or {}
        matches_hostvol = (
                csi_source is not None
                and getattr(csi_source, "driver", None) == CSI_DRIVER_NAME
                and isinstance(attributes, dict)
                and attributes.get("hostvol") == volname
        )
        matches_storage_class = (
            getattr(spec, "storage_class_name", None)
            == class_name
        )
        if matches_hostvol or matches_storage_class:
            yield persistent_volume


def _pool_persistent_volume_is_mutable(persistent_volume, volname):
    """Return whether one attributable PV is safe for Kadalu to mutate."""
    spec = persistent_volume.spec
    csi_source = getattr(spec, "csi", None)
    attributes = getattr(csi_source, "volume_attributes", None) or {}
    attributed_hostvol = (
        attributes.get("hostvol")
        if isinstance(attributes, dict)
        else None
    )
    return (
        csi_source is not None
        and getattr(csi_source, "driver", None) == CSI_DRIVER_NAME
        and (
            not attributed_hostvol
            or attributed_hostvol == volname
        )
    )


def reconcile_pool_pv_reclaim_policy(
        core_v1_client, volname, pool_policy, before_config_write,
        class_name=None):
    """Patch existing PV policy on the data-safe side of a config write."""
    reclaim_policy = storage_class_reclaim_policy(pool_policy)
    if (
            before_config_write is not None
            and (reclaim_policy == "Retain") != before_config_write):
        return

    for persistent_volume in _pool_persistent_volumes(
            core_v1_client, volname, class_name):
        spec = persistent_volume.spec
        if not _pool_persistent_volume_is_mutable(
                persistent_volume, volname):
            # StorageClass-name attribution is deliberately broad for
            # fail-closed deletion counts. It is not sufficient authority to
            # mutate a foreign driver's (or another pool's) data policy.
            continue
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
        core_v1_client, data, existing_serialized,
        recovered_mount_identity=None):
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
        recovered_mount_identity,
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
        f"exist: {details}. Remove the block volumes before changing the mount "
        "boundary. If provisioning is already quiesced, keep it quiesced; "
        "stopping workloads alone is not sufficient because an old block "
        "target may lack recovery state."
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


def list_storage_resources(storage_client):
    """Return one authoritative Kadalustorage list and its watch boundary."""
    storage_list = storage_client.list_namespaced_custom_object(
        "kadalu-operator.storage",
        "v1alpha1",
        NAMESPACE,
        "kadalustorages",
    )
    items = storage_list.get("items")
    metadata = storage_list.get("metadata") or {}
    resource_version = metadata.get("resourceVersion")
    if not isinstance(items, list) or not resource_version:
        raise RuntimeError(
            "Unable to establish an authoritative Kadalustorage snapshot"
        )
    return storage_list


def _valid_qualified_label_name(value):
    """Return whether value follows Kubernetes qualified-name syntax."""
    parts = value.split("/")
    if len(parts) > 2:
        return False
    name = parts[-1]
    if (
            not name
            or len(name) > 63
            or LABEL_NAME_RE.fullmatch(name) is None):
        return False
    if len(parts) == 1:
        return True
    prefix = parts[0]
    return (
        bool(prefix)
        and len(prefix) <= 253
        and DNS_SUBDOMAIN_RE.fullmatch(prefix) is not None
    )


def _valid_label_value(value):
    """Return whether value follows Kubernetes label-value syntax."""
    return (
        value == ""
        or (
            len(value) <= 63
            and LABEL_NAME_RE.fullmatch(value) is not None
        )
    )


# pylint: disable=too-many-boolean-expressions
def _normalized_toleration(toleration, context):
    """Convert a dict or Kubernetes toleration model to API field names."""
    fields = (
        ("effect", "effect"),
        ("key", "key"),
        ("operator", "operator"),
        ("tolerationSeconds", "toleration_seconds"),
        ("value", "value"),
    )
    normalized = {}
    for api_name, model_name in fields:
        if isinstance(toleration, dict):
            value = toleration.get(api_name)
        else:
            value = getattr(toleration, api_name, None)
        if value is None and model_name != api_name:
            if isinstance(toleration, dict):
                value = toleration.get(model_name)
            else:
                value = getattr(toleration, model_name, None)
        if value is not None:
            normalized[api_name] = value

    if not isinstance(toleration, dict) and not normalized:
        raise RuntimeError(f"{context} contains an invalid toleration")
    for field in ("effect", "key", "operator", "value"):
        if field in normalized and not isinstance(normalized[field], str):
            raise RuntimeError(f"{context} contains an invalid toleration")
    toleration_seconds = normalized.get("tolerationSeconds")
    if (
            toleration_seconds is not None
            and (
                isinstance(toleration_seconds, bool)
                or not isinstance(toleration_seconds, int)
                or toleration_seconds < -(2 ** 63)
                or toleration_seconds > (2 ** 63) - 1
            )):
        raise RuntimeError(f"{context} contains an invalid toleration")

    key = normalized.get("key", "")
    operator = normalized.get("operator", "")
    effect = normalized.get("effect", "")
    value = normalized.get("value", "")
    effective_operator = operator or "Equal"
    if (
            effective_operator not in TOLERATION_OPERATORS
            or effect not in TOLERATION_EFFECTS
            or (key and not _valid_qualified_label_name(key))
            or not _valid_label_value(value)
            or (not key and effective_operator != "Exists")
            or (effective_operator == "Exists" and value != "")
            or (
                toleration_seconds is not None
                and effect != "NoExecute"
            )):
        raise RuntimeError(f"{context} contains an invalid toleration")

    # Match Kubernetes API defaulting and round-trip behavior so a CR, a
    # stored pool record, and a live Pod template compare semantically. The
    # API defaults an omitted/empty operator to Equal and drops empty scalar
    # fields when it serializes the resulting Toleration.
    canonical = {"operator": effective_operator}
    for field, value in (
            ("key", key),
            ("effect", effect),
            ("value", value)):
        if value:
            canonical[field] = value
    if toleration_seconds is not None:
        canonical["tolerationSeconds"] = toleration_seconds
    return canonical


def normalize_tolerations(tolerations, context):
    """Validate, de-duplicate, and deterministically order tolerations."""
    if tolerations is None:
        return []
    if not isinstance(tolerations, list):
        raise RuntimeError(f"{context} has invalid tolerations")

    canonical = {}
    for toleration in tolerations:
        normalized = _normalized_toleration(toleration, context)
        serialized = json.dumps(
            normalized,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        canonical[serialized] = normalized
    return sorted(
        canonical.values(),
        key=lambda item: (
            str(item.get("key", "")),
            str(item.get("effect", "")),
            str(item.get("operator", "")),
            str(item.get("value", "")),
            item.get("tolerationSeconds", -1),
        ),
    )


def _validate_stored_bricks(volname, data):
    """Validate the legacy brick order before any StatefulSet is changed."""
    bricks = data.get("bricks")
    if not isinstance(bricks, list):
        raise RuntimeError(
            f"Storage pool {volname} has invalid stored bricks"
        )

    indices = []
    required_fields = (
        "brick_path",
        "node",
        "node_id",
        "host_brick_path",
        "kube_hostname",
        "brick_device",
        "pvc_name",
    )
    for brick in bricks:
        if not isinstance(brick, dict):
            raise RuntimeError(
                f"Storage pool {volname} has invalid stored bricks"
            )
        index = brick.get("brick_index")
        if isinstance(index, bool) or not isinstance(index, int):
            raise RuntimeError(
                f"Storage pool {volname} has unsafe brick indices"
            )
        indices.append(index)

    if sorted(indices) != list(range(len(bricks))):
        raise RuntimeError(
            f"Storage pool {volname} has unsafe brick indices"
        )

    for brick in bricks:
        index = brick["brick_index"]
        if any(field not in brick for field in required_fields):
            raise RuntimeError(
                f"Storage pool {volname} has incomplete stored brick {index}"
            )
        if (
                brick["node_id"] != f"node-{index}"
                or brick["node"] != get_brick_hostname(volname, index)
                or brick["brick_path"]
                != f"/bricks/{volname}/data/brick"):
            raise RuntimeError(
                f"Storage pool {volname} has inconsistent stored brick "
                f"identity {index}"
            )

    logical_bricks = [
        {
            "node": brick["kube_hostname"],
            "path": brick["host_brick_path"],
            "device": brick["brick_device"],
            "pvc": brick["pvc_name"],
        }
        for brick in sorted(bricks, key=lambda item: item["brick_index"])
    ]
    if not bricks_validation(logical_bricks):
        raise RuntimeError(
            f"Storage pool {volname} has invalid stored brick backing"
        )
    return bricks


def _validate_stored_pool_identity(volname, data):
    """Validate operator-owned pool identity before any upgrade mutation."""
    if data.get("volname") != volname:
        raise RuntimeError(
            f"Storage pool record {volname} has an inconsistent volname"
        )
    bricks = data.get("bricks")
    if not generated_resource_names_validation({
            "metadata": {"name": volname},
            "spec": {
                "type": data.get("type"),
                "storageClassName": data.get("storageClassName"),
                "storage": (
                    [{} for _brick in bricks]
                    if isinstance(bricks, list)
                    else []
                ),
            },
    }):
        raise RuntimeError(
            f"Storage pool {volname} has unsafe generated resource names"
        )
    volume_id = data.get("volume_id")
    try:
        canonical_volume_id = str(uuid.UUID(volume_id)) == volume_id
    except (AttributeError, TypeError, ValueError):
        canonical_volume_id = False
    if not canonical_volume_id:
        raise RuntimeError(
            f"Storage pool {volname} has an invalid stored volume identity"
        )
    cleanup_only = data.get(CLEANUP_ONLY_RECORD_FIELD)
    if cleanup_only is not None and cleanup_only is not True:
        raise RuntimeError(
            f"Storage pool {volname} has invalid cleanup-only metadata"
        )
    if cleanup_only is True and (
            not data.get("storage_uid")
            or data.get("provisioning_disabled") is not True):
        raise RuntimeError(
            f"Storage pool {volname} has incomplete cleanup-only metadata"
        )
    deletion_quarantined = data.get(DELETION_QUARANTINE_RECORD_FIELD)
    if deletion_quarantined is not None and deletion_quarantined is not True:
        raise RuntimeError(
            f"Storage pool {volname} has invalid deletion quarantine metadata"
        )
    if deletion_quarantined is True and (
            data.get("provisioning_disabled") is not True
            or data.get("storage_uid") is not None
            or cleanup_only is True):
        raise RuntimeError(
            f"Storage pool {volname} has inconsistent deletion quarantine "
            "metadata"
        )

    mount_identity = data.get("mount_identity")
    if (
            mount_identity is not None
            and _canonical_mount_identity(mount_identity) != mount_identity):
        raise RuntimeError(
            f"Storage pool {volname} has an invalid stored mount identity"
        )
    fingerprint = data.get("mount_config_fingerprint")
    if (
            fingerprint is not None
            and fingerprint != mount_config_fingerprint(data)):
        raise RuntimeError(
            f"Storage pool {volname} has inconsistent stored mount metadata"
        )


def storage_record_cleanup_only(data):
    """Return whether a tombstoned record can only finish old cleanup."""
    storage_uid = data.get("storage_uid")
    return (
        data.get(CLEANUP_ONLY_RECORD_FIELD) is True
        or (
            data.get("provisioning_disabled") is True
            and isinstance(storage_uid, str)
            and storage_uid.startswith(CLEANUP_ONLY_STORAGE_UID_PREFIX)
        )
    )


def storage_record_blocks_adoption(data):
    """Return whether a deletion record must reject same-name CRs."""
    return (
        storage_record_cleanup_only(data)
        or data.get(DELETION_QUARANTINE_RECORD_FIELD) is True
        or data.get("provisioning_disabled") is True
    )


def event_targets_deletion_blocked_record(core_v1_client, obj):
    """Detect a CR event which must not adopt a deletion-blocked record."""
    if core_v1_client is None:
        return False
    read_config_map = getattr(
        core_v1_client,
        "read_namespaced_config_map",
        None,
    )
    if read_config_map is None:
        return False
    volname = (obj.get("metadata") or {}).get("name")
    if not volname:
        return False
    configmap_data = read_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    serialized = (configmap_data.data or {}).get(f"{volname}.info")
    if serialized is None:
        return False
    record = _decode_pool_record(serialized)
    if record is None:
        raise RuntimeError(
            f"Storage metadata for {volname} is invalid during event dispatch"
        )
    _validate_stored_pool_identity(volname, record)
    return storage_record_blocks_adoption(record)


def _statefulset_tolerations(stateful_set, volname):
    """Return normalized tolerations from one live server template."""
    spec = getattr(stateful_set, "spec", None)
    pod_template = getattr(spec, "template", None)
    pod_spec = getattr(pod_template, "spec", None)
    if pod_spec is None:
        raise RuntimeError(
            f"Storage pool {volname} has an invalid live StatefulSet template"
        )
    return normalize_tolerations(
        getattr(pod_spec, "tolerations", None),
        f"Storage pool {volname}",
    )


def _orphan_tolerations(apps_v1_client, volname, data, bricks):
    """Recover one orphan's scheduling policy without depending on its CR."""
    has_persisted = "tolerations" in data
    persisted = normalize_tolerations(
        data.get("tolerations"),
        f"Storage pool {volname}",
    )
    live = []
    missing = False
    for brick in bricks:
        name = get_brick_hostname(
            volname,
            brick["brick_index"],
            suffix=False,
        )
        try:
            stateful_set = apps_v1_client.read_namespaced_stateful_set(
                name,
                NAMESPACE,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            if getattr(err, "status", None) not in (404, "404"):
                raise
            missing = True
            continue
        live.append(_statefulset_tolerations(stateful_set, volname))

    if live and any(tolerations != live[0] for tolerations in live[1:]):
        raise RuntimeError(
            f"Storage pool {volname} has inconsistent live tolerations"
        )
    if live and missing and "tolerations" in data and persisted != live[0]:
        raise RuntimeError(
            f"Storage pool {volname} has inconsistent live tolerations"
        )
    if live:
        return live[0]
    if not has_persisted:
        raise MissingTolerationSourceError(
            f"Storage pool {volname} has no trustworthy toleration source"
        )
    return persisted


def _nodeplugin_template_tolerations(apps_v1_client):
    """Read the currently published shared nodeplugin scheduling policy."""
    try:
        daemon_set = apps_v1_client.read_namespaced_daemon_set(
            NODE_PLUGIN,
            NAMESPACE,
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        if getattr(err, "status", None) not in (404, "404"):
            raise
        raise RuntimeError(
            "A legacy External orphan has no persisted tolerations and the "
            "current nodeplugin scheduling policy is unavailable"
        ) from err

    spec = getattr(daemon_set, "spec", None)
    pod_template = getattr(spec, "template", None)
    pod_spec = getattr(pod_template, "spec", None)
    if pod_spec is None:
        raise RuntimeError(
            "A legacy External orphan has no persisted tolerations and the "
            "current nodeplugin scheduling policy is invalid"
        )
    return normalize_tolerations(
        getattr(pod_spec, "tolerations", None),
        "Current nodeplugin template",
    )


def _zero_pv_orphan_tolerations(
        core_v1_client, apps_v1_client, volname, data,
        missing_source_error):
    """Preserve shared policy while allowing an empty partial delete to end."""
    if get_num_pvs(core_v1_client, data) != 0:
        raise missing_source_error
    try:
        tolerations = _nodeplugin_template_tolerations(apps_v1_client)
    except RuntimeError:
        tolerations = []
    logging.warning(logf(
        "Finishing zero-PV orphan without a pool toleration source",
        volname=volname,
    ))
    return tolerations


def _upgrade_object_from_record(volname, data, bricks, tolerations):
    """Reconstruct the stable server inputs stored for one native pool."""
    volume_id = data.get("volume_id")
    if not volume_id:
        raise RuntimeError(
            f"Storage pool {volname} has no stored volume identity"
        )
    storage = []
    for brick in sorted(bricks, key=lambda item: item["brick_index"]):
        storage.append({
            "node_id": brick["node_id"],
            "path": brick["host_brick_path"],
            "node": brick["kube_hostname"],
            "device": brick["brick_device"],
            "pvc": brick["pvc_name"],
        })
    return {
        "metadata": {"name": volname},
        "spec": {
            "type": data["type"],
            "pvReclaimPolicy": data.get("pvReclaimPolicy", "delete"),
            "storageClassName": storage_class_name(data),
            "volume_id": volume_id,
            "storage": storage,
            "tolerations": tolerations,
        },
    }


def validate_unique_storage_class_names(records, active_objects):
    """Reject plans where multiple pools target one cluster-scoped class."""
    owners_by_class = {}
    for volname, source in list(records.items()) + list(active_objects.items()):
        owners_by_class.setdefault(storage_class_name(source), set()).add(
            volname
        )
    conflicts = {
        name: sorted(owners)
        for name, owners in owners_by_class.items()
        if len(owners) > 1
    }
    if conflicts:
        details = "; ".join(
            f"{name}: {', '.join(owners)}"
            for name, owners in sorted(conflicts.items())
        )
        raise RuntimeError(
            "Multiple Kadalustorages request the same StorageClass; "
            f"startup reconciliation was not attempted ({details})"
        )


def prepare_storage_upgrade(
        core_v1_client, apps_v1_client, storage_list, storage_api=None):
    """Preflight active resources and deletion-orphans without mutation."""
    storage_items = storage_list.get("items")
    metadata = storage_list.get("metadata") or {}
    if not isinstance(storage_items, list):
        raise RuntimeError(
            "Unable to verify Kadalustorage resources before server upgrade"
        )
    if storage_api is None:
        storage_api = client.StorageV1Api()

    snapshot_objects = {}
    active_objects = {}
    invalid_objects = {}
    deleting_objects = {}
    for storage_obj in storage_items:
        if not isinstance(storage_obj, dict):
            raise RuntimeError("Invalid Kadalustorage resource in snapshot")
        obj_metadata = storage_obj.get("metadata") or {}
        name = obj_metadata.get("name")
        if not name:
            raise RuntimeError("Unnamed Kadalustorage resource in snapshot")
        if name in snapshot_objects:
            raise RuntimeError(
                f"Duplicate Kadalustorage resource for {name}; "
                "server upgrade was not attempted"
            )
        snapshot_objects[name] = storage_obj
        if obj_metadata.get("deletionTimestamp"):
            deleting_objects[name] = storage_obj
            continue
        spec = storage_obj.get("spec")
        if not isinstance(spec, dict):
            invalid_objects[name] = storage_obj
            logging.error(logf(
                "Quarantined invalid Kadalustorage",
                storage=name,
            ))
            continue
        if not validate_volume_request(storage_obj):
            invalid_objects[name] = storage_obj
            logging.error(logf(
                "Quarantined invalid Kadalustorage",
                storage=name,
            ))
            continue
        normalize_tolerations(
            spec.get("tolerations"),
            f"Kadalustorage {name}",
        )
        active_objects[name] = storage_obj

    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    records = {}
    for key, serialized in (configmap_data.data or {}).items():
        if not key.endswith(".info"):
            continue
        volname = key[:-len(".info")]
        data = _decode_pool_record(serialized)
        if data is None:
            raise RuntimeError(f"Invalid storage pool metadata in {key}")
        pool_type = data.get("type")
        if pool_type not in VALID_HOSTING_VOLUME_TYPES:
            raise RuntimeError(
                f"Storage pool {volname} has invalid stored type"
            )
        _validate_stored_pool_identity(volname, data)
        records[volname] = data

    validate_unique_storage_class_names(records, active_objects)

    all_tolerations = []
    upgrade_objects = []
    deletion_orphans = []
    for volname in sorted(records):
        data = records[volname]
        bricks = None
        tolerations = []
        if data["type"] != VOLUME_TYPE_EXTERNAL:
            bricks = _validate_stored_bricks(volname, data)
        storage_obj = active_objects.get(volname)
        quarantined = volname in invalid_objects
        deletion_obj = deleting_objects.get(volname)
        cleanup_only = storage_record_cleanup_only(data)
        deletion_quarantined = (
            data.get(DELETION_QUARANTINE_RECORD_FIELD) is True
        )
        observed_obj = (
            storage_obj
            or deletion_obj
            or invalid_objects.get(volname)
        )
        if (
                observed_obj is not None
                and storage_record_blocks_adoption(data)):
            logging.error(logf(
                "Quarantined Kadalustorage which cannot adopt a deletion "
                "orphan",
                storage=volname,
            ))
            invalid_objects[volname] = observed_obj
            active_objects.pop(volname, None)
            deleting_objects.pop(volname, None)
            storage_obj = None
            deletion_obj = None
            # The replacement CR is quarantined, but the cleanup-only record
            # must continue draining independently.
            quarantined = False
        legacy_storage_uid = None
        legacy_quarantine = deletion_quarantined
        cleanup_quarantine = False
        storage_class_preflight_complete = deletion_quarantined
        if cleanup_only:
            try:
                preflight_storage_class_owner(storage_api, data)
            except Exception as err:  # pylint: disable=broad-exception-caught
                cleanup_quarantine = True
                logging.error(logf(
                    "Quarantined cleanup-only deletion orphan",
                    storage=volname,
                    error=err,
                ))
            storage_class_preflight_complete = True
        if (
                storage_obj is None
                and not quarantined
                and not data.get("storage_uid")
                and not deletion_quarantined):
            if deletion_obj is not None:
                _validate_storage_record_owner(
                    deletion_obj,
                    data,
                    core_v1_client,
                    apps_v1_client,
                )
                candidate_storage_uid = (
                    (deletion_obj.get("metadata") or {}).get("uid")
                )
                if not candidate_storage_uid:
                    raise RuntimeError(
                        f"Deleting Kadalustorage {volname} has no UID"
                    )
                try:
                    preflight_legacy_orphan_storage_class(
                        storage_api,
                        data,
                    )
                except RuntimeError as err:
                    legacy_quarantine = True
                    logging.error(logf(
                        "Quarantined UID-less deleting storage pool",
                        storage=volname,
                        error=err,
                    ))
                else:
                    legacy_storage_uid = candidate_storage_uid
                storage_class_preflight_complete = True
            else:
                try:
                    preflight_legacy_orphan_storage_class(
                        storage_api,
                        data,
                    )
                except RuntimeError as err:
                    legacy_quarantine = True
                    logging.error(logf(
                        "Quarantined UID-less deletion orphan",
                        storage=volname,
                        error=err,
                    ))
                else:
                    legacy_storage_uid = (
                        f"{CLEANUP_ONLY_STORAGE_UID_PREFIX}"
                        f"{data['volume_id']}"
                    )
                storage_class_preflight_complete = True
        elif deletion_obj is not None and not quarantined:
            _validate_deletion_record_owner(deletion_obj, data)

        if storage_obj is not None:
            _validate_storage_record_owner(
                storage_obj,
                data,
                core_v1_client,
                apps_v1_client,
            )
            spec = storage_obj["spec"]
            if spec.get("type") != data.get("type"):
                raise RuntimeError(
                    f"Kadalustorage {volname} does not match its stored pool "
                    "type; server upgrade was not attempted"
                )
            if storage_class_name(storage_obj) != storage_class_name(data):
                raise RuntimeError(
                    f"Kadalustorage {volname} requests an immutable "
                    "StorageClass name change; server upgrade was not "
                    "attempted"
                )
            _validate_requested_backend_identity(storage_obj, data)
            preflight_existing_pool_storage_class(
                storage_api,
                storage_obj,
                data,
            )
            tolerations = normalize_tolerations(
                spec.get("tolerations"),
                f"Kadalustorage {volname}",
            )
        else:
            if (
                    not storage_class_preflight_complete
                    and data.get("provisioning_disabled") is True):
                try:
                    preflight_storage_class_owner(storage_api, data)
                except Exception as err:  # pylint: disable=broad-exception-caught
                    cleanup_quarantine = True
                    logging.error(logf(
                        "Quarantined tombstoned storage pool with a "
                        "conflicting StorageClass",
                        storage=volname,
                        error=err,
                    ))
                storage_class_preflight_complete = True

        if storage_obj is None:
            if data["type"] == VOLUME_TYPE_EXTERNAL:
                if not storage_class_preflight_complete:
                    preflight_storage_class_owner(storage_api, data)
                if "tolerations" in data:
                    tolerations = normalize_tolerations(
                        data["tolerations"],
                        f"Storage pool {volname}",
                    )
                else:
                    try:
                        tolerations = _nodeplugin_template_tolerations(
                            apps_v1_client,
                        )
                    except RuntimeError as err:
                        tolerations = _zero_pv_orphan_tolerations(
                            core_v1_client,
                            apps_v1_client,
                            volname,
                            data,
                            err,
                        )
            else:
                if not storage_class_preflight_complete:
                    preflight_storage_class_owner(storage_api, data)
                try:
                    tolerations = _orphan_tolerations(
                        apps_v1_client,
                        volname,
                        data,
                        bricks,
                    )
                except MissingTolerationSourceError as err:
                    tolerations = _zero_pv_orphan_tolerations(
                        core_v1_client,
                        apps_v1_client,
                        volname,
                        data,
                        err,
                    )

        all_tolerations.extend(tolerations)
        if storage_obj is None and not quarantined:
            if deletion_obj is None:
                deletion_obj = {
                    "metadata": {"name": volname},
                    "spec": {"type": data["type"]},
                }
            deletion_orphan = {
                "object": deletion_obj,
                "record": data,
            }
            if legacy_storage_uid is not None:
                deletion_orphan["legacy_storage_uid"] = legacy_storage_uid
            if legacy_quarantine:
                deletion_orphan["legacy_quarantine"] = True
            if cleanup_quarantine:
                deletion_orphan["cleanup_quarantine"] = True
            deletion_orphans.append(deletion_orphan)
        if (
                data["type"] == VOLUME_TYPE_EXTERNAL
                or storage_obj is None):
            continue
        upgrade_objects.append(_upgrade_object_from_record(
            volname,
            data,
            bricks,
            tolerations,
        ))

    for volname, storage_obj in active_objects.items():
        if volname in records:
            continue
        preflight_storage_class_owner(storage_api, storage_obj)
        all_tolerations.extend(normalize_tolerations(
            storage_obj["spec"].get("tolerations"),
            f"Kadalustorage {volname}",
        ))

    return {
        "items": storage_items,
        "reconcile_items": list(active_objects.values()),
        "invalid_items": list(invalid_objects.values()),
        "resource_version": metadata.get("resourceVersion"),
        "upgrade_objects": upgrade_objects,
        "deletion_orphans": deletion_orphans,
        "nodeplugin_tolerations": normalize_tolerations(
            all_tolerations,
            "Kadalustorage snapshot",
        ),
    }


def reconcile_nodeplugin_tolerations(apps_v1_client, tolerations):
    """Set the shared nodeplugin policy and propagate any API failure."""
    apps_v1_client.patch_namespaced_daemon_set(
        NODE_PLUGIN,
        NAMESPACE,
        {"spec": {"template": {"spec": {"tolerations": tolerations}}}},
    )
    logging.info(logf(
        "Reconciled nodeplugin tolerations",
        tolerations=tolerations,
    ))


def reconcile_nodeplugin_from_storage(
        core_v1_client, apps_v1_client, storage_client,
        storage_list=None):
    """Recompute and apply the union from one authoritative snapshot."""
    if storage_list is None:
        storage_list = list_storage_resources(storage_client)
    storage_plan = prepare_storage_upgrade(
        core_v1_client,
        apps_v1_client,
        storage_list,
    )
    reconcile_nodeplugin_tolerations(
        apps_v1_client,
        storage_plan["nodeplugin_tolerations"],
    )
    return storage_plan


def gate_nodeplugin_until_current(
        core_v1_client, apps_v1_client, storage_client, storage_plan,
        current_tolerations=None):
    """Gate every desired nodeplugin revision until a fresh union matches."""
    gated_tolerations = current_tolerations
    candidate = storage_plan
    while True:
        desired_tolerations = candidate["nodeplugin_tolerations"]
        if gated_tolerations != desired_tolerations:
            reconcile_nodeplugin_tolerations(
                apps_v1_client,
                desired_tolerations,
            )
            wait_for_csi_nodeplugin_rollout(apps_v1_client)
            gated_tolerations = desired_tolerations

        fresh = prepare_storage_upgrade(
            core_v1_client,
            apps_v1_client,
            list_storage_resources(storage_client),
        )
        if fresh["nodeplugin_tolerations"] == gated_tolerations:
            return fresh
        candidate = fresh


def upgrade_storage_pods(
        core_v1_client, apps_v1_client, storage_client,
        storage_plan=None):
    """Upgrade every active native server from a fully preflighted plan."""
    if storage_plan is None:
        storage_plan = prepare_storage_upgrade(
            core_v1_client,
            apps_v1_client,
            list_storage_resources(storage_client),
        )
    for obj in storage_plan["upgrade_objects"]:
        deploy_server_pods(obj, apps_v1_client)


def reconcile_deletion_orphans(
        core_v1_client, deletion_orphans, apps_v1_client=None,
        provisioner_fenced=False, storage_client=None):
    """Retry every skipped deletion and fail closed on uncertain outcomes."""
    for orphan in deletion_orphans:
        obj = orphan["object"]
        if not handle_deleted(
                core_v1_client,
                obj,
                storage_info_data=orphan["record"],
                apps_v1_client=apps_v1_client,
                provisioner_fenced=provisioner_fenced,
                storage_client=storage_client,
                legacy_storage_uid=orphan.get("legacy_storage_uid"),
                legacy_quarantine=orphan.get(
                    "legacy_quarantine",
                    False,
                ),
                cleanup_quarantine=orphan.get(
                    "cleanup_quarantine",
                    False,
                )):
            name = (obj.get("metadata") or {}).get("name", "<unknown>")
            raise RuntimeError(
                f"Deletion reconciliation failed for Kadalustorage {name}"
            )
    return True


def reconcile_planned_deletion_orphans(
        core_v1_client, apps_v1_client, storage_client, storage_plan,
        provisioner_fenced=False):
    """Reconcile one plan, supplying a CR client only for legacy sealing."""
    kwargs = {}
    if any(
            orphan.get("legacy_storage_uid") is not None
            or orphan.get("legacy_quarantine")
            for orphan in storage_plan["deletion_orphans"]):
        kwargs["storage_client"] = storage_client
    if provisioner_fenced:
        kwargs["provisioner_fenced"] = True
    return reconcile_deletion_orphans(
        core_v1_client,
        storage_plan["deletion_orphans"],
        apps_v1_client,
        **kwargs,
    )


def reconcile_storage_plan(
        core_v1_client, apps_v1_client, storage_client, storage_plan):
    """Reconcile native orphans, active objects, and skipped deletions."""
    upgrade_storage_pods(
        core_v1_client,
        apps_v1_client,
        storage_client,
        storage_plan=storage_plan,
    )
    reconcile_initial_storage(
        core_v1_client,
        storage_plan["reconcile_items"],
        apps_v1_client,
        provisioner_fenced=True,
    )
    reconcile_planned_deletion_orphans(
        core_v1_client,
        apps_v1_client,
        storage_client,
        storage_plan,
        provisioner_fenced=True,
    )
    return True


def _storage_object_fingerprint(obj):
    """Return only the Custom Resource fields consumed by reconciliation."""
    metadata = obj.get("metadata") or {}
    desired_metadata = {
        "name": metadata.get("name"),
        "uid": metadata.get("uid"),
    }
    if metadata.get("deletionTimestamp") is not None:
        desired_metadata["deletionTimestamp"] = metadata["deletionTimestamp"]
    return {
        "metadata": desired_metadata,
        "spec": obj.get("spec"),
    }


def _storage_plan_fingerprint(storage_plan):
    """Capture canonical desired state before handlers mutate CR dictionaries."""
    items = sorted(
        (_storage_object_fingerprint(item) for item in storage_plan["items"]),
        key=lambda item: (
            str(item["metadata"].get("name", "")),
            str(item["metadata"].get("uid", "")),
        ),
    )
    deletion_orphans = sorted(
        ({
            "object": _storage_object_fingerprint(orphan["object"]),
            "record": orphan["record"],
            "legacy_storage_uid": orphan.get("legacy_storage_uid"),
            "legacy_quarantine": bool(orphan.get("legacy_quarantine")),
            "cleanup_quarantine": bool(orphan.get("cleanup_quarantine")),
        } for orphan in storage_plan["deletion_orphans"]),
        key=lambda orphan: (
            str(orphan["object"]["metadata"].get("name", "")),
            str(orphan["object"]["metadata"].get("uid", "")),
        ),
    )
    return json.dumps(
        {
            "items": items,
            "invalid_items": sorted(
                (_storage_object_fingerprint(item)
                 for item in storage_plan["invalid_items"]),
                key=lambda item: (
                    str(item["metadata"].get("name", "")),
                    str(item["metadata"].get("uid", "")),
                ),
            ),
            "upgrade_objects": sorted(
                storage_plan["upgrade_objects"],
                key=lambda obj: str((obj.get("metadata") or {}).get("name", "")),
            ),
            "deletion_orphans": deletion_orphans,
            "nodeplugin_tolerations": storage_plan["nodeplugin_tolerations"],
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def reconcile_storage_until_stable(
        core_v1_client, apps_v1_client, storage_client, storage_plan):
    """Keep the controller fenced until two semantic snapshots agree."""
    candidate = storage_plan
    gated_tolerations = storage_plan["nodeplugin_tolerations"]
    while True:
        candidate_fingerprint = _storage_plan_fingerprint(candidate)
        reconcile_storage_plan(
            core_v1_client,
            apps_v1_client,
            storage_client,
            candidate,
        )
        fresh = prepare_storage_upgrade(
            core_v1_client,
            apps_v1_client,
            list_storage_resources(storage_client),
        )
        fresh = gate_nodeplugin_until_current(
            core_v1_client,
            apps_v1_client,
            storage_client,
            fresh,
            current_tolerations=gated_tolerations,
        )
        gated_tolerations = fresh["nodeplugin_tolerations"]
        if _storage_plan_fingerprint(fresh) == candidate_fingerprint:
            return fresh
        candidate = fresh


def update_config_map(
        core_v1_client, obj, recovered_mount_identity=None):
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
        "storageClassName": storage_class_name(obj),
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
    if "tolerations" in obj["spec"]:
        data["tolerations"] = obj["spec"]["tolerations"]
    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)
    volinfo_file = "%s.info" % volname
    existing_serialized = configmap_data.data.get(volinfo_file)
    existing_record = None
    if existing_serialized is not None:
        existing_record = _decode_pool_record(existing_serialized)
        if existing_record is None:
            return False
    storage_uid = (obj.get("metadata") or {}).get("uid")
    if storage_uid:
        data["storage_uid"] = storage_uid
    elif existing_record is not None:
        if existing_record.get("storage_uid"):
            data["storage_uid"] = existing_record["storage_uid"]

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
            core_v1_client,
            data,
            existing_serialized,
            recovered_mount_identity,
    ):
        return False

    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=True,
        class_name=data["storageClassName"],
    )
    configmap_data.data[volinfo_file] = json.dumps(data)

    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=False,
        class_name=data["storageClassName"],
    )
    logging.info(logf("Updated configmap", name=KADALU_CONFIG_MAP,
                      volname=volname))
    return True


def storage_heal_summary_is_clean(summary, expected_bricks):
    """Return whether every brick is connected with no pending heal work."""
    if not isinstance(summary, str) or expected_bricks < 1:
        return False

    statuses = re.findall(r"^Status:\s*(.+?)\s*$", summary, re.MULTILINE)
    if (
            len(statuses) != expected_bricks
            or any(status != "Connected" for status in statuses)):
        return False

    counters = (
        "Total Number of entries",
        "Number of entries in heal pending",
        "Number of entries in split-brain",
        "Number of entries possibly healing",
    )
    for label in counters:
        values = re.findall(
            rf"^{re.escape(label)}:\s*([0-9]+)\s*$",
            summary,
            re.MULTILINE,
        )
        if (
                len(values) != expected_bricks
                or any(int(value) != 0 for value in values)):
            return False
    return True


def wait_for_storage_heal(
        volname, server_names, timeout_seconds=HEAL_WAIT_TIMEOUT_SECONDS,
        poll_interval=HEAL_WAIT_INTERVAL_SECONDS):
    """Wait until one server reports every pool brick connected and clean."""
    deadline = time.monotonic() + timeout_seconds
    last_error = "no server accepted the heal query"
    while True:
        for server_name in server_names:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                stdout, _stderr, _pid = lib_execute(
                    KUBECTL_CMD,
                    "-n",
                    NAMESPACE,
                    "exec",
                    server_name,
                    "-c",
                    "server",
                    "--",
                    "/opt/libexec/glusterfs/glfsheal",
                    volname,
                    "info-summary",
                    "volfile-path",
                    f"/var/lib/kadalu/volfiles/{volname}.vol",
                    timeout=max(1, remaining),
                )
            except CommandException as err:
                last_error = str(err)
                continue
            if storage_heal_summary_is_clean(stdout, len(server_names)):
                logging.info(logf(
                    "Storage heal gate is clean",
                    volname=volname,
                    server=server_name,
                ))
                return
            last_error = f"unclean or incomplete summary from {server_name}"

        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for storage pool {volname} to report "
                f"every brick Connected with zero heal entries: {last_error}. "
                "Further brick StatefulSet rollouts were not attempted."
            )
        time.sleep(poll_interval)


def existing_server_statefulset_names(apps_v1_client, server_names):
    """Return desired server StatefulSets which already exist."""
    existing = set()
    for name in server_names:
        try:
            apps_v1_client.read_namespaced_stateful_set(name, NAMESPACE)
        except Exception as err:  # pylint: disable=broad-exception-caught
            if getattr(err, "status", None) not in (404, "404"):
                raise
            continue
        existing.add(name)
    return existing


def unavailable_server_statefulset_names(apps_v1_client, server_names):
    """Return existing servers which are not currently serving one replica."""
    unavailable = set()
    for name in server_names:
        stateful_set = apps_v1_client.read_namespaced_stateful_set(
            name,
            NAMESPACE,
        )
        if not server_statefulset_rollout_complete(stateful_set):
            unavailable.add(name)
    return unavailable


def preflight_existing_storage_heal(apps_v1_client, storage_plan):
    """Reject an unclean fully-serving pool before fencing provisioning."""
    for obj in storage_plan["upgrade_objects"]:
        if obj["spec"]["type"] not in HEAL_GATED_VOLUME_TYPES:
            continue
        volname = obj["metadata"]["name"]
        statefulset_names = [
            get_brick_hostname(volname, index, suffix=False)
            for index, _brick in enumerate(obj["spec"]["storage"])
        ]
        existing = existing_server_statefulset_names(
            apps_v1_client,
            statefulset_names,
        )
        if existing != set(statefulset_names):
            continue
        if unavailable_server_statefulset_names(
                apps_v1_client, statefulset_names):
            continue
        wait_for_storage_heal(
            volname,
            [f"{name}-0" for name in statefulset_names],
        )


def deploy_storage_service(volname):
    """Ensure peer DNS exists before any server starts or heal is queried."""
    filename = os.path.join(MANIFESTS_DIR, "services.yaml")
    template(filename, namespace=NAMESPACE, volname=volname)
    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
    logging.info(logf(
        "Deployed Service",
        volname=volname,
        manifest=filename,
    ))


def deploy_server_pods(obj, apps_v1_client=None):
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
    if voltype in HEAL_GATED_VOLUME_TYPES and apps_v1_client is None:
        raise RuntimeError(
            f"Storage pool {volname} requires the Kubernetes Apps client "
            "for heal-gated server reconciliation"
        )
    deploy_storage_service(volname)

    shd_required = voltype in HEAL_GATED_VOLUME_TYPES

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

    server_templates = []
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

        server_templates.append((storage, dict(template_args)))

    heal_gated = (
        apps_v1_client is not None
        and voltype in HEAL_GATED_VOLUME_TYPES
    )
    statefulset_names = [
        values["serverpod_name"] for _storage, values in server_templates
    ]
    server_pod_names = [f"{name}-0" for name in statefulset_names]
    existing_names = set()
    if heal_gated:
        existing_names = existing_server_statefulset_names(
            apps_v1_client,
            statefulset_names,
        )

    missing_names = set(statefulset_names) - existing_names
    unavailable_names = set()
    if heal_gated and existing_names:
        unavailable_names = unavailable_server_statefulset_names(
            apps_v1_client,
            sorted(existing_names),
        )
    recovery_names = missing_names | unavailable_names
    recovery_templates = [
        item for item in server_templates
        if item[1]["serverpod_name"] in recovery_names
    ]
    healthy_templates = [
        item for item in server_templates
        if item[1]["serverpod_name"] not in recovery_names
    ]
    if not heal_gated:
        recovery_templates = server_templates
        healthy_templates = []
    elif not recovery_templates:
        # A rollout must never begin while the previous rollout or an earlier
        # outage still has pending self-heal work.
        wait_for_storage_heal(volname, server_pod_names)

    for storage, values in recovery_templates + healthy_templates:
        filename = os.path.join(MANIFESTS_DIR, "server.yaml")
        template(filename, **values)
        lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
        if apps_v1_client is not None:
            wait_for_server_statefulset_rollout(
                apps_v1_client,
                values["serverpod_name"],
            )
        logging.info(logf("Deployed Server pod",
                          volname=volname,
                          manifest=filename,
                          node=storage.get("node", "")))
        if heal_gated:
            is_last_recovery = (
                recovery_templates
                and values["serverpod_name"]
                == recovery_templates[-1][1]["serverpod_name"]
            )
            if (
                    values["serverpod_name"] not in recovery_names
                    or is_last_recovery):
                # Missing/unavailable members are recovery. Once all expected
                # members serve again, and after every healthy member is
                # applied, require a clean heal before touching another.
                wait_for_storage_heal(volname, server_pod_names)


def handle_external_storage_addition(
        core_v1_client, obj, storage_api=None,
        recovered_mount_identity=None):
    """Deploy service(One service per Volume)"""
    volname = obj["metadata"]["name"]
    details = obj["spec"]["details"]
    pv_reclaim_policy = obj["spec"].get("pvReclaimPolicy", "delete")
    hosts = []
    ghost = details.get("gluster_host", None)
    ghosts = details.get("gluster_hosts", None)
    if ghost:
        hosts.append(ghost)
    if ghosts:
        hosts.extend(ghosts)

    data = {
        "volname": volname,
        "storageClassName": storage_class_name(obj),
        "volume_id": obj["spec"]["volume_id"],
        "type": VOLUME_TYPE_EXTERNAL,
        "pvReclaimPolicy": pv_reclaim_policy,
        # CRD would set 'native' but just being cautious
        "single_pv_per_pool": get_single_pv_per_pool(obj["spec"]),
        "gluster_hosts": ",".join(_canonical_gluster_hosts(hosts)),
        "gluster_volname": details["gluster_volname"],
        "gluster_options": details.get("gluster_options", ""),
    }
    if "tolerations" in obj["spec"]:
        data["tolerations"] = obj["spec"]["tolerations"]
    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)
    volinfo_file = "%s.info" % volname
    existing_serialized = configmap_data.data.get(volinfo_file)
    existing_record = None
    if existing_serialized is not None:
        existing_record = _decode_pool_record(existing_serialized)
        if existing_record is None:
            return False
    storage_uid = (obj.get("metadata") or {}).get("uid")
    if storage_uid:
        data["storage_uid"] = storage_uid
    elif existing_record is not None:
        if existing_record.get("storage_uid"):
            data["storage_uid"] = existing_record["storage_uid"]
    if storage_api is None:
        storage_api = client.StorageV1Api()

    if existing_serialized is None:
        installed = existing_storage_class(
            storage_api,
            data["storageClassName"],
        )
        durable_identity = (
            recover_storage_class_identity(installed, obj)
            if installed is not None
            else None
        )
        retained_pvs = list(_pool_persistent_volumes(
            core_v1_client,
            volname,
            data["storageClassName"],
        ))
        if retained_pvs and durable_identity is None:
            logging.error(logf(
                "Refusing to reconstruct retained External storage without "
                "durable identity evidence",
                volname=volname,
            ))
            return False
        if durable_identity is not None and (
                durable_identity["volume_id"] != data["volume_id"]
                or durable_identity["mount_identity"]
                != recovered_mount_identity):
            logging.error(logf(
                "External storage identity changed during recovery",
                volname=volname,
            ))
            return False
        if recovered_mount_identity is not None and durable_identity is None:
            logging.error(logf(
                "External storage recovery evidence disappeared",
                volname=volname,
            ))
            return False
    else:
        storage_class_source, class_mount_identity = (
            preflight_existing_pool_storage_class(
                storage_api,
                obj,
                existing_record,
            )
        )
        preflight_storage_class_owner(
            storage_api,
            storage_class_source,
        )
        if class_mount_identity is not None:
            if (
                    recovered_mount_identity is not None
                    and recovered_mount_identity != class_mount_identity):
                logging.error(logf(
                    "External storage mount identity changed during "
                    "recovery",
                    volname=volname,
                ))
                return False
            recovered_mount_identity = class_mount_identity

    if not apply_pool_mount_metadata_for_write(
        core_v1_client,
        data,
        existing_serialized,
        recovered_mount_identity,
    ):
        return False
    storage_class_owner = preflight_storage_class_owner(storage_api, data)
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=True,
        class_name=data["storageClassName"],
    )
    configmap_data.data[volinfo_file] = json.dumps(data)

    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=False,
        class_name=data["storageClassName"],
    )
    logging.info(logf("Updated configmap", name=KADALU_CONFIG_MAP,
                      volname=volname))
    filename = os.path.join(MANIFESTS_DIR, "external-storageclass.yaml")
    template(
        filename,
        **data,
        namespace=NAMESPACE,
        storage_class_name=data["storageClassName"],
        backend_fingerprint=storage_class_owner["backend_fingerprint"],
        reclaim_policy=storage_class_reclaim_policy(pv_reclaim_policy),
    )
    reconcile_storage_class_manifest(
        storage_api,
        filename,
        data["storageClassName"],
        storage_class_reclaim_policy(pv_reclaim_policy),
        storage_class_owner,
    )
    # Close the provisioning race around immutable StorageClass replacement.
    # The directional pass above protects data; this pass catches a PV created
    # from the old class while reconciliation was in progress.
    reconcile_pool_pv_reclaim_policy(
        core_v1_client,
        volname,
        pv_reclaim_policy,
        before_config_write=None,
        class_name=data["storageClassName"],
    )
    logging.info(logf("Deployed External StorageClass", volname=volname, manifest=filename))
    return True


def recover_missing_pool_identity(
        core_v1_client, obj, pods, apps_v1_client, storage_api):
    """Resolve a missing record only from complete retained evidence."""
    volname = obj["metadata"]["name"]
    voltype = obj["spec"]["type"]
    requested_volume_id = obj["spec"].get("volume_id")
    retained_pvs = list(_pool_persistent_volumes(
        core_v1_client,
        volname,
        storage_class_name(obj),
    ))
    preflight_storage_class_owner(storage_api, obj)
    installed = existing_storage_class(
        storage_api,
        storage_class_name(obj),
    )
    durable_identity = (
        recover_storage_class_identity(installed, obj)
        if installed is not None
        else None
    )

    server_volume_id = None
    if voltype != VOLUME_TYPE_EXTERNAL:
        server_volume_id = recover_native_pool_volume_id(
            obj,
            pods,
            apps_v1_client,
        )
        if (
                server_volume_id is not None
                and not storage_class_proves_current_cr(installed, obj)):
            raise ValueError(
                "live native servers do not prove ownership by the current "
                "Kadalustorage"
            )
        complex_geometry = (
            voltype == VOLUME_TYPE_DISPERSE
            or (
                voltype == VOLUME_TYPE_REPLICA_2
                and bool(obj["spec"].get("tiebreaker"))
            )
        )
        if (
                server_volume_id is not None
                and complex_geometry
                and durable_identity is None):
            raise ValueError(
                "live servers do not prove the requested complex native "
                "geometry"
            )
        if installed is not None and server_volume_id is None:
            raise ValueError(
                "installed native StorageClass has no complete live "
                "server proof"
            )
        if retained_pvs and (
                server_volume_id is None or durable_identity is None):
            raise ValueError(
                "retained native PVs have no complete server and mount "
                "identity proof"
            )
        if (
                server_volume_id is not None
                and durable_identity is not None
                and server_volume_id != durable_identity["volume_id"]):
            raise ValueError(
                "server workloads and StorageClass disagree on VOLUME_ID"
            )
    elif installed is not None and durable_identity is None:
        raise ValueError(
            "installed External StorageClass has no durable identity"
        )
    elif retained_pvs and durable_identity is None:
        raise ValueError(
            "retained External PVs have no durable StorageClass identity"
        )

    recovered_volume_id = server_volume_id
    if recovered_volume_id is None and durable_identity is not None:
        recovered_volume_id = durable_identity["volume_id"]
    if (
            recovered_volume_id is not None
            and requested_volume_id is not None
            and recovered_volume_id != requested_volume_id):
        raise ValueError(
            "requested volume_id disagrees with retained storage evidence"
        )
    volume_id = (
        recovered_volume_id
        or requested_volume_id
        or str(uuid.uuid4())
    )
    mount_identity = (
        durable_identity["mount_identity"]
        if durable_identity is not None
        else None
    )
    return volume_id, mount_identity


_NO_RUNTIME_RECLAIM_POLICY_TRANSITION = object()


def _effective_reclaim_policy(source):
    """Return the Kubernetes reclaim policy represented by a CR or record."""
    return storage_class_reclaim_policy(_configured_reclaim_policy(source))


def _configured_reclaim_policy(source):
    """Return the exact validated Kadalu reclaim behavior."""
    spec = source.get("spec")
    values = spec if isinstance(spec, dict) else source
    return values.get("pvReclaimPolicy", "delete")


def read_current_storage_resource(storage_client, expected):
    """Read the active CR incarnation used by a fenced runtime transition."""
    metadata = expected.get("metadata") or {}
    name = metadata.get("name")
    expected_uid = metadata.get("uid")
    if storage_client is None:
        raise RuntimeError(
            f"Kadalustorage {name or '<unknown>'} cannot be re-read under "
            "the CSI provisioner fence"
        )
    if not name or not expected_uid:
        raise RuntimeError(
            "A fenced reclaim policy transition requires Kadalustorage "
            "name and UID"
        )
    try:
        current = storage_client.get_namespaced_custom_object(
            "kadalu-operator.storage",
            "v1alpha1",
            NAMESPACE,
            "kadalustorages",
            name,
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        if getattr(err, "status", None) in (404, "404"):
            raise RuntimeError(
                f"Kadalustorage {name} no longer exists under the CSI "
                "provisioner fence"
            ) from err
        raise

    current_metadata = (
        current.get("metadata") if isinstance(current, dict) else None
    ) or {}
    if (
            current_metadata.get("name") != name
            or current_metadata.get("uid") != expected_uid):
        raise RuntimeError(
            f"Kadalustorage {name} changed identity under the CSI "
            "provisioner fence"
        )
    if current_metadata.get("deletionTimestamp") is not None:
        raise RuntimeError(
            f"Kadalustorage {name} is being deleted under the CSI "
            "provisioner fence"
        )
    if not validate_volume_request(current):
        raise RuntimeError(
            f"Kadalustorage {name} became invalid under the CSI "
            "provisioner fence"
        )
    return current


def verify_runtime_reclaim_policy_transition(
        core_v1_client, obj, apps_v1_client=None, storage_api=None):
    """Require the ConfigMap, StorageClass, and every owned PV to agree."""
    volname = obj["metadata"]["name"]
    configured_policy = _configured_reclaim_policy(obj)
    target_policy = _effective_reclaim_policy(obj)
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    serialized = (configmap_data.data or {}).get(f"{volname}.info")
    record = _decode_pool_record(serialized)
    if record is None:
        raise RuntimeError(
            f"Storage metadata for {volname} disappeared during reclaim "
            "policy transition"
        )
    _validate_runtime_storage_record(
        obj,
        record,
        core_v1_client,
        apps_v1_client,
    )
    if _configured_reclaim_policy(record) != configured_policy:
        raise RuntimeError(
            f"Storage metadata for {volname} has an inconsistent reclaim "
            "behavior"
        )

    if storage_api is None:
        storage_api = client.StorageV1Api()
    storage_class = existing_storage_class(
        storage_api,
        storage_class_name(record),
    )
    if storage_class is None:
        raise RuntimeError(
            f"StorageClass {storage_class_name(record)} is absent "
            "after reclaim policy transition"
        )
    validate_storage_class_owner(
        storage_class,
        storage_class_identity(record),
        allow_legacy=False,
    )
    storage_class_policy = _k8s_value(
        storage_class,
        "reclaimPolicy",
        "reclaim_policy",
    ) or "Delete"
    if storage_class_policy != target_policy:
        raise RuntimeError(
            f"StorageClass {storage_class_name(record)} has an "
            "inconsistent reclaim policy"
        )

    inconsistent_pvs = []
    for persistent_volume in _pool_persistent_volumes(
            core_v1_client, volname, storage_class_name(record)):
        if not _pool_persistent_volume_is_mutable(
                persistent_volume, volname):
            continue
        current_policy = getattr(
            persistent_volume.spec,
            "persistent_volume_reclaim_policy",
            None,
        ) or "Delete"
        if current_policy != target_policy:
            inconsistent_pvs.append(persistent_volume.metadata.name)
    if inconsistent_pvs:
        raise RuntimeError(
            f"PersistentVolumes for {volname} have an inconsistent reclaim "
            f"policy: {', '.join(sorted(inconsistent_pvs))}"
        )
    return True


def reconcile_runtime_reclaim_policy_transition(
        core_v1_client, obj, existing, context):
    """Run one changed runtime policy under a provisioner fence we own."""
    apps_v1_client = context["apps_v1_client"]
    storage_api = context["storage_api"]
    storage_client = context["storage_client"]
    handler = context["handler"]
    provisioner_fenced = context["provisioner_fenced"]
    if (
            provisioner_fenced
            or _configured_reclaim_policy(existing)
            == _configured_reclaim_policy(obj)):
        return _NO_RUNTIME_RECLAIM_POLICY_TRANSITION
    volname = obj["metadata"]["name"]
    if apps_v1_client is None:
        logging.error(logf(
            "Reclaim policy transition requires a CSI provisioner fence",
            storage=volname,
        ))
        return False

    # Readiness must fall before the serving controller is stopped. A failed
    # transition deliberately leaves both surfaces down for supervised retry.
    clear_operator_ready()
    quiesce_csi_provisioner(core_v1_client, apps_v1_client)
    try:
        candidate = read_current_storage_resource(storage_client, obj)
        while True:
            candidate_fingerprint = _storage_object_fingerprint(candidate)
            reconciled = handler(
                core_v1_client,
                deepcopy(candidate),
                apps_v1_client=apps_v1_client,
                storage_api=storage_api,
                provisioner_fenced=True,
                storage_client=storage_client,
            )
            if not reconciled:
                logging.error(logf(
                    "Reclaim policy transition failed under CSI fence",
                    storage=volname,
                ))
                return False
            verify_runtime_reclaim_policy_transition(
                core_v1_client,
                candidate,
                apps_v1_client,
                storage_api,
            )
            current = read_current_storage_resource(storage_client, obj)
            if _storage_object_fingerprint(current) == candidate_fingerprint:
                break
            logging.info(logf(
                "Kadalustorage changed during fenced reclaim transition; "
                "reconciling the current desired state",
                storage=volname,
            ))
            candidate = current
    except Exception:  # pylint: disable=broad-exception-caught
        logging.exception(logf(
            "Reclaim policy transition left CSI provisioner fenced",
            storage=volname,
        ))
        raise

    try:
        resume_csi_provisioner(apps_v1_client)
        wait_for_csi_provisioner_rollout(apps_v1_client)
        mark_operator_ready()
    except Exception:  # pylint: disable=broad-exception-caught
        # A serving controller without operator readiness cannot be treated as
        # a completed handoff. Put it back behind the same fence before retry.
        clear_operator_ready()
        quiesce_csi_provisioner(core_v1_client, apps_v1_client)
        raise
    return True


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def handle_added(
        core_v1_client, obj, apps_v1_client=None, storage_api=None,
        provisioner_fenced=False, storage_client=None):
    """
    New Volume is requested. Update the configMap and deploy
    """

    if not validate_volume_request(obj):
        # TODO: Delete Custom resource
        logging.debug(logf(
            "validation of volume request failed",
            yaml=obj
        ))
        return False

    volname = obj["metadata"]["name"]
    voltype = obj["spec"]["type"]
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
            return False
        try:
            _validate_runtime_storage_record(
                obj,
                existing,
                core_v1_client,
                apps_v1_client,
            )
        except RuntimeError as err:
            logging.error(logf(
                "Rejected storage pool ownership change",
                volname=volname,
                error=err,
            ))
            return False
        requested_mode = get_single_pv_per_pool(obj["spec"])
        if requested_mode != get_single_pv_per_pool(existing):
            logging.error(logf(
                "Rejected immutable single_pv_per_pool change",
                volname=volname,
                existing=get_single_pv_per_pool(existing),
                requested=requested_mode,
            ))
            return False

    existing_volume_id = existing.get("volume_id") if existing else None
    requested_volume_id = obj["spec"].get("volume_id")
    recovered_mount_identity = None
    if existing is not None:
        if not existing_volume_id:
            logging.error(logf(
                "Existing storage pool has no volume identity",
                volname=volname,
            ))
            return False
        if (
                requested_volume_id is not None
                and requested_volume_id != existing_volume_id):
            logging.error(logf(
                "Rejected immutable storage pool volume identity change",
                volname=volname,
                existing=existing_volume_id,
                requested=requested_volume_id,
            ))
            return False
        obj["spec"]["volume_id"] = existing_volume_id
        logging.info(logf(
            "Reusing existing volume id",
            volume_id=existing_volume_id,
        ))
    else:
        if storage_api is None:
            storage_api = client.StorageV1Api()
        try:
            volume_id, recovered_mount_identity = (
                recover_missing_pool_identity(
                    core_v1_client,
                    obj,
                    _k8s_value(pods, "items", default=[]) or [],
                    apps_v1_client,
                    storage_api,
                )
            )
        except (RuntimeError, ValueError) as err:
            logging.error(logf(
                "Refusing to reconstruct storage pool identity",
                volname=volname,
                error=err,
            ))
            return False
        obj["spec"]["volume_id"] = volume_id
        logging.info(logf(
            "Resolved storage pool volume id",
            volume_id=volume_id,
        ))

    if voltype == VOLUME_TYPE_EXTERNAL:
        if existing is not None:
            if storage_api is None:
                storage_api = client.StorageV1Api()
            storage_class_source, recovered_mount_identity = (
                preflight_existing_pool_storage_class(
                    storage_api,
                    obj,
                    existing,
                )
            )
            preflight_storage_class_owner(
                storage_api,
                storage_class_source,
            )
            transition_result = reconcile_runtime_reclaim_policy_transition(
                core_v1_client,
                obj,
                existing,
                {
                    "apps_v1_client": apps_v1_client,
                    "storage_api": storage_api,
                    "storage_client": storage_client,
                    "handler": handle_added,
                    "provisioner_fenced": provisioner_fenced,
                },
            )
            if (
                    transition_result
                    is not _NO_RUNTIME_RECLAIM_POLICY_TRANSITION):
                return transition_result
        if recovered_mount_identity is None:
            return handle_external_storage_addition(
                core_v1_client,
                obj,
                storage_api,
            )
        return handle_external_storage_addition(
            core_v1_client,
            obj,
            storage_api,
            recovered_mount_identity,
        )

    # Generate Node ID for each storage device.
    for idx, _ in enumerate(obj["spec"]["storage"]):
        obj["spec"]["storage"][idx]["node_id"] = "node-%d" % idx

    if storage_api is None:
        storage_api = client.StorageV1Api()
    storage_class_source = existing or obj
    if existing is not None:
        storage_class_source, recovered_mount_identity = (
            preflight_existing_pool_storage_class(
                storage_api,
                obj,
                existing,
            )
        )
    elif recovered_mount_identity is not None:
        storage_class_source = {
            "metadata": obj["metadata"],
            "spec": {
                **obj["spec"],
                "mount_identity": recovered_mount_identity,
            },
        }
    preflight_storage_class_owner(storage_api, storage_class_source)
    if existing is not None:
        transition_result = reconcile_runtime_reclaim_policy_transition(
            core_v1_client,
            obj,
            existing,
            {
                "apps_v1_client": apps_v1_client,
                "storage_api": storage_api,
                "storage_client": storage_client,
                "handler": handle_added,
                "provisioner_fenced": provisioner_fenced,
            },
        )
        if transition_result is not _NO_RUNTIME_RECLAIM_POLICY_TRANSITION:
            return transition_result
    if not update_config_map(
            core_v1_client,
            obj,
            recovered_mount_identity,
    ):
        return False
    deploy_storage_class(obj, core_v1_client, storage_api)

    # Applying an unchanged StatefulSet is non-disruptive, while always
    # reconciling it ensures storage placement, devices, tolerations, and other
    # CR changes made while the operator was down reach existing servers.
    deploy_server_pods(obj, apps_v1_client)

    return True


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def handle_modified(
        core_v1_client, obj, apps_v1_client=None, storage_api=None,
        provisioner_fenced=False, storage_client=None):
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
            return False

        configmap_data = core_v1_client.read_namespaced_config_map(
            KADALU_CONFIG_MAP, NAMESPACE)
        existing_serialized = configmap_data.data.get(f"{volname}.info")
        if existing_serialized is None:
            logging.warning(logf(
                "Volume config not found",
                storagename=volname
            ))
            return handle_added(
                core_v1_client,
                obj,
                apps_v1_client,
                storage_api,
                provisioner_fenced,
                storage_client,
            )

        existing = _decode_pool_record(existing_serialized)
        if existing is None:
            return False
        try:
            _validate_runtime_storage_record(
                obj,
                existing,
                core_v1_client,
                apps_v1_client,
            )
        except RuntimeError as err:
            logging.error(logf(
                "Rejected storage pool ownership change",
                volname=volname,
                error=err,
            ))
            return False
        existing_volume_id = existing.get("volume_id")
        if not existing_volume_id:
            logging.error(logf(
                "Existing storage pool has no volume identity",
                volname=volname,
            ))
            return False
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
            return False

        obj["spec"]["volume_id"] = existing_volume_id
        if storage_api is None:
            storage_api = client.StorageV1Api()
        storage_class_source, _recovered_mount_identity = (
            preflight_existing_pool_storage_class(
                storage_api,
                obj,
                existing,
            )
        )
        preflight_storage_class_owner(storage_api, storage_class_source)
        transition_result = reconcile_runtime_reclaim_policy_transition(
            core_v1_client,
            obj,
            existing,
            {
                "apps_v1_client": apps_v1_client,
                "storage_api": storage_api,
                "storage_client": storage_client,
                "handler": handle_modified,
                "provisioner_fenced": provisioner_fenced,
            },
        )
        if transition_result is not _NO_RUNTIME_RECLAIM_POLICY_TRANSITION:
            return transition_result
        return handle_external_storage_addition(
            core_v1_client,
            obj,
            storage_api,
        )

    if not validate_volume_request(obj):
        logging.debug(logf(
            "validation of volume request failed",
            yaml=obj
        ))
        return False

    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)

    if not configmap_data.data.get("%s.info" % volname, None):
        logging.warning(logf(
            "Volume config not found",
            storagename=volname
        ))
        # Volume doesn't exist yet, so create it
        return handle_added(
            core_v1_client,
            obj,
            apps_v1_client,
            storage_api,
            provisioner_fenced,
            storage_client,
        )

    # Volume ID (uuid) is already generated, re-use
    cfgmap = _decode_pool_record(configmap_data.data[volname + ".info"])
    if cfgmap is None:
        return False
    try:
        _validate_runtime_storage_record(
            obj,
            cfgmap,
            core_v1_client,
            apps_v1_client,
        )
    except RuntimeError as err:
        logging.error(logf(
            "Rejected storage pool ownership change",
            volname=volname,
            error=err,
        ))
        return False
    # Get volume-id from config map
    obj["spec"]["volume_id"] = cfgmap["volume_id"]

    # Set Node ID for each storage device from configmap
    for idx, _ in enumerate(obj["spec"]["storage"]):
        obj["spec"]["storage"][idx]["node_id"] = "node-%d" % idx

    if storage_api is None:
        storage_api = client.StorageV1Api()
    storage_class_source, recovered_mount_identity = (
        preflight_existing_pool_storage_class(
            storage_api,
            obj,
            cfgmap,
        )
    )
    preflight_storage_class_owner(storage_api, storage_class_source)
    transition_result = reconcile_runtime_reclaim_policy_transition(
        core_v1_client,
        obj,
        cfgmap,
        {
            "apps_v1_client": apps_v1_client,
            "storage_api": storage_api,
            "storage_client": storage_client,
            "handler": handle_modified,
            "provisioner_fenced": provisioner_fenced,
        },
    )
    if transition_result is not _NO_RUNTIME_RECLAIM_POLICY_TRANSITION:
        return transition_result
    # Add new entry in the existing config map
    if not update_config_map(
            core_v1_client,
            obj,
            recovered_mount_identity,
    ):
        return False
    deploy_storage_class(obj, core_v1_client, storage_api)
    deploy_server_pods(obj, apps_v1_client)

    return True


def _read_deletion_storage_info(core_v1_client, volname):
    """Read deletion metadata while distinguishing absence from API failure."""
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    serialized = (configmap_data.data or {}).get(f"{volname}.info")
    if serialized is None:
        return None
    storage_info_data = _decode_pool_record(serialized)
    if storage_info_data is None:
        raise RuntimeError(
            f"Storage metadata for {volname} is invalid"
        )
    return storage_info_data


def _verify_legacy_deletion_resource(
        core_v1_client, apps_v1_client, storage_client, obj, record):
    """Recheck the CR absence or deleting incarnation under the CSI fence."""
    metadata = obj.get("metadata") or {}
    volname = metadata.get("name")
    event_uid = metadata.get("uid")
    if storage_client is None:
        raise RuntimeError(
            f"UID-less storage pool {volname} cannot be sealed without an "
            "authoritative Kadalustorage client"
        )
    try:
        current = storage_client.get_namespaced_custom_object(
            "kadalu-operator.storage",
            "v1alpha1",
            NAMESPACE,
            "kadalustorages",
            volname,
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        if getattr(err, "status", None) not in (404, "404"):
            raise
        if event_uid:
            raise RuntimeError(
                f"Deleting Kadalustorage {volname} disappeared before its "
                "legacy record could be sealed"
            ) from err
        return

    current_metadata = (
        current.get("metadata") if isinstance(current, dict) else None
    ) or {}
    if not event_uid:
        logging.warning(logf(
            "A same-name Kadalustorage appeared while sealing a legacy "
            "orphan and will remain quarantined",
            storage=volname,
            observed_uid=current_metadata.get("uid"),
        ))
        return
    if current_metadata.get("name") != volname:
        raise RuntimeError(
            f"Legacy deletion lookup for {volname} returned another object"
        )
    if current_metadata.get("uid") != event_uid:
        logging.warning(logf(
            "A replacement Kadalustorage appeared while sealing a legacy "
            "deletion and will remain quarantined",
            storage=volname,
            deleting_uid=event_uid,
            replacement_uid=current_metadata.get("uid"),
        ))
        return
    if current_metadata.get("deletionTimestamp") is None:
        raise RuntimeError(
            f"Kadalustorage {volname} is no longer deleting while its "
            "legacy record is being sealed"
        )
    _validate_storage_record_owner(
        current,
        record,
        core_v1_client,
        apps_v1_client,
    )


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def seal_legacy_deletion_record(
        core_v1_client, apps_v1_client, storage_client, obj,
        expected_record, legacy_storage_uid=None,
        legacy_quarantine=False, storage_api=None):
    """CAS-tombstone a UID-less orphan and claim it only with exact proof."""
    volname = (obj.get("metadata") or {}).get("name", "<unknown>")
    if expected_record.get("storage_uid"):
        raise RuntimeError(
            f"Storage pool {volname} is not a UID-less legacy record"
        )
    _verify_legacy_deletion_resource(
        core_v1_client,
        apps_v1_client,
        storage_client,
        obj,
        expected_record,
    )

    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    metadata = getattr(configmap_data, "metadata", None)
    resource_version = _k8s_value(
        metadata,
        "resourceVersion",
        "resource_version",
    )
    if not resource_version:
        raise RuntimeError(
            f"Storage metadata for {volname} has no resourceVersion for "
            "legacy orphan sealing"
        )
    key = f"{volname}.info"
    current = _decode_pool_record((configmap_data.data or {}).get(key))
    if current is None:
        raise RuntimeError(
            f"Storage metadata for {volname} disappeared before legacy "
            "orphan sealing"
        )
    _validate_stored_pool_identity(volname, current)
    if current.get("type") != VOLUME_TYPE_EXTERNAL:
        _validate_stored_bricks(volname, current)
    if current != expected_record:
        raise RuntimeError(
            f"Storage pool {volname} changed before legacy orphan sealing"
        )

    pv_count = get_num_pvs(core_v1_client, current)
    if pv_count == -1:
        raise RuntimeError(
            f"PersistentVolumes for legacy storage pool {volname} could "
            "not be verified"
        )

    cleanup_owned = not legacy_quarantine
    if cleanup_owned:
        event_uid = (obj.get("metadata") or {}).get("uid")
        expected_uid = event_uid or (
            f"{CLEANUP_ONLY_STORAGE_UID_PREFIX}{current['volume_id']}"
        )
        if legacy_storage_uid != expected_uid:
            raise RuntimeError(
                f"Storage pool {volname} has an invalid legacy deletion "
                "owner"
            )
        if storage_api is None:
            storage_api = client.StorageV1Api()
        try:
            preflight_legacy_orphan_storage_class(storage_api, current)
        except RuntimeError as err:
            cleanup_owned = False
            logging.error(logf(
                "Quarantined UID-less deletion orphan",
                storage=volname,
                error=err,
            ))

    updated = deepcopy(current)
    updated["provisioning_disabled"] = True
    if cleanup_owned:
        updated["storage_uid"] = legacy_storage_uid
        updated[CLEANUP_ONLY_RECORD_FIELD] = True
        updated.pop(DELETION_QUARANTINE_RECORD_FIELD, None)
    else:
        updated[DELETION_QUARANTINE_RECORD_FIELD] = True
    if updated != current:
        replacement = deepcopy(configmap_data)
        replacement.data[key] = json.dumps(updated)
        core_v1_client.replace_namespaced_config_map(
            KADALU_CONFIG_MAP,
            NAMESPACE,
            replacement,
        )

    confirmed = _read_deletion_storage_info(core_v1_client, volname)
    if confirmed != updated:
        raise RuntimeError(
            f"Storage pool {volname} changed while sealing its legacy "
            "deletion record"
        )
    logging.warning(logf(
        (
            "Sealed UID-less deletion orphan"
            if cleanup_owned
            else "Tombstoned unproven UID-less deletion orphan"
        ),
        storage=volname,
        number_of_pvs=pv_count,
    ))
    return confirmed, cleanup_owned


def _deletion_record_identity(data):
    """Return durable fields which must not change during pool deletion."""
    return {
        "volname": data.get("volname"),
        "type": data.get("type"),
        "storageClassName": storage_class_name(data),
        "storage_uid": data.get("storage_uid"),
        CLEANUP_ONLY_RECORD_FIELD: data.get(CLEANUP_ONLY_RECORD_FIELD),
        DELETION_QUARANTINE_RECORD_FIELD: data.get(
            DELETION_QUARANTINE_RECORD_FIELD
        ),
        "volume_id": data.get("volume_id"),
        "mount_identity": data.get("mount_identity"),
        "mount_config_fingerprint": data.get("mount_config_fingerprint"),
        "single_pv_per_pool": get_single_pv_per_pool(data),
        "backend_fingerprint": storage_class_backend_fingerprint(data),
    }


def _validate_deletion_record_owner(obj, data):
    """Require a durable record owner and reject stale deletion events."""
    metadata = obj.get("metadata") or {}
    event_uid = metadata.get("uid")
    stored_uid = data.get("storage_uid")
    if not stored_uid:
        raise RuntimeError(
            f"Storage pool {metadata.get('name')} has no deletion owner proof"
        )
    if event_uid and event_uid != stored_uid:
        raise RuntimeError(
            f"Kadalustorage {metadata.get('name')} does not own the pool "
            "being deleted"
        )


def _validate_deletion_snapshot(obj, expected, current):
    """Reject record replacement between deletion preflight and mutation."""
    volname = (obj.get("metadata") or {}).get("name", "<unknown>")
    _validate_stored_pool_identity(volname, current)
    if current.get("type") != VOLUME_TYPE_EXTERNAL:
        _validate_stored_bricks(volname, current)
    _validate_deletion_record_owner(obj, current)
    if _deletion_record_identity(current) != _deletion_record_identity(
            expected):
        raise RuntimeError(
            f"Storage pool {volname} changed during deletion"
        )


def disable_pool_provisioning(
        core_v1_client, volname, expected_storage_info=None):
    """Persist a pool-local admission tombstone while CSI is fenced."""
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    key = f"{volname}.info"
    serialized = (configmap_data.data or {}).get(key)
    record = _decode_pool_record(serialized)
    if record is None:
        raise RuntimeError(
            f"Storage metadata for {volname} disappeared during deletion"
        )
    _validate_stored_pool_identity(volname, record)
    if expected_storage_info is not None and (
            _deletion_record_identity(record)
            != _deletion_record_identity(expected_storage_info)):
        raise RuntimeError(
            f"Storage pool {volname} changed before its deletion tombstone"
        )
    if (
            record.get("provisioning_disabled") is True
            and record.get(CLEANUP_ONLY_RECORD_FIELD) is True):
        return record
    record["provisioning_disabled"] = True
    record[CLEANUP_ONLY_RECORD_FIELD] = True
    configmap_data.data[key] = json.dumps(record)
    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
        configmap_data,
    )
    logging.info(logf(
        "Disabled new volume provisioning from deleting pool",
        storage=volname,
    ))
    return record


# pylint: disable-next=too-many-arguments,too-many-positional-arguments
def handle_deleted(
        core_v1_client, obj, storage_info_data=None, apps_v1_client=None,
        provisioner_fenced=False, storage_client=None,
        legacy_storage_uid=None, legacy_quarantine=False,
        cleanup_quarantine=False):
    """
    If number of pvs provisioned from that volume
    is zero - Delete the respective server pods
    If number of pvs is not zero, wait or periodically
    check for num_pvs. Delete Server pods only when pvs becomes zero.
    """

    volname = obj["metadata"]["name"]

    if storage_info_data is None and core_v1_client is not None:
        storage_info_data = _read_deletion_storage_info(
            core_v1_client,
            volname,
        )
        if storage_info_data is None:
            # The class name is derivable even after ownership metadata has
            # already been removed by an interrupted cleanup. The deleted CR
            # UID must still match so an old event cannot delete a replacement
            # pool's same-name cluster-scoped class.
            try:
                delete_storage_class(
                    volname,
                    None,
                    expected_storage_uid=(obj.get("metadata") or {}).get(
                        "uid"
                    ),
                    storage_class_source=obj,
                )
            except RuntimeError as err:
                logging.error(logf(
                    "Refusing unowned interrupted storage cleanup",
                    storage=volname,
                    error=err,
                ))
                return False
            logging.info(logf(
                "Storage deletion is already complete",
                storage=volname,
            ))
            return True
    if storage_info_data is None:
        storage_info_data = get_configmap_data(volname)

    logging.info(logf("Delete requested", volname=volname))

    if storage_info_data is None:
        logging.error(logf(
            "Storage delete failed. Storage metadata is unavailable",
            storage=volname,
        ))
        return False

    try:
        _validate_stored_pool_identity(volname, storage_info_data)
        if storage_info_data.get("type") != VOLUME_TYPE_EXTERNAL:
            _validate_stored_bricks(volname, storage_info_data)
        if legacy_storage_uid is None and not legacy_quarantine:
            _validate_deletion_record_owner(obj, storage_info_data)
    except RuntimeError as err:
        logging.error(logf(
            "Refusing unowned storage deletion",
            storage=volname,
            error=err,
        ))
        return False

    if (
            legacy_quarantine
            and storage_info_data.get(
                DELETION_QUARANTINE_RECORD_FIELD) is True):
        authoritative = _read_deletion_storage_info(
            core_v1_client,
            volname,
        )
        if authoritative is None:
            return True
        _validate_stored_pool_identity(volname, authoritative)
        if authoritative.get("type") != VOLUME_TYPE_EXTERNAL:
            _validate_stored_bricks(volname, authoritative)
        if _deletion_record_identity(authoritative) != (
                _deletion_record_identity(storage_info_data)):
            raise RuntimeError(
                f"Quarantined storage pool {volname} changed during deletion"
            )
        logging.error(logf(
            "Preserved unproven UID-less deletion orphan",
            storage=volname,
        ))
        return True

    # Once cleanup-only ownership is durable, a nonzero PV count cannot lead
    # to destructive cleanup. Check it while the shared provisioner remains
    # available, and acquire the global fence only when a zero count means the
    # pool may actually be removed. The fenced path always re-reads and
    # re-counts before mutation.
    if (
            not provisioner_fenced
            and storage_record_cleanup_only(storage_info_data)):
        authoritative = _read_deletion_storage_info(
            core_v1_client,
            volname,
        )
        if authoritative is None:
            logging.info(logf(
                "Storage deletion is already complete",
                storage=volname,
            ))
            return True
        _validate_deletion_snapshot(
            obj,
            storage_info_data,
            authoritative,
        )
        storage_info_data = authoritative
        if cleanup_quarantine:
            logging.error(logf(
                "Preserved quarantined cleanup-only storage pool",
                storage=volname,
            ))
            return True
        preliminary_pv_count = get_num_pvs(
            core_v1_client,
            storage_info_data,
        )
        if preliminary_pv_count == -1:
            logging.error(logf(
                "Storage delete failed before provisioner fence. "
                "Failed to get PV count",
                number_of_pvs=preliminary_pv_count,
                storage=volname,
            ))
            return False
        if preliminary_pv_count != 0:
            logging.warning(logf(
                "Storage deletion is waiting for PersistentVolumes to drain",
                number_of_pvs=preliminary_pv_count,
                storage=volname,
            ))
            return True

    # A tombstone prevents brand-new placement, but CSI must still complete
    # an already-persisted create intent for idempotency. Fence every cleanup
    # attempt which may remove the pool, then perform an authoritative zero-PV
    # recheck so such a retry cannot race server or metadata deletion.
    owns_fence = not provisioner_fenced
    if owns_fence and apps_v1_client is None:
        logging.error(
            "Storage deletion requires a CSI provisioner fence"
        )
        return False

    safe_to_resume = False
    try:
        if owns_fence:
            quiesce_csi_provisioner(core_v1_client, apps_v1_client)

        if core_v1_client is None:
            raise RuntimeError(
                "Storage deletion cannot revalidate metadata without the "
                "Kubernetes client"
            )
        if legacy_storage_uid is not None or legacy_quarantine:
            # Legacy sealing only writes a restrictive pool-local tombstone.
            # If its proof/CAS fails, restoring the previously running shared
            # provisioner is safe and avoids a permanent cluster-wide fence.
            safe_to_resume = True
            storage_info_data, cleanup_owned = seal_legacy_deletion_record(
                core_v1_client,
                apps_v1_client,
                storage_client,
                obj,
                storage_info_data,
                legacy_storage_uid=legacy_storage_uid,
                legacy_quarantine=legacy_quarantine,
            )
            # A persisted pool-local tombstone is sufficient to resume the
            # shared provisioner even when ownership proof was quarantined.
            safe_to_resume = True
            if not cleanup_owned:
                return True
            _validate_deletion_record_owner(obj, storage_info_data)
        authoritative = _read_deletion_storage_info(
            core_v1_client,
            volname,
        )
        if authoritative is None:
            # With no pool record, CSI cannot admit a new volume even if
            # cleanup of an already-orphaned StorageClass must be retried.
            safe_to_resume = True
            delete_storage_class(
                volname,
                None,
                expected_storage_uid=(
                    (obj.get("metadata") or {}).get("uid")
                    or storage_info_data.get("storage_uid")
                ),
                storage_class_source=storage_info_data,
            )
            logging.info(logf(
                "Storage deletion completed while waiting for the fence",
                storage=volname,
            ))
            return True
        _validate_deletion_snapshot(
            obj,
            storage_info_data,
            authoritative,
        )
        storage_info_data = authoritative
        if cleanup_quarantine:
            if storage_info_data.get("provisioning_disabled") is not True:
                raise RuntimeError(
                    f"Cleanup-only storage pool {volname} has no durable "
                    "provisioning tombstone"
                )
            safe_to_resume = True
            if not storage_record_cleanup_only(storage_info_data):
                storage_info_data = disable_pool_provisioning(
                    core_v1_client,
                    volname,
                    storage_info_data,
                )
            logging.error(logf(
                "Preserved quarantined cleanup-only storage pool",
                storage=volname,
            ))
            return True
        hostvol_type = storage_info_data.get("type")
        # Stop admitting new claims only after the fenced authoritative record
        # has been proven to be the same pool as the deletion snapshot. Persist
        # this before retiring the generated class so any later cleanup failure
        # can safely resume provisioning for unrelated pools.
        cleanup_sealed = storage_record_cleanup_only(storage_info_data)
        if not cleanup_sealed:
            storage_info_data = disable_pool_provisioning(
                core_v1_client,
                volname,
                storage_info_data,
            )
        safe_to_resume = True
        try:
            delete_storage_class(volname, hostvol_type, storage_info_data)
        except Exception as err:  # pylint: disable=broad-exception-caught
            storage_uid = storage_info_data.get("storage_uid")
            if not (
                    storage_info_data.get(CLEANUP_ONLY_RECORD_FIELD) is True
                    or (
                        isinstance(storage_uid, str)
                        and storage_uid.startswith(
                            CLEANUP_ONLY_STORAGE_UID_PREFIX)
                    )):
                raise
            logging.error(logf(
                "Deferred cleanup-only StorageClass retirement",
                storage=volname,
                error=err,
            ))
            return True

        # The tombstone excludes custom as well as generated StorageClasses.
        # Count only after it is visible under the global controller fence.
        pv_count = get_num_pvs(core_v1_client, storage_info_data)
        if pv_count == -1:
            logging.error(logf(
                "Storage delete failed after provisioner fence. "
                "Failed to get PV count",
                number_of_pvs=pv_count,
                storage=volname,
            ))
            return False
        if pv_count != 0:
            logging.warning(logf(
                "Storage deletion is waiting for PersistentVolumes to drain",
                number_of_pvs=pv_count,
                storage=volname,
            ))
            return True

        latest = _read_deletion_storage_info(core_v1_client, volname)
        if latest is None:
            logging.info(logf(
                "Storage deletion completed after PersistentVolume drain",
                storage=volname,
            ))
            return True
        _validate_deletion_snapshot(obj, storage_info_data, latest)
        storage_info_data = latest

        if hostvol_type != "External":
            delete_server_pods(storage_info_data, obj)
            filename = os.path.join(MANIFESTS_DIR, "services.yaml")
            template(filename, namespace=NAMESPACE, volname=volname)
            lib_execute(
                KUBECTL_CMD,
                DELETE_CMD,
                "-f",
                filename,
                "--ignore-not-found=true",
            )
            logging.info(logf(
                "Deleted Service",
                volname=volname,
                manifest=filename,
            ))

        delete_config_map(
            core_v1_client,
            obj,
            storage_info_data,
        )
        return True
    finally:
        if owns_fence and safe_to_resume:
            resume_csi_provisioner(apps_v1_client)


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


def get_num_pvs(core_v1_client, storage_info_data):
    """Count every PV attributable to a pool through the Kubernetes API."""
    return sum(1 for _pv in _pool_persistent_volumes(
        core_v1_client,
        storage_info_data["volname"],
        storage_class_name(storage_info_data),
    ))


def delete_server_pods(storage_info_data, obj):
    """
    Delete server pods depending on type of Hosting
    Volume and other options specified
    """

    volname = obj["metadata"]["name"]
    voltype = storage_info_data['type']
    volumeid = storage_info_data['volume_id']

    docker_user = os.environ.get("DOCKER_USER", "joejulian")

    shd_required = voltype in HEAL_GATED_VOLUME_TYPES

    template_args = {
        "namespace": NAMESPACE,
        "kadalu_version": VERSION,
        "docker_user": docker_user,
        "images_hub": IMAGES_HUB,
        "volname": volname,
        "voltype": voltype,
        "volume_id": volumeid,
        "shd_required": shd_required,
        "tolerations": storage_info_data.get("tolerations", []),
        "verbose": VERBOSE,
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
        template_args["brick_device_dir"] = brick.get(
            "brick_device_dir",
            "",
        )
        template_args["brick_node_id"] = brick['node_id']
        template_args["k8s_dist"] = K8S_DIST

        filename = os.path.join(MANIFESTS_DIR, "server.yaml")
        template(filename, **template_args)
        lib_execute(
            KUBECTL_CMD,
            DELETE_CMD,
            "-f",
            filename,
            "--ignore-not-found=true",
        )
        logging.info(logf(
            "Deleted Server pod",
            volname=volname,
            manifest=filename,
            node=brick['node']
        ))


def delete_config_map(
        core_v1_client, obj, expected_storage_info=None):
    """
    Volinfo of existing Volume is generated and ConfigMap is deleted
    """

    volname = obj["metadata"]["name"]

    # Add new entry in the existing config map
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE)

    volinfo_file = "%s.info" % volname
    if expected_storage_info is not None:
        current = _decode_pool_record(
            (configmap_data.data or {}).get(volinfo_file)
        )
        if current is None:
            raise RuntimeError(
                f"Storage metadata for {volname} disappeared before cleanup"
            )
        _validate_deletion_snapshot(obj, expected_storage_info, current)
    configmap_data.data[volinfo_file] = None

    core_v1_client.patch_namespaced_config_map(
        KADALU_CONFIG_MAP, NAMESPACE, configmap_data)
    logging.info(logf(
        "Deleted configmap",
        name=KADALU_CONFIG_MAP,
        volname=volname
    ))


def delete_storage_class(
        hostvol_name, _hostvol_type, storage_info_data=None,
        storage_api=None, expected_storage_uid=None,
        storage_class_source=None):
    """
    Deletes deployed External and Custom StorageClass
    """

    source = storage_info_data or storage_class_source or {
        "metadata": {"name": hostvol_name},
        "spec": {},
    }
    sc_name = storage_class_name(source)
    if storage_api is None:
        storage_api = client.StorageV1Api()
    installed = existing_storage_class(storage_api, sc_name)
    if installed is None:
        logging.info(logf(
            "Storage class is already absent",
            volname=hostvol_name,
        ))
        return

    if storage_info_data is not None:
        validate_storage_class_owner(
            installed,
            storage_class_identity(storage_info_data),
        )
    else:
        # Interrupted cleanup may have removed the pool record first. Only an
        # explicitly annotated class can then prove it belongs to this
        # namespace; never delete an unowned same-name cluster-scoped object.
        if not expected_storage_uid:
            raise RuntimeError(
                f"StorageClass {sc_name} has no deletion owner proof"
            )
        identity = {
            "namespace": NAMESPACE,
            "name": hostvol_name,
            "storage_class_name": sc_name,
            "uid": expected_storage_uid,
            "volume_id": "",
            "mount_identity": "",
            "backend_fingerprint": "",
            "parameters": _storage_class_value(
                installed,
                "parameters",
                {},
            ) or {},
        }
        validate_storage_class_owner(
            installed,
            identity,
            allow_legacy=False,
        )
    _delete_storage_class_preconditioned(storage_api, installed, sc_name)
    logging.info(logf(
        "Deleted Storage class",
        volname=hostvol_name
    ))


def reconcile_initial_storage(
        core_v1_client, storage_items, apps_v1_client=None,
        provisioner_fenced=False, storage_client=None):
    """Reconcile every active object in one authoritative list snapshot."""
    for item in storage_items:
        metadata = item.get("metadata") or {}
        name = metadata.get("name")
        if not name:
            raise RuntimeError("Unnamed Kadalustorage in initial snapshot")
        if metadata.get("deletionTimestamp"):
            continue
        if not handle_added(
                core_v1_client,
                item,
                apps_v1_client,
                provisioner_fenced=provisioner_fenced,
                storage_client=storage_client):
            raise RuntimeError(
                f"Initial reconciliation failed for Kadalustorage {name}"
            )
    return True


def reconcile_watch_storage_until_stable(
        core_v1_client, apps_v1_client, storage_client, storage_plan):
    """Re-plan after orphan cleanup so newly unblocked CRs are reconciled."""
    candidate = storage_plan
    while True:
        candidate_fingerprint = _storage_plan_fingerprint(candidate)
        reconcile_initial_storage(
            core_v1_client,
            candidate["reconcile_items"],
            apps_v1_client,
            storage_client=storage_client,
        )
        reconcile_planned_deletion_orphans(
            core_v1_client,
            apps_v1_client,
            storage_client,
            candidate,
        )
        if not candidate["deletion_orphans"]:
            return candidate
        fresh = reconcile_nodeplugin_from_storage(
            core_v1_client,
            apps_v1_client,
            storage_client,
        )
        if _storage_plan_fingerprint(fresh) == candidate_fingerprint:
            return fresh
        candidate = fresh


def watch_stream(
        core_v1_client, k8s_client, apps_v1_client=None,
        resource_version=None):
    """Watch from the last successfully reconciled event resource version."""
    crds = client.CustomObjectsApi(k8s_client)
    k8s_watch = watch.Watch()
    acknowledged_version = resource_version
    if acknowledged_version is None:
        initial_list = list_storage_resources(crds)
        initial_items = initial_list["items"]
        if core_v1_client is not None and apps_v1_client is not None:
            storage_plan = reconcile_nodeplugin_from_storage(
                core_v1_client,
                apps_v1_client,
                crds,
                storage_list=initial_list,
            )
            storage_plan = reconcile_watch_storage_until_stable(
                core_v1_client,
                apps_v1_client,
                crds,
                storage_plan,
            )
            acknowledged_version = storage_plan["resource_version"]
        else:
            acknowledged_version = initial_list["metadata"]["resourceVersion"]
            reconcile_initial_storage(
                core_v1_client,
                initial_items,
                apps_v1_client,
                storage_client=crds,
            )
    elif core_v1_client is not None and apps_v1_client is not None:
        # Periodic watch EOFs intentionally re-list deletion-orphans so a pool
        # retained while nonempty is cleaned once its last PV disappears.
        # Resume after the authoritative snapshot: cleanup can remove an old
        # ownership marker, so replaying pre-snapshot events is unsafe.
        storage_plan = reconcile_nodeplugin_from_storage(
            core_v1_client,
            apps_v1_client,
            crds,
        )
        storage_plan = reconcile_watch_storage_until_stable(
            core_v1_client,
            apps_v1_client,
            crds,
            storage_plan,
        )
        acknowledged_version = storage_plan["resource_version"]

    for event in k8s_watch.stream(crds.list_namespaced_custom_object,
                                  "kadalu-operator.storage",
                                  "v1alpha1",
                                  NAMESPACE,
                                  "kadalustorages",
                                  resource_version=acknowledged_version,
                                  timeout_seconds=WATCH_TIMEOUT_SECONDS,
                                  allow_watch_bookmarks=True):
        obj = event["object"]
        operation = event['type']
        if operation == "ERROR":
            status = obj.get("code")
            message = obj.get("message") or "Kadalustorage watch failed"
            raise WatchStatusError(message, status)

        metadata = obj.get("metadata") or {}
        event_resource_version = metadata.get("resourceVersion")
        if operation == "BOOKMARK":
            if event_resource_version:
                acknowledged_version = event_resource_version
            continue
        if operation not in ("ADDED", "MODIFIED", "DELETED"):
            logging.warning(logf(
                "Ignored unknown Kadalustorage watch event",
                operation=operation,
            ))
            continue
        if not event_resource_version:
            raise RuntimeError(
                f"Kadalustorage {operation} event has no resourceVersion"
            )

        logging.debug(logf("Event", operation=operation, object=repr(obj)))
        if (
                operation in ("ADDED", "MODIFIED")
                and not validate_volume_request(obj)):
            logging.error(logf(
                "Acknowledged quarantined invalid Kadalustorage event",
                operation=operation,
                storage=metadata.get("name", "<unknown>"),
            ))
            reconciled = True
        elif event_targets_deletion_blocked_record(core_v1_client, obj):
            logging.error(logf(
                "Acknowledged quarantined Kadalustorage event for "
                "deletion-blocked pool",
                operation=operation,
                storage=metadata.get("name", "<unknown>"),
            ))
            reconciled = True
        elif operation == "ADDED":
            reconciled = handle_added(
                core_v1_client,
                obj,
                apps_v1_client,
                storage_client=crds,
            )
        elif operation == "MODIFIED":
            reconciled = handle_modified(
                core_v1_client,
                obj,
                apps_v1_client,
                storage_client=crds,
            )
        else:
            reconciled = handle_deleted(
                core_v1_client,
                obj,
                apps_v1_client=apps_v1_client,
            )

        if not reconciled:
            name = metadata.get("name", "<unknown>")
            raise RuntimeError(
                f"{operation} reconciliation failed for "
                f"Kadalustorage {name}"
            )
        if core_v1_client is not None and apps_v1_client is not None:
            storage_plan = reconcile_nodeplugin_from_storage(
                core_v1_client,
                apps_v1_client,
                crds,
            )
            storage_plan = reconcile_watch_storage_until_stable(
                core_v1_client,
                apps_v1_client,
                crds,
                storage_plan,
            )
            # The LIST may have reconciled changes beyond this watch event.
            # End this stream so buffered pre-snapshot events cannot mutate
            # state after cleanup removed its ownership tombstone.
            return storage_plan["resource_version"]
        acknowledged_version = event_resource_version
    return acknowledged_version


def crd_watch(
        core_v1_client, k8s_client, apps_v1_client=None,
        resource_version=None):
    """
    Watches the CRD to provision new PV Hosting Volumes
    """
    while True:
        try:
            resource_version = watch_stream(
                core_v1_client,
                k8s_client,
                apps_v1_client,
                resource_version=resource_version,
            )
        except (ProtocolError, NewConnectionError):
            # It might so happen that this'll be logged for every hit in k8s
            # event stream in kadalu namespace and better to log at debug level
            logging.debug(
                logf(
                    "Watch connection broken and restarting watch on the stream"
                ))
            resource_version = None
            time.sleep(30)
        except Exception as err:  # pylint: disable=broad-exception-caught
            if getattr(err, "status", None) not in (410, "410"):
                raise
            logging.warning(
                "Kadalustorage watch resource version expired; re-listing"
            )
            resource_version = None


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


def resume_csi_provisioner(apps_v1_client):
    """Restore provisioning after a fenced runtime deletion completes."""
    apps_v1_client.patch_namespaced_stateful_set(
        CSI_PROVISIONER,
        NAMESPACE,
        {"spec": {"replicas": 1}},
    )
    logging.info("CSI provisioner resumed after storage deletion fence")


def csi_nodeplugin_rollout_complete(daemon_set):
    """Return whether every desired node runs the observed DaemonSet revision."""
    if daemon_set is None:
        return False
    metadata = getattr(daemon_set, "metadata", None)
    status = getattr(daemon_set, "status", None)
    if metadata is None or status is None:
        return False

    desired = getattr(status, "desired_number_scheduled", None)
    updated = getattr(status, "updated_number_scheduled", None)
    ready = getattr(status, "number_ready", None)
    if desired is None or updated is None or ready is None:
        return False

    generation = getattr(metadata, "generation", None)
    observed = getattr(status, "observed_generation", None)
    if (
            generation is None
            or observed is None
            or observed < generation):
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
        timeout_seconds=CSI_NODEPLUGIN_ROLLOUT_TIMEOUT_SECONDS,
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


def server_statefulset_rollout_complete(stateful_set):
    """Return whether one server StatefulSet is fully on its desired revision."""
    if stateful_set is None:
        return False
    metadata = getattr(stateful_set, "metadata", None)
    spec = getattr(stateful_set, "spec", None)
    status = getattr(stateful_set, "status", None)
    if metadata is None or spec is None or status is None:
        return False

    generation = getattr(metadata, "generation", None)
    observed = getattr(status, "observed_generation", None)
    desired = getattr(spec, "replicas", None)
    if (
            generation is None
            or observed is None
            or observed < generation
            or desired != 1):
        return False

    current_revision = getattr(status, "current_revision", None)
    update_revision = getattr(status, "update_revision", None)
    if (
            not current_revision
            or not update_revision
            or current_revision != update_revision):
        return False

    replica_counts = (
        getattr(status, "replicas", None),
        getattr(status, "current_replicas", None),
        getattr(status, "updated_replicas", None),
        getattr(status, "ready_replicas", None),
        getattr(status, "available_replicas", None),
    )
    return all(count == desired for count in replica_counts)


def wait_for_server_statefulset_rollout(
        apps_v1_client,
        name,
        timeout_seconds=SERVER_ROLLOUT_TIMEOUT_SECONDS,
        poll_interval=2):
    """Wait for one singleton server before allowing the next to update."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        stateful_set = apps_v1_client.read_namespaced_stateful_set(
            name,
            NAMESPACE,
        )
        if server_statefulset_rollout_complete(stateful_set):
            logging.info(logf(
                "Server StatefulSet rollout is complete",
                statefulset=name,
            ))
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for server StatefulSet {name} to reach "
                "its desired revision with one Ready and Available replica. "
                "Further brick StatefulSet rollouts were not attempted."
            )
        time.sleep(poll_interval)


def wait_for_csi_provisioner_rollout(
        apps_v1_client,
        timeout_seconds=CSI_PROVISIONER_ROLLOUT_TIMEOUT_SECONDS,
        poll_interval=2):
    """Wait until the singleton CSI provisioner is Ready and current."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            stateful_set = apps_v1_client.read_namespaced_stateful_set(
                CSI_PROVISIONER,
                NAMESPACE,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            if getattr(err, "status", None) not in (404, "404"):
                raise
            stateful_set = None

        if server_statefulset_rollout_complete(stateful_set):
            logging.info("CSI provisioner rollout is complete")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Timed out waiting for the CSI provisioner StatefulSet to "
                "reach its current revision with one Ready and Available "
                "replica. Operator readiness remains false."
            )
        time.sleep(poll_interval)


def deploy_csi_pods(
        core_v1_client, provisioner_replicas=1,
        nodeplugin_tolerations=None):
    """
    Look for CSI pods, if any one CSI pod found then
    that means it is deployed
    """
    if provisioner_replicas not in (0, 1):
        raise ValueError(
            "CSI provisioner replicas must be zero (upgrade fence) or one"
        )
    nodeplugin_tolerations = normalize_tolerations(
        nodeplugin_tolerations,
        "CSI nodeplugin",
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
             provisioner_replicas=provisioner_replicas,
             nodeplugin_tolerations=nodeplugin_tolerations)

    lib_execute(KUBECTL_CMD, APPLY_CMD, "-f", filename)
    logging.info(logf("Deployed CSI Pods", manifest=filename))


def deploy_csi_upgrade(
        core_v1_client, apps_v1_client, storage_client):
    """Keep provisioning fenced until metadata and every node are current."""
    storage_plan = prepare_storage_upgrade(
        core_v1_client,
        apps_v1_client,
        list_storage_resources(storage_client),
    )
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    migration_required = legacy_mount_migration_required(configmap_data.data)
    if migration_required:
        # Reject a known-incompatible legacy block volume before disrupting
        # provisioning. A second check after the fence closes the create race.
        ensure_kadalu_block_volumes_absent(
            core_v1_client,
            "migrate legacy mount identities",
        )
    preflight_existing_storage_heal(apps_v1_client, storage_plan)
    quiesce_csi_provisioner(core_v1_client, apps_v1_client)
    # The initial pass avoids disrupting a cluster with invalid legacy state.
    # Re-list after the fence so no CR or last-moment PV change can make the
    # published nodeplugin policy stale while nodes are rotated.
    storage_plan = prepare_storage_upgrade(
        core_v1_client,
        apps_v1_client,
        list_storage_resources(storage_client),
    )
    configmap_data = core_v1_client.read_namespaced_config_map(
        KADALU_CONFIG_MAP,
        NAMESPACE,
    )
    migration_required = legacy_mount_migration_required(configmap_data.data)
    if migration_required:
        ensure_kadalu_block_volumes_absent(
            core_v1_client,
            "migrate legacy mount identities",
        )

    # Publish the desired OnDelete template without restarting live clients or
    # allowing the controller to create block volumes during the transition.
    deploy_csi_pods(
        core_v1_client,
        provisioner_replicas=0,
        nodeplugin_tolerations=storage_plan["nodeplugin_tolerations"],
    )
    migrate_legacy_single_pv_claims(core_v1_client)
    return gate_nodeplugin_until_current(
        core_v1_client,
        apps_v1_client,
        storage_client,
        storage_plan,
    )


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


def storage_class_identity(source):
    """Return the immutable owner and parameters for a generated class."""
    spec = source.get("spec")
    if isinstance(spec, dict):
        metadata = source.get("metadata") or {}
        volname = metadata.get("name")
        storage_uid = metadata.get("uid", "")
        voltype = spec.get("type")
        volume_id = spec.get("volume_id", "")
        mount_identity = spec.get("mount_identity", "")
        single_pv_per_pool = get_single_pv_per_pool(spec)
        if voltype == VOLUME_TYPE_EXTERNAL:
            details = spec.get("details") or {}
            hosts = []
            if details.get("gluster_host"):
                hosts.append(details["gluster_host"])
            hosts.extend(details.get("gluster_hosts") or [])
            parameters = {
                "hostvol_type": VOLUME_TYPE_EXTERNAL,
                "gluster_hosts": ",".join(_canonical_gluster_hosts(hosts)),
                "gluster_volname": details.get("gluster_volname", ""),
                "single_pv_per_pool": str(single_pv_per_pool),
            }
            if details.get("gluster_options", ""):
                parameters["gluster_options"] = details["gluster_options"]
        else:
            parameters = {
                "storage_name": volname,
                "single_pv_per_pool": str(single_pv_per_pool),
            }
    else:
        volname = source.get("volname")
        storage_uid = source.get("storage_uid", "")
        voltype = source.get("type")
        volume_id = source.get("volume_id", "")
        mount_identity = source.get("mount_identity", "")
        single_pv_per_pool = get_single_pv_per_pool(source)
        if voltype == VOLUME_TYPE_EXTERNAL:
            parameters = {
                "hostvol_type": VOLUME_TYPE_EXTERNAL,
                "gluster_hosts": ",".join(_canonical_gluster_hosts(
                    source.get("gluster_hosts", "")
                )),
                "gluster_volname": source.get("gluster_volname", ""),
                "single_pv_per_pool": str(single_pv_per_pool),
            }
            if source.get("gluster_options", ""):
                parameters["gluster_options"] = source["gluster_options"]
        else:
            parameters = {
                "storage_name": volname,
                "single_pv_per_pool": str(single_pv_per_pool),
            }

    return {
        "namespace": NAMESPACE,
        "name": volname,
        "storage_class_name": storage_class_name(source),
        "uid": str(storage_uid or ""),
        "volume_id": str(volume_id or ""),
        "mount_identity": str(mount_identity or ""),
        "backend_fingerprint": storage_class_backend_fingerprint(source),
        "parameters": parameters,
    }


def _storage_class_metadata_value(storage_class, field, default=None):
    """Read a StorageClass metadata value from model or mapping objects."""
    metadata = (
        storage_class.get("metadata", {})
        if isinstance(storage_class, dict)
        else getattr(storage_class, "metadata", None)
    )
    if isinstance(metadata, dict):
        return metadata.get(field, default)
    return getattr(metadata, field, default) if metadata is not None else default


def _storage_class_value(storage_class, field, default=None):
    """Read a StorageClass value from model or mapping objects."""
    if isinstance(storage_class, dict):
        return storage_class.get(field, default)
    return getattr(storage_class, field, default)


def _storage_class_uid(storage_class, name):
    """Return the UID required to delete exactly the validated class."""
    uid = _storage_class_metadata_value(storage_class, "uid", "")
    if not uid:
        raise RuntimeError(
            f"StorageClass {name} has no UID for deletion precondition"
        )
    return str(uid)


def _storage_class_resource_version(storage_class, name):
    """Return the resource version required for an exact deletion."""
    resource_version = _storage_class_metadata_value(
        storage_class,
        "resource_version",
        _storage_class_metadata_value(storage_class, "resourceVersion", ""),
    )
    if not resource_version:
        raise RuntimeError(
            f"StorageClass {name} has no resourceVersion for deletion "
            "precondition"
        )
    return str(resource_version)


def _delete_storage_class_preconditioned(
        storage_api, storage_class, name,
        timeout_seconds=STORAGE_CLASS_DELETE_TIMEOUT_SECONDS,
        poll_interval=1):
    """Delete exactly one validated StorageClass incarnation and wait."""
    uid = _storage_class_uid(storage_class, name)
    delete_options = client.V1DeleteOptions(
        preconditions=client.V1Preconditions(
            uid=uid,
            resource_version=_storage_class_resource_version(
                storage_class,
                name,
            ),
        ),
    )
    try:
        storage_api.delete_storage_class(name, body=delete_options)
    except Exception as err:  # pylint: disable=broad-exception-caught
        if getattr(err, "status", None) not in (404, "404"):
            raise

    deadline = time.monotonic() + timeout_seconds
    while True:
        current = existing_storage_class(storage_api, name)
        if current is None:
            return
        if _storage_class_uid(current, name) != uid:
            raise RuntimeError(
                f"StorageClass {name} was replaced during deletion"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for StorageClass {name} deletion"
            )
        time.sleep(poll_interval)


def _storage_class_owner_annotations(identity):
    """Return the complete annotations rendered for an owned class."""
    return {
        STORAGE_CLASS_NAMESPACE_ANNOTATION: identity["namespace"],
        STORAGE_CLASS_NAME_ANNOTATION: identity["name"],
        STORAGE_CLASS_UID_ANNOTATION: identity["uid"],
        STORAGE_CLASS_VOLUME_ID_ANNOTATION: identity["volume_id"],
        STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION: identity["mount_identity"],
        STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION: (
            identity["backend_fingerprint"]
        ),
    }


def _canonical_storage_class_parameters(parameters):
    """Normalize semantically unordered StorageClass parameters."""
    normalized = dict(parameters or {})
    if normalized.get("hostvol_type") == VOLUME_TYPE_EXTERNAL:
        normalized["gluster_hosts"] = ",".join(_canonical_gluster_hosts(
            normalized.get("gluster_hosts", "")
        ))
    return normalized


def validate_storage_class_owner(
        storage_class, identity, *, allow_legacy=None):
    """Reject a generated class which is not demonstrably this pool's."""
    name = _storage_class_metadata_value(storage_class, "name", "")
    expected_name = identity["storage_class_name"]
    if allow_legacy is None:
        allow_legacy = (
            expected_name
            == f"{STORAGE_CLASS_NAME_PREFIX}{identity['name']}"
        )
    provisioner = _storage_class_value(storage_class, "provisioner", "")
    parameters = _storage_class_value(storage_class, "parameters", {}) or {}
    if (
            name != expected_name
            or provisioner != CSI_DRIVER_NAME
            or _canonical_storage_class_parameters(parameters)
            != _canonical_storage_class_parameters(identity["parameters"])):
        raise RuntimeError(
            f"StorageClass {expected_name} is not owned by Kadalustorage "
            f"{identity['namespace']}/{identity['name']}"
        )

    annotations = (
        _storage_class_metadata_value(storage_class, "annotations", {}) or {}
    )
    owner_keys = {
        STORAGE_CLASS_NAMESPACE_ANNOTATION,
        STORAGE_CLASS_NAME_ANNOTATION,
        STORAGE_CLASS_UID_ANNOTATION,
        STORAGE_CLASS_VOLUME_ID_ANNOTATION,
        STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION,
        STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION,
    }
    has_owner_annotation = any(key in annotations for key in owner_keys)
    if not has_owner_annotation:
        if allow_legacy:
            return
        raise RuntimeError(
            f"StorageClass {expected_name} has no Kadalu ownership metadata"
        )

    if (
            annotations.get(STORAGE_CLASS_NAMESPACE_ANNOTATION)
            != identity["namespace"]
            or annotations.get(STORAGE_CLASS_NAME_ANNOTATION)
            != identity["name"]
            or (
                identity["uid"]
                and annotations.get(STORAGE_CLASS_UID_ANNOTATION)
                != identity["uid"]
            )):
        raise RuntimeError(
            f"StorageClass {expected_name} is owned by another "
            "Kadalustorage"
        )

    durable_values = (
        (STORAGE_CLASS_VOLUME_ID_ANNOTATION, identity.get("volume_id", "")),
        (
            STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION,
            identity.get("mount_identity", ""),
        ),
        (
            STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION,
            identity.get("backend_fingerprint", ""),
        ),
    )
    if any(
            key in annotations
            and expected
            and annotations.get(key) != expected
            for key, expected in durable_values):
        raise RuntimeError(
            f"StorageClass {expected_name} has a different durable "
            "storage identity"
        )


def recover_storage_class_identity(storage_class, source):
    """Recover durable pool identity from a fully annotated exact class."""
    identity = storage_class_identity(source)
    validate_storage_class_owner(storage_class, identity)
    annotations = (
        _storage_class_metadata_value(storage_class, "annotations", {}) or {}
    )
    durable_keys = {
        STORAGE_CLASS_VOLUME_ID_ANNOTATION,
        STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION,
        STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION,
    }
    present = durable_keys.intersection(annotations)
    if not present:
        return None
    if present != durable_keys:
        raise RuntimeError(
            "StorageClass durable storage identity is incomplete"
        )
    if not identity["uid"]:
        raise RuntimeError(
            "Kadalustorage UID is required for durable recovery"
        )
    validate_storage_class_owner(
        storage_class,
        identity,
        allow_legacy=False,
    )
    volume_id = annotations[STORAGE_CLASS_VOLUME_ID_ANNOTATION]
    mount_identity = annotations[STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION]
    try:
        canonical_volume_id = str(uuid.UUID(volume_id)) == volume_id
    except (AttributeError, TypeError, ValueError):
        canonical_volume_id = False
    if (
            not canonical_volume_id
            or _canonical_mount_identity(mount_identity) != mount_identity
            or annotations[STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION]
            != identity["backend_fingerprint"]):
        raise RuntimeError(
            "StorageClass durable storage identity is invalid"
        )
    if identity["volume_id"] and identity["volume_id"] != volume_id:
        raise RuntimeError(
            "StorageClass has a different durable storage identity"
        )
    if (
            identity["mount_identity"]
            and identity["mount_identity"] != mount_identity):
        raise RuntimeError(
            "StorageClass has a different durable storage identity"
        )
    return {
        "volume_id": volume_id,
        "mount_identity": mount_identity,
    }


def storage_class_proves_current_cr(storage_class, source):
    """Return whether exact owner annotations prove this CR incarnation."""
    if storage_class is None:
        return False
    identity = storage_class_identity(source)
    if not identity["uid"]:
        return False
    annotations = (
        _storage_class_metadata_value(storage_class, "annotations", {}) or {}
    )
    return (
        _storage_class_metadata_value(storage_class, "name", "")
        == identity["storage_class_name"]
        and annotations.get(STORAGE_CLASS_NAMESPACE_ANNOTATION)
        == identity["namespace"]
        and annotations.get(STORAGE_CLASS_NAME_ANNOTATION)
        == identity["name"]
        and annotations.get(STORAGE_CLASS_UID_ANNOTATION) == identity["uid"]
    )


def preflight_existing_pool_storage_class(storage_api, obj, record):
    """Validate a class against the pool's prospective durable record."""
    source = dict(record)
    storage_uid = (obj.get("metadata") or {}).get("uid")
    if not source.get("storage_uid") and storage_uid:
        source["storage_uid"] = storage_uid
    identity = preflight_storage_class_owner(storage_api, source)
    name = identity["storage_class_name"]
    installed = existing_storage_class(storage_api, name)
    recovered_mount_identity = None
    if installed is not None:
        durable_identity = recover_storage_class_identity(installed, source)
        if durable_identity is not None:
            recovered_mount_identity = durable_identity["mount_identity"]
            if not source.get("mount_identity"):
                source["mount_identity"] = recovered_mount_identity
        validate_storage_class_owner(
            installed,
            storage_class_identity(source),
        )
    return source, recovered_mount_identity


def existing_storage_class(storage_api, name):
    """Return one installed StorageClass without trusting a name collision."""
    return next((
        item for item in storage_api.list_storage_class().items
        if _storage_class_metadata_value(item, "name") == name
    ), None)


def _owned_storage_classes(storage_api, identity):
    """Return classes annotated as belonging to one logical storage pool."""
    owned = []
    for storage_class in storage_api.list_storage_class().items or []:
        annotations = (
            _storage_class_metadata_value(
                storage_class,
                "annotations",
                {},
            ) or {}
        )
        if (
                annotations.get(STORAGE_CLASS_NAMESPACE_ANNOTATION)
                == identity["namespace"]
                and annotations.get(STORAGE_CLASS_NAME_ANNOTATION)
                == identity["name"]):
            owned.append(storage_class)
    return owned


def preflight_legacy_orphan_storage_class(storage_api, source):
    """Require the exact unannotated class emitted for a legacy pool."""
    identity = storage_class_identity(source)
    canonical_name = f"{STORAGE_CLASS_NAME_PREFIX}{identity['name']}"
    if identity["storage_class_name"] != canonical_name:
        raise RuntimeError(
            f"UID-less storage pool {identity['name']} has a noncanonical "
            "StorageClass name"
        )
    installed = existing_storage_class(storage_api, canonical_name)
    if installed is None:
        raise RuntimeError(
            f"UID-less storage pool {identity['name']} has no retained "
            "legacy StorageClass"
        )
    annotations = (
        _storage_class_metadata_value(installed, "annotations", {}) or {}
    )
    owner_keys = {
        STORAGE_CLASS_NAMESPACE_ANNOTATION,
        STORAGE_CLASS_NAME_ANNOTATION,
        STORAGE_CLASS_UID_ANNOTATION,
        STORAGE_CLASS_VOLUME_ID_ANNOTATION,
        STORAGE_CLASS_MOUNT_IDENTITY_ANNOTATION,
        STORAGE_CLASS_BACKEND_FINGERPRINT_ANNOTATION,
    }
    if any(key in annotations for key in owner_keys):
        raise RuntimeError(
            f"UID-less storage pool {identity['name']} has a StorageClass "
            "with newer ownership metadata"
        )
    if _owned_storage_classes(storage_api, identity):
        raise RuntimeError(
            f"UID-less storage pool {identity['name']} has another owned "
            "StorageClass"
        )
    validate_storage_class_owner(installed, identity, allow_legacy=True)
    return installed


def preflight_storage_class_owner(storage_api, source):
    """Validate a generated StorageClass before writing pool metadata."""
    identity = storage_class_identity(source)
    name = identity["storage_class_name"]
    conflicting_names = sorted({
        _storage_class_metadata_value(item, "name", "")
        for item in _owned_storage_classes(storage_api, identity)
        if _storage_class_metadata_value(item, "name", "") != name
    })
    if conflicting_names:
        raise RuntimeError(
            f"Kadalustorage {identity['namespace']}/{identity['name']} "
            "already owns a different StorageClass: "
            f"{', '.join(conflicting_names)}"
        )
    canonical_name = f"{STORAGE_CLASS_NAME_PREFIX}{identity['name']}"
    if name != canonical_name:
        canonical_class = existing_storage_class(storage_api, canonical_name)
        if canonical_class is not None:
            canonical_identity = {
                **identity,
                "storage_class_name": canonical_name,
            }
            validate_storage_class_owner(
                canonical_class,
                canonical_identity,
            )
            raise RuntimeError(
                f"Kadalustorage {identity['namespace']}/{identity['name']} "
                "already has the canonical StorageClass "
                f"{canonical_name}"
            )
    installed = existing_storage_class(storage_api, name)
    if installed is not None:
        validate_storage_class_owner(installed, identity)
    return identity


def reconcile_storage_class_manifest(
        storage_api, filename, name, reclaim_policy, identity=None):
    """Create or precondition-replace without adopting a racing object."""
    if identity is None:
        raise RuntimeError(
            f"StorageClass {name} cannot be reconciled without owner identity"
        )
    existing = existing_storage_class(storage_api, name)
    if existing is not None:
        validate_storage_class_owner(existing, identity)
    existing_policy = _k8s_value(
        existing,
        "reclaimPolicy",
        "reclaim_policy",
    )
    # Kubernetes defaults an omitted StorageClass policy to Delete, so legacy
    # classes which did not render this field do not need replacement.
    existing_policy = existing_policy or "Delete"
    replace_existing = False
    if existing is not None:
        annotations = (
            _storage_class_metadata_value(existing, "annotations", {}) or {}
        )
        expected_annotations = _storage_class_owner_annotations(identity)
        expected_expansion = (
            str(identity["parameters"].get("single_pv_per_pool", "false"))
            .lower() != "true"
        )
        existing_expansion = bool(_k8s_value(
            existing,
            "allowVolumeExpansion",
            "allow_volume_expansion",
            False,
        ))
        replace_existing = (
            existing_policy != reclaim_policy
            or any(
                annotations.get(key) != value
                for key, value in expected_annotations.items()
            )
            or existing_expansion != expected_expansion
        )

    if replace_existing:
        logging.info(logf(
            "Replacing StorageClass with an owned desired object",
            name=name,
            existing=existing_policy,
            requested=reclaim_policy,
        ))
        # Removing a StorageClass does not remove its PVs or PVCs. Delete only
        # the validated UID and wait for it to disappear before creating the
        # immutable replacement. A racing same-name object is never adopted.
        _delete_storage_class_preconditioned(storage_api, existing, name)
    if existing is None or replace_existing:
        lib_execute(KUBECTL_CMD, CREATE_CMD, "-f", filename)


def deploy_storage_class(obj, core_v1_client=None, storage_api=None):
    """Deploys the default and custom storage class for KaDalu if not exists"""

    # Deploy defalut Storage Class
    if storage_api is None:
        storage_api = client.StorageV1Api()
    storage_class_source = obj
    if core_v1_client is not None:
        configmap_data = core_v1_client.read_namespaced_config_map(
            KADALU_CONFIG_MAP,
            NAMESPACE,
        )
        serialized = configmap_data.data.get(
            f"{obj['metadata']['name']}.info"
        )
        if serialized is not None:
            stored = _decode_pool_record(serialized)
            if stored is None:
                raise RuntimeError(
                    "Unable to read post-write StorageClass identity"
                )
            storage_class_source = stored
    storage_class_owner = preflight_storage_class_owner(
        storage_api,
        storage_class_source,
    )
    sc_names = []
    for tmpl in os.listdir(TEMPLATES_DIR):
        if tmpl.startswith("storageclass-") and tmpl.endswith(".j2"):
            sc_names.append(
                tmpl.replace("storageclass-", "").replace(".yaml.j2", "")
            )

    for sc_name in sc_names:
        filename = os.path.join(MANIFESTS_DIR, "storageclass-%s.yaml" % sc_name)
        class_name = storage_class_owner["storage_class_name"]
        reclaim_policy = storage_class_reclaim_policy(
            obj["spec"].get("pvReclaimPolicy", "delete")
        )

        template(filename, namespace=NAMESPACE, kadalu_version=VERSION,
                 hostvol_name=obj["metadata"]["name"],
                 storage_class_name=class_name,
                 storage_uid=storage_class_owner["uid"],
                 volume_id=storage_class_owner["volume_id"],
                 mount_identity=storage_class_owner["mount_identity"],
                 backend_fingerprint=(
                     storage_class_owner["backend_fingerprint"]
                 ),
                 single_pv_per_pool=get_single_pv_per_pool(obj["spec"]),
                 reclaim_policy=reclaim_policy)
        reconcile_storage_class_manifest(
            storage_api,
            filename,
            class_name,
            reclaim_policy,
            storage_class_owner,
        )
        if core_v1_client is not None:
            reconcile_pool_pv_reclaim_policy(
                core_v1_client,
                obj["metadata"]["name"],
                obj["spec"].get("pvReclaimPolicy", "delete"),
                before_config_write=None,
                class_name=class_name,
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

def _run_operator():
    """Reconcile fenced startup state, publish readiness, and watch CRs."""
    config.load_incluster_config()

    core_v1_client = client.CoreV1Api()
    k8s_client = client.ApiClient()
    apps_v1_client = client.AppsV1Api(k8s_client)
    storage_client = client.CustomObjectsApi(k8s_client)

    # ConfigMap
    uid, upgrade = deploy_config_map(core_v1_client)

    # Fresh installs and upgrades use the same fence. Pre-created CRs must be
    # reconciled before a provisioner can serve requests.
    storage_plan = deploy_csi_upgrade(
        core_v1_client,
        apps_v1_client,
        storage_client,
    )
    if upgrade:
        logging.info(logf("Upgrading to ", version=VERSION))
    storage_plan = reconcile_storage_until_stable(
        core_v1_client,
        apps_v1_client,
        storage_client,
        storage_plan,
    )
    deploy_csi_pods(
        core_v1_client,
        provisioner_replicas=1,
        nodeplugin_tolerations=storage_plan["nodeplugin_tolerations"],
    )
    wait_for_csi_provisioner_rollout(apps_v1_client)
    mark_operator_ready()

    # Send Analytics Tracker
    # The information from this analytics is available for
    # developers to understand and build project in a better
    # way
    send_analytics_tracker("operator", uid)

    # Watch CRD
    crd_watch(
        core_v1_client,
        k8s_client,
        apps_v1_client,
        resource_version=storage_plan["resource_version"],
    )


def main():
    """Run the operator without preserving readiness across any exit."""
    clear_operator_ready()
    try:
        _run_operator()
    finally:
        clear_operator_ready()


if __name__ == "__main__":
    logging_setup()

    # This not advised in general, but in kadalu's operator, it is OK to
    # ignore these warnings as we know to make calls only inside of
    # kubernetes cluster
    urllib3.disable_warnings()

    main()
