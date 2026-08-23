"""
controller server implementation
"""
# pylint: disable=too-many-lines
import functools
import errno
import fcntl
import hashlib
import inspect
import json
import logging
import os
import random
import shlex
import threading
import time
from contextlib import contextmanager

import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import (PV_TYPE_RAWBLOCK, PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK,
                       CommandException, execute, is_valid_csi_identifier,
                       logf, reachable_host,
                       send_analytics_tracker, get_single_pv_per_pool)
from volumeutils import (HOSTVOL_MOUNTDIR, check_external_volume,
                         claim_single_pv_volume,
                         create_block_volume, create_subdir_volume,
                         delete_volume,
                         get_accounted_pv_size, get_pv_hosting_volumes,
                         is_hosting_volume_free,
                         mount_and_select_hosting_volume,
                         mount_single_pv_hosting_volume,
                         new_volume_hosting_candidates, search_volume,
                         finish_volume_expansion,
                         finish_volume_creation,
                         LegacySinglePVPoolUnclaimedError,
                         read_volume_creation_intent,
                         read_volume_expansion_intent,
                         reserve_volume_expansion,
                         SinglePVPoolClaimedError,
                         SinglePVPoolNotEmptyError,
                         SinglePVPoolRetiredError,
                         UnsupportedCapacityRangeError,
                         UnsupportedReclaimPolicyError,
                         VolumeOperationConflictError,
                         VolumeOperationLockTimeoutError,
                         save_pv_metadata,
                         single_pv_operation_lock,
                         unmount_glusterfs,
                         update_free_size, update_pv_metadata,
                         update_subdir_volume,
                         validate_committed_creation,
                         volume_operation_lock,
                         volume_incarnation,
                         yield_list_of_pvcs)

VOLINFO_DIR = "/var/lib/gluster"
KADALU_VERSION = os.environ.get("KADALU_VERSION", "latest")
DEFAULT_VOLUME_SIZE = 1024 * 1024 * 1024

# Generator to be used in ListVolumes
GEN = None

# Rate limiting number of PVCs returned per request of ListVolumes if CO
# doesn't mention any max_entries
LIMIT = 30

# Create, expand, and delete each perform filesystem work between checking and
# updating stat.db. Keep those operations in one process-wide critical section
# so concurrent CSI requests cannot consume the same free capacity.
CAPACITY_OPERATION_LOCK = threading.Lock()
CAPACITY_LOCK_WAIT_TIMEOUT_SECONDS = 1.0
CREATE_IDENTITY_LOCK_DIR = os.environ.get(
    "KADALU_CREATE_IDENTITY_LOCK_DIR",
    "/var/lib/kadalu/create-locks",
)


def serialized_capacity_operation(method):
    """Serialize controller operations which change pool capacity."""
    # Manual acquire/release is required so lock acquisition can honor the
    # gRPC deadline before entering the protected operation.
    # pylint: disable=consider-using-with
    @functools.wraps(method)
    def locked_method(*args, **kwargs):
        context = kwargs.get("context")
        if context is None and len(args) >= 3:
            context = args[2]

        remaining = None
        time_remaining = getattr(context, "time_remaining", None)
        if callable(time_remaining):
            remaining = time_remaining()

        if remaining is None:
            acquired = CAPACITY_OPERATION_LOCK.acquire(
                timeout=CAPACITY_LOCK_WAIT_TIMEOUT_SECONDS,
            )
        elif remaining <= 0:
            acquired = False
        else:
            acquired = CAPACITY_OPERATION_LOCK.acquire(
                timeout=min(
                    remaining,
                    CAPACITY_LOCK_WAIT_TIMEOUT_SECONDS,
                    threading.TIMEOUT_MAX,
                ),
            )

        if not acquired:
            errmsg = "Another capacity operation is still in progress"
            logging.warning(errmsg)
            abort = getattr(context, "abort", None)
            if callable(abort):
                abort(grpc.StatusCode.ABORTED, errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ABORTED)
            return None

        try:
            return method(*args, **kwargs)
        finally:
            CAPACITY_OPERATION_LOCK.release()

    return locked_method


def serialized_existing_volume_operation(
        response_type,
        request_is_ready=None,
        volume_id_attribute="volume_id",
        missing_is_conflict=False):
    """Serialize one existing volume lifecycle across controller replicas."""
    def decorate(method):
        @functools.wraps(method)
        def locked_method(self, request, context):
            volume_id = getattr(request, volume_id_attribute, "")
            if (
                    not is_valid_csi_identifier(volume_id)
                    or (
                        request_is_ready is not None
                        and not request_is_ready(request)
                    )):
                return method(self, request, context)

            volume = search_volume(volume_id)
            if volume is None:
                return method(self, request, context)
            hostvol_mnt = os.path.join(HOSTVOL_MOUNTDIR, volume.hostvol)
            observed_incarnation = volume_incarnation(volume)

            try:
                operation_lock = (
                    single_pv_operation_lock
                    if volume.single_pv_per_pool
                    else volume_operation_lock
                )
                lock_args = (
                    (hostvol_mnt,)
                    if volume.single_pv_per_pool
                    else (hostvol_mnt, volume_id)
                )
                with operation_lock(
                        *lock_args,
                        timeout=volume_operation_timeout(context)):
                    locked_volume = search_volume(volume_id)
                    if locked_volume is None and missing_is_conflict:
                        raise VolumeOperationConflictError(
                            "Volume changed while waiting for its lifecycle "
                            "lock"
                        )
                    if (
                            locked_volume is not None
                            and locked_volume.hostvol != volume.hostvol):
                        raise VolumeOperationConflictError(
                            "Volume hosting pool changed while waiting for "
                            "its lifecycle lock"
                        )
                    if (
                            locked_volume is not None
                            and volume_incarnation(locked_volume)
                            != observed_incarnation):
                        raise VolumeOperationConflictError(
                            "Volume was replaced while waiting for its "
                            "lifecycle lock"
                        )
                    return method(self, request, context)
            except (
                    VolumeOperationConflictError,
                    VolumeOperationLockTimeoutError,
            ) as err:
                logging.warning(str(err))
                context.set_details(str(err))
                context.set_code(grpc.StatusCode.ABORTED)
                return response_type()

        return locked_method
    return decorate


def valid_expansion_request_shape(request):
    """Return whether expansion input is ready for a storage lookup."""
    if not request.HasField("capacity_range"):
        return False
    required = request.capacity_range.required_bytes
    limit = request.capacity_range.limit_bytes
    return (
        required >= 0
        and limit >= 0
        and (not limit or required <= limit)
        and (required > 0 or limit > 0)
    )


def valid_creation_request_shape(request):
    """Return whether create input is ready for a storage lookup."""
    if not request.volume_capabilities:
        return False
    block_volume = (
        request.parameters.get("pv_type", "").lower() == "block"
        or is_block_request(request)
    )
    if block_volume:
        single_node_writer = getattr(
            csi_pb2.VolumeCapability.AccessMode,
            "SINGLE_NODE_WRITER",
        )
        if pvc_access_mode(request) != single_node_writer:
            return False
        if get_single_pv_per_pool(request.parameters):
            return False
    if not request.HasField("capacity_range"):
        return True
    required = request.capacity_range.required_bytes
    limit = request.capacity_range.limit_bytes
    return (
        required >= 0
        and limit >= 0
        and (not limit or required <= limit)
        and (required > 0 or limit > 0)
    )


def volume_operation_timeout(context):
    """Bound a lifecycle-lock wait by both policy and the RPC deadline."""
    remaining = None
    time_remaining = getattr(context, "time_remaining", None)
    if callable(time_remaining):
        remaining = time_remaining()
    if remaining is None:
        return CAPACITY_LOCK_WAIT_TIMEOUT_SECONDS
    return min(max(remaining, 0), CAPACITY_LOCK_WAIT_TIMEOUT_SECONDS)


@contextmanager
def volume_creation_identity_lock(volume_id, context):
    """Serialize one CSI name across processes in the controller pod."""
    os.makedirs(CREATE_IDENTITY_LOCK_DIR, mode=0o700, exist_ok=True)
    digest = hashlib.sha256(volume_id.encode("utf-8")).hexdigest()
    lock_path = os.path.join(CREATE_IDENTITY_LOCK_DIR, f"{digest}.lock")
    with open(lock_path, "a+b") as lock_file:
        deadline = time.monotonic() + volume_operation_timeout(context)
        while True:
            try:
                fcntl.flock(
                    lock_file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
                break
            except BlockingIOError as err:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise VolumeOperationLockTimeoutError(
                        "Another create with this volume ID is still in "
                        "progress"
                    ) from err
                time.sleep(min(0.05, remaining))
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def serialized_volume_creation_identity(response_type):
    """Serialize valid creates before searching the complete pool set.

    Kubernetes renders at most one serving controller pod, and Nomad's sample
    controller group has count one. The filesystem lock additionally orders
    multiple driver processes sharing that controller allocation.
    """
    def decorate(method):
        @functools.wraps(method)
        def locked_method(self, request, context):
            if (
                    not is_valid_csi_identifier(request.name)
                    or not valid_creation_request_shape(request)):
                return method(self, request, context)
            try:
                with volume_creation_identity_lock(request.name, context):
                    return method(self, request, context)
            except (
                    VolumeOperationConflictError,
                    VolumeOperationLockTimeoutError,
            ) as err:
                logging.warning(str(err))
                context.set_details(str(err))
                context.set_code(grpc.StatusCode.ABORTED)
                return response_type()

        return locked_method
    return decorate


def valid_volume_identifier(value, label, context):
    """Validate an opaque identifier against the CSI string contract."""
    if is_valid_csi_identifier(value):
        return True

    errmsg = f"{label} does not satisfy the CSI identifier contract"
    logging.error(errmsg)
    context.set_details(errmsg)
    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    return False


def requested_capacity(capacity, context, default_size=DEFAULT_VOLUME_SIZE):
    """Validate CSI CapacityRange values before any storage mutation."""
    if capacity is None:
        return default_size
    required = capacity.required_bytes
    limit = capacity.limit_bytes
    if required < 0 or limit < 0:
        errmsg = "Capacity range values must not be negative"
    elif limit and required > limit:
        errmsg = "Required bytes must not exceed limit bytes"
    elif required == 0 and limit == 0:
        errmsg = "Capacity range must specify required bytes or limit bytes"
    else:
        selected = required or limit
        if selected > 0:
            return selected
        errmsg = "Requested capacity must be greater than zero"

    logging.error(errmsg)
    context.set_details(errmsg)
    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    return None


def creation_capacity_call(method, *args, volume_id, pvtype):
    """Call a capacity helper with create identity when it supports it.

    The two-argument fallback preserves compatibility with injected legacy
    selectors while the in-tree helpers use the identity to make admission and
    the durable create reservation one cross-process transaction.
    """
    parameters = inspect.signature(method).parameters.values()
    supports_keywords = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )
    parameter_names = {parameter.name for parameter in parameters}
    if supports_keywords or {"volume_id", "pvtype"} <= parameter_names:
        return method(
            *args,
            volume_id=volume_id,
            pvtype=pvtype,
        )
    return method(*args)


def finish_committed_creation(volume):
    """Drop a leftover create intent once PV metadata is authoritative."""
    mntdir = os.path.join(HOSTVOL_MOUNTDIR, volume.hostvol)
    intent = read_volume_creation_intent(
        mntdir,
        volume.volname,
        volume.voltype,
        volume.volpath,
    )
    if intent is not None:
        validate_committed_creation(
            {
                "incarnation": volume.extra.get("incarnation"),
                "size": volume.size,
            },
            intent,
        )
        finish_volume_creation(
            mntdir,
            volume.volname,
            volume.voltype,
            volume.volpath,
            intent,
        )


def set_storage_error(context, err):
    """Translate storage I/O failures into retryable CSI status codes."""
    transient_errors = {
        errno.EIO,
        errno.ENOTCONN,
        errno.ESTALE,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
    }
    context.set_details(f"Storage operation failed: {err}")
    if isinstance(err, (
            VolumeOperationConflictError,
            VolumeOperationLockTimeoutError,
    )):
        status = grpc.StatusCode.ABORTED
    elif isinstance(err, CommandException):
        status = (
            grpc.StatusCode.DEADLINE_EXCEEDED
            if err.ret == 124
            else grpc.StatusCode.UNAVAILABLE
        )
    elif isinstance(err, OSError) and err.errno in transient_errors:
        status = grpc.StatusCode.UNAVAILABLE
    else:
        status = grpc.StatusCode.INTERNAL
    context.set_code(status)


def translate_storage_errors(response_type):
    """Turn unexpected backend failures into retry-safe CSI responses."""
    def decorate(method):
        @functools.wraps(method)
        def translated(self, request, context):
            try:
                return method(self, request, context)
            except (CommandException, OSError, ValueError) as err:
                logging.exception("CSI storage operation failed")
                set_storage_error(context, err)
                return response_type()

        return translated
    return decorate


# noqa # pylint: disable=too-many-arguments,too-many-positional-arguments
def execute_gluster_quota_command(privkey, user, host, gvolname, path, size):
    """
    Function to execute the GlusterFS's quota command on external cluster
    """
    # 'size' can always be parsed as integer with no errors
    size = int(size) * 0.95

    host = reachable_host(host)
    if host is None:
        errmsg = "All hosts are not reachable"
        logging.error(logf(errmsg))
        raise CommandException(-1, "reachable_host", errmsg)

    remote_command = shlex.join([
        "sudo",
        "gluster",
        "volume",
        "quota",
        str(gvolname),
        "limit-usage",
        "/%s" % path,
        "%s" % size,
    ])
    quota_cmd = [
        "ssh",
        "-oStrictHostKeyChecking=no",
        "-i",
        "%s" % privkey,
        "%s@%s" % (user, host),
        remote_command,
    ]
    try:
        execute(*quota_cmd)
    except CommandException as err:
        errmsg = "Unable to set Gluster Quota via ssh"
        logging.error(logf(errmsg, error=err))
        raise

# Assuming multiple volume_capabilities isn't requested
def is_block_request(request):
    """Returns True if the PVC requests rawblock"""
    for vol_capability in request.volume_capabilities:
        if vol_capability.WhichOneof("access_type") == "block":
            return True
    return False

# Assuming multiple volume_capabilities isn't requested
def pvc_access_mode(request):
    """Fetch Access modes from Volume capabilities"""
    for vol_capability in request.volume_capabilities:
        # TODO: A PVC can be asked with multiple access modes
        return vol_capability.access_mode.mode


def existing_volume_response(volume):
    """Build the original CSI response for an idempotent create retry."""
    volume_context = {
        "type": volume.extra['hostvoltype'],
        "hostvol": volume.hostvol,
        "pvtype": volume.voltype,
        "fstype": "xfs",
        "single_pv_per_pool": f"{volume.single_pv_per_pool}",
    }
    if not volume.single_pv_per_pool:
        volume_context["path"] = volume.volpath
    elif volume.extra.get("single_pv_claim_version") is not None:
        volume_context["single_pv_claim_version"] = str(
            volume.extra["single_pv_claim_version"]
        )
    if volume.extra['hostvoltype'] == "External":
        volume_context.update({
            "gvolname": volume.extra['gvolname'],
            "gserver": volume.extra['ghost'],
            "options": volume.extra['goptions'],
        })

    return csi_pb2.CreateVolumeResponse(volume={
        "volume_id": volume.volname,
        "capacity_bytes": volume.size,
        "volume_context": volume_context,
    })


def existing_volume_is_compatible(volume, request, pvtype):
    """Check whether an existing name can satisfy a CreateVolume retry."""
    capacity = request.capacity_range
    if volume.size < capacity.required_bytes:
        return False
    if capacity.limit_bytes and volume.size > capacity.limit_bytes:
        return False
    if volume.voltype != pvtype:
        return False

    parameters = request.parameters
    storage_name = parameters.get("storage_name")
    if storage_name and storage_name != volume.hostvol:
        return False

    storage_type = parameters.get(
        "storage_type",
        parameters.get("hostvol_type"),
    )
    if storage_type and storage_type != volume.extra['hostvoltype']:
        return False

    node_affinity = parameters.get("node_affinity")
    if node_affinity and node_affinity != volume.extra['node_affinity']:
        return False

    if volume.extra['hostvoltype'] == "External":
        if parameters.get("gluster_hosts") != volume.extra['ghost']:
            return False
        if parameters.get("gluster_volname") != volume.extra['gvolname']:
            return False
        if parameters.get("gluster_options", "") != volume.extra['goptions']:
            return False

    return (
        get_single_pv_per_pool(parameters) == volume.single_pv_per_pool
    )


def apply_subdir_quota(volume, size, context, update_metadata):
    """Apply a subvolume quota, optionally committing its new metadata."""
    mntdir = os.path.join(HOSTVOL_MOUNTDIR, volume.hostvol)
    use_gluster_quota = (
        volume.extra['hostvoltype'] == "External"
        and os.path.isfile("/etc/secret-volume/ssh-privatekey")
        and "SECRET_GLUSTERQUOTA_SSH_USERNAME" in os.environ
    )

    if not use_gluster_quota:
        update_subdir_volume(
            mntdir,
            volume.extra['hostvoltype'],
            volume.volname,
            size,
            update_metadata=update_metadata,
        )
        return True

    try:
        execute_gluster_quota_command(
            "/etc/secret-volume/ssh-privatekey",
            os.environ.get('SECRET_GLUSTERQUOTA_SSH_USERNAME'),
            volume.extra['ghost'],
            volume.extra['gvolname'],
            volume.volpath,
            size,
        )
    except CommandException as err:
        set_storage_error(context, err)
        return False

    if update_metadata:
        update_pv_metadata(mntdir, volume.volpath, size)

    return True


def reconcile_committed_capacity(hostvol, pvname, size):
    """Make stat.db match durable metadata without a free-space admission."""
    accounted_size = get_accounted_pv_size(hostvol, pvname)
    if accounted_size != size:
        logging.warning(logf(
            "Repairing PV capacity accounting",
            hostvol=hostvol,
            pvname=pvname,
            accounted_size=accounted_size,
            committed_size=size,
        ))
        update_free_size(hostvol, pvname, -size)


def verify_creation_reservation(hostvol, volume_id, pvtype, size):
    """Require the exact admitted create before mutating pool payload."""
    reserved = search_volume(volume_id)
    if reserved is None:
        raise VolumeOperationConflictError(
            "Creation reservation disappeared before mutation"
        )
    if (
            reserved.hostvol != hostvol
            or reserved.voltype != pvtype
            or reserved.size != size
            or reserved.extra.get("state") not in (None, "creating")):
        raise VolumeOperationConflictError(
            "Creation reservation changed before payload mutation"
        )
    return reserved


class ControllerServer(csi_pb2_grpc.ControllerServicer):
    """
    ControllerServer object is responsible for handling host
    volume mount and PV creation.
    Ref:https://github.com/container-storage-interface/spec/blob/master/spec.md
    """

    # noqa # pylint: disable=too-many-locals,too-many-statements,too-many-branches
    @serialized_capacity_operation
    @translate_storage_errors(csi_pb2.CreateVolumeResponse)
    @serialized_volume_creation_identity(csi_pb2.CreateVolumeResponse)
    @serialized_existing_volume_operation(
        csi_pb2.CreateVolumeResponse,
        valid_creation_request_shape,
        volume_id_attribute="name",
        missing_is_conflict=True,
    )
    def CreateVolume(self, request, context):
        start_time = time.time()
        logging.debug(logf(
            "Create Volume request",
            request=request
        ))

        if not valid_volume_identifier(request.name, "Volume name", context):
            return csi_pb2.CreateVolumeResponse()

        if not request.volume_capabilities:
            errmsg = "Volume Capabilities is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.CreateVolumeResponse()

        capacity = (
            request.capacity_range
            if request.HasField("capacity_range")
            else None
        )
        pvsize = requested_capacity(capacity, context)
        if pvsize is None:
            return csi_pb2.CreateVolumeResponse()

        pvtype = PV_TYPE_SUBVOL
        is_block = False

        # Mounted BlockVolume is requested via Storage Class.
        # GlusterFS File Volume may not be useful for some workloads
        # they can request for the Virtual Block formated and mounted
        # as default MountVolume.
        if request.parameters.get("pv_type", "").lower() == "block":
            pvtype = PV_TYPE_VIRTBLOCK
            is_block = True

        # RawBlock volume is requested via PVC
        if is_block_request(request):
            pvtype = PV_TYPE_RAWBLOCK
            is_block = True

        if is_block:
            single_node_writer = getattr(csi_pb2.VolumeCapability.AccessMode,
                                         "SINGLE_NODE_WRITER")

            # Multi node writer is not allowed for PV_TYPE_VIRTBLOCK/PV_TYPE_RAWBLOCK
            if pvc_access_mode(request) != single_node_writer:
                errmsg = "Only SINGLE_NODE_WRITER is allowed for block Volume"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                return csi_pb2.CreateVolumeResponse()

        single_pv_per_pool = get_single_pv_per_pool(request.parameters)
        if single_pv_per_pool and pvtype != PV_TYPE_SUBVOL:
            errmsg = "single_pv_per_pool supports filesystem volumes only"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.CreateVolumeResponse()

        # CreateVolume is idempotent: retrying a compatible request must return
        # the original volume without selecting another pool or charging space
        # a second time.
        try:
            volume = search_volume(request.name)
        except (CommandException, OSError, ValueError) as err:
            set_storage_error(context, err)
            return csi_pb2.CreateVolumeResponse()
        pending_creation = None
        if volume and volume.extra.get("state") == "creating":
            if not existing_volume_is_compatible(volume, request, pvtype):
                errmsg = "A pending create with this name is incompatible"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                return csi_pb2.CreateVolumeResponse()
            pending_creation = volume
            # The durable intent selected the exact size on the original call.
            # A compatible CapacityRange retry resumes that same allocation.
            pvsize = volume.size
        elif volume:
            if volume.extra.get("state") in (
                    "archiving", "deleting", "reclaiming"):
                errmsg = "Volume deletion is still in progress"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.CreateVolumeResponse()
            if (
                    volume.single_pv_per_pool
                    and volume.extra.get("single_pv_claim_state") != "active"):
                errmsg = "Single-PV pool is being deleted or is retired"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.CreateVolumeResponse()
            if existing_volume_is_compatible(volume, request, pvtype):
                # A prior request can create the PV metadata and then fail
                # while updating stat.db. Reconcile the absolute-size record
                # on retries; update_pv_record uses INSERT OR REPLACE, so this
                # repairs interrupted creates without adding the size twice.
                if not volume.single_pv_per_pool:
                    finish_committed_creation(volume)
                    reconcile_committed_capacity(
                        volume.hostvol,
                        volume.volname,
                        volume.size,
                    )
                return existing_volume_response(volume)

            errmsg = "A volume with this name already exists incompatibly"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            return csi_pb2.CreateVolumeResponse()

        logging.debug(logf(
            "Found PV type",
            pvtype=pvtype,
            capabilities=request.volume_capabilities
        ))

        # TODO: Check the available space under lock

        # Add everything from parameter as filter item
        filters = {}
        for pkey, pvalue in request.parameters.items():
            filters[pkey] = pvalue

        logging.debug(logf(
            "Filters applied to choose storage",
            **filters
        ))

        # UID is stored at the time of installation in configmap.
        uid = None
        with open(os.path.join(VOLINFO_DIR, "uid")) as uid_file:
            uid = uid_file.read()

        host_volumes = get_pv_hosting_volumes(filters)
        if pending_creation is None:
            host_volumes = new_volume_hosting_candidates(host_volumes)
        logging.debug(logf(
            "Got list of hosting Volumes",
            volumes=",".join(v['name'] for v in host_volumes)
        ))
        hostvol = (
            pending_creation.hostvol
            if pending_creation is not None
            else None
        )
        ext_volume = None
        data = {}
        hostvoltype = (
            pending_creation.extra.get("hostvoltype")
            if pending_creation is not None
            else filters.get("hostvol_type", None)
        )
        if pending_creation is not None and not any(
                item["name"] == pending_creation.hostvol
                for item in host_volumes):
            errmsg = "Pending create belongs to an incompatible storage pool"
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            return csi_pb2.CreateVolumeResponse()
        if not hostvoltype:
            # This means, the request came on 'kadalu' storage class type.

            # Randomize the entries so we can issue PV from different storage
            random.shuffle(host_volumes)

            try:
                if single_pv_per_pool:
                    hostvol, pvsize = mount_single_pv_hosting_volume(
                        host_volumes,
                        pvsize,
                        request.capacity_range.limit_bytes,
                    )
                else:
                    hostvol = creation_capacity_call(
                        mount_and_select_hosting_volume,
                        host_volumes,
                        pvsize,
                        volume_id=request.name,
                        pvtype=pvtype,
                    )
            except UnsupportedCapacityRangeError as err:
                context.set_details(str(err))
                context.set_code(grpc.StatusCode.OUT_OF_RANGE)
                return csi_pb2.CreateVolumeResponse()
            except LegacySinglePVPoolUnclaimedError as err:
                context.set_details(str(err))
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.CreateVolumeResponse()
            except (CommandException, OSError, ValueError) as err:
                set_storage_error(context, err)
                return csi_pb2.CreateVolumeResponse()
            if hostvol is None:
                errmsg = "No Hosting Volumes available, add more storage"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                return csi_pb2.CreateVolumeResponse()

            info_file_path = os.path.join(VOLINFO_DIR, "%s.info" % hostvol)
            with open(info_file_path) as info_file:
                data = json.load(info_file)

            hostvoltype = data['type']

        if hostvoltype == 'External':
            external_candidates = host_volumes
            if pending_creation is not None:
                # The durable intent already selected this exact pool. Restrict
                # discovery before it mounts anything so a retry can never move
                # an interrupted create to an equivalent external pool.
                external_candidates = [
                    item for item in host_volumes
                    if item["name"] == pending_creation.hostvol
                ]
            ext_volume = check_external_volume(request, external_candidates)

            if (
                    pending_creation is not None
                    and ext_volume is not None
                    and ext_volume["name"] != pending_creation.hostvol):
                errmsg = "Pending create cannot switch external storage pools"
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.CreateVolumeResponse()

            if ext_volume:
                mntdir = os.path.join(HOSTVOL_MOUNTDIR, ext_volume['name'])

                # By default 'single_pv_per_pool' is set to 'False' as part of CRD
                # definition
                if single_pv_per_pool:
                    # If 'single_pv_per_pool' is True, the request will be
                    # considered as to map 1 PV to 1 Gluster volume

                    try:
                        _, pvsize = mount_single_pv_hosting_volume(
                            [ext_volume],
                            pvsize,
                            (
                                request.capacity_range.limit_bytes
                                if request.HasField("capacity_range")
                                else 0
                            ),
                        )
                        claim_single_pv_volume(
                            mntdir,
                            request.name,
                            pvsize,
                        )
                    except UnsupportedCapacityRangeError as err:
                        context.set_details(str(err))
                        context.set_code(grpc.StatusCode.OUT_OF_RANGE)
                        return csi_pb2.CreateVolumeResponse()
                    except LegacySinglePVPoolUnclaimedError as err:
                        context.set_details(str(err))
                        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                        return csi_pb2.CreateVolumeResponse()
                    except SinglePVPoolClaimedError as err:
                        errmsg = (
                            "Single-PV pool already belongs to volume "
                            f"{err}"
                        )
                        context.set_details(errmsg)
                        context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                        return csi_pb2.CreateVolumeResponse()
                    except SinglePVPoolRetiredError:
                        errmsg = (
                            "Deleted single-PV pool requires administrator "
                            "reset before it can be reused"
                        )
                        context.set_details(errmsg)
                        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                        return csi_pb2.CreateVolumeResponse()
                    except SinglePVPoolNotEmptyError as err:
                        context.set_details(str(err))
                        context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                        return csi_pb2.CreateVolumeResponse()
                    except (OSError, ValueError) as err:
                        set_storage_error(context, err)
                        return csi_pb2.CreateVolumeResponse()
                    finally:
                        # No need to keep the dedicated mount on controller.
                        unmount_glusterfs(mntdir, ext_volume['g_volname'])

                    logging.info(logf(
                        "Volume (External) created",
                        name=request.name,
                        size=pvsize,
                        mount=mntdir,
                        hostvol=ext_volume['g_volname'],
                        pvtype=pvtype,
                        volpath=ext_volume['g_host'],
                        duration_seconds=time.time() - start_time
                    ))

                    send_analytics_tracker("pvc-external", uid)
                    return csi_pb2.CreateVolumeResponse(
                        volume={
                            "volume_id": request.name,
                            "capacity_bytes": pvsize,
                            "volume_context": {
                                "type": hostvoltype,
                                "hostvol": ext_volume['name'],
                                "pvtype": pvtype,
                                "gvolname": ext_volume['g_volname'],
                                "gserver": ext_volume['g_host'],
                                "fstype": "xfs",
                                "options": ext_volume['g_options'],
                                "single_pv_per_pool": f"{single_pv_per_pool}",
                                "single_pv_claim_version": "1",
                            }
                        }
                    )

                # The external volume should be used as kadalu host vol

                if (
                        pending_creation is None
                        and not creation_capacity_call(
                            is_hosting_volume_free,
                            ext_volume['name'],
                            pvsize,
                            volume_id=request.name,
                            pvtype=pvtype,
                        )):

                    logging.error(logf(
                        "Hosting volume is full. Add more storage",
                        volume=ext_volume['name']
                    ))
                    errmsg = "External resource is exhausted"
                    context.set_details(errmsg)
                    context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                    return csi_pb2.CreateVolumeResponse()

                with volume_operation_lock(
                        mntdir,
                        request.name,
                        timeout=volume_operation_timeout(context)):
                    verify_creation_reservation(
                        ext_volume['name'],
                        request.name,
                        pvtype,
                        pvsize,
                    )
                    if pvtype in [PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK]:
                        vol = create_block_volume(
                            pvtype, mntdir, request.name, pvsize)
                    else:
                        use_gluster_quota = False
                        if (
                                os.path.isfile(
                                    "/etc/secret-volume/ssh-privatekey"
                                )
                                and "SECRET_GLUSTERQUOTA_SSH_USERNAME"
                                in os.environ):
                            use_gluster_quota = True
                        secret_private_key = (
                            "/etc/secret-volume/ssh-privatekey"
                        )
                        secret_username = os.environ.get(
                            'SECRET_GLUSTERQUOTA_SSH_USERNAME',
                            None,
                        )
                        hostname = filters.get("gluster_hosts", None)
                        gluster_vol_name = filters.get(
                            "gluster_volname",
                            None,
                        )
                        vol = create_subdir_volume(
                            mntdir,
                            request.name,
                            pvsize,
                            use_gluster_quota,
                            save_metadata=not use_gluster_quota,
                        )
                        quota_size = pvsize
                        quota_path = vol.volpath
                        if use_gluster_quota is False:
                            logging.debug(logf(
                                "Set Quota in the native way"
                            ))
                        else:
                            logging.debug(logf(
                                "Set Quota using gluster directory Quota"
                            ))
                            execute_gluster_quota_command(
                                secret_private_key,
                                secret_username,
                                hostname,
                                gluster_vol_name,
                                quota_path,
                                quota_size,
                            )
                            creation_intent = read_volume_creation_intent(
                                mntdir,
                                vol.volname,
                                vol.voltype,
                                vol.volpath,
                            )
                            if creation_intent is None:
                                raise VolumeOperationConflictError(
                                    "Creation reservation disappeared before "
                                    "metadata commit"
                                )
                            save_pv_metadata(
                                mntdir,
                                vol.volpath,
                                pvsize,
                                incarnation=creation_intent.get(
                                    "incarnation"
                                ),
                            )
                            finish_volume_creation(
                                mntdir,
                                vol.volname,
                                vol.voltype,
                                vol.volpath,
                                creation_intent,
                            )

                    # Keep metadata and its absolute accounting record under
                    # the same lifecycle lock as payload creation. A delete
                    # can never finish between these two commits.
                    update_free_size(
                        ext_volume['name'],
                        request.name,
                        -pvsize,
                    )

                logging.info(logf(
                    "Volume created",
                    name=request.name,
                    size=pvsize,
                    hostvol=ext_volume['name'],
                    pvtype=pvtype,
                    volpath=vol.volpath,
                    duration_seconds=time.time() - start_time
                ))

                send_analytics_tracker("pvc-external-kadalu", uid)
                # Pass required argument to get mount working on
                # nodeplugin through volume_context
                return csi_pb2.CreateVolumeResponse(
                    volume={
                        "volume_id": request.name,
                        "capacity_bytes": pvsize,
                        "volume_context": {
                            "type": hostvoltype,
                            "hostvol": ext_volume['name'],
                            "pvtype": pvtype,
                            "path": vol.volpath,
                            "gvolname": ext_volume['g_volname'],
                            "gserver": ext_volume['g_host'],
                            "fstype": "xfs",
                            "options": ext_volume['g_options'],
                            "single_pv_per_pool": f"{single_pv_per_pool}",
                        }
                    }
                )

            # If external volume not found
            logging.debug(logf(
                "Here as checking external volume failed",
                external_volume=ext_volume
            ))
            errmsg = "External Storage provided not valid"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.CreateVolumeResponse()

        if not hostvol:
            # Randomize the entries so we can issue PV from different storage
            random.shuffle(host_volumes)

            hostvol = creation_capacity_call(
                mount_and_select_hosting_volume,
                host_volumes,
                pvsize,
                volume_id=request.name,
                pvtype=pvtype,
            )
            if hostvol is None:
                errmsg = "No Hosting Volumes available, add more storage"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                return csi_pb2.CreateVolumeResponse()

        if single_pv_per_pool:
            # Then mount the whole volume as PV
            msg = "non-native way of Kadalu mount expected"
            logging.info(msg)
            try:
                claim_single_pv_volume(
                    os.path.join(HOSTVOL_MOUNTDIR, hostvol),
                    request.name,
                    pvsize,
                )
            except SinglePVPoolClaimedError as err:
                errmsg = (
                    "Single-PV pool already belongs to volume "
                    f"{err}"
                )
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                return csi_pb2.CreateVolumeResponse()
            except SinglePVPoolRetiredError:
                errmsg = (
                    "Deleted single-PV pool requires administrator reset "
                    "before it can be reused"
                )
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.CreateVolumeResponse()
            except SinglePVPoolNotEmptyError as err:
                context.set_details(str(err))
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.CreateVolumeResponse()
            except (OSError, ValueError) as err:
                set_storage_error(context, err)
                return csi_pb2.CreateVolumeResponse()
            return csi_pb2.CreateVolumeResponse(
                volume={
                    "volume_id": request.name,
                    "capacity_bytes": pvsize,
                    "volume_context": {
                        "type": hostvoltype,
                        "hostvol": hostvol,
                        "pvtype": pvtype,
                        "fstype": "xfs",
                            "single_pv_per_pool": f"{single_pv_per_pool}",
                    }
                }
            )

        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
        with volume_operation_lock(
                mntdir,
                request.name,
                timeout=volume_operation_timeout(context)):
            verify_creation_reservation(
                hostvol,
                request.name,
                pvtype,
                pvsize,
            )
            if pvtype in [PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK]:
                vol = create_block_volume(
                    pvtype, mntdir, request.name, pvsize)
            else:
                use_gluster_quota = False
                vol = create_subdir_volume(
                    mntdir, request.name, pvsize, use_gluster_quota)

            # Keep metadata and its absolute accounting record under the same
            # lifecycle lock as payload creation and quota initialization.
            update_free_size(hostvol, request.name, -pvsize)
        logging.info(logf(
            "Volume created",
            name=request.name,
            size=pvsize,
            hostvol=hostvol,
            pvtype=pvtype,
            volpath=vol.volpath,
            duration_seconds=time.time() - start_time
        ))

        send_analytics_tracker("pvc-%s" % hostvoltype, uid)

        return csi_pb2.CreateVolumeResponse(
            volume={
                "volume_id": request.name,
                "capacity_bytes": pvsize,
                "volume_context": {
                    "type": hostvoltype,
                    "hostvol": hostvol,
                    "pvtype": pvtype,
                    "path": vol.volpath,
                    "fstype": "xfs",
                    "single_pv_per_pool": f"{single_pv_per_pool}"
                }
            })

    @serialized_capacity_operation
    @translate_storage_errors(csi_pb2.DeleteVolumeResponse)
    def DeleteVolume(self, request, context):
        start_time = time.time()

        if not valid_volume_identifier(request.volume_id, "Volume ID", context):
            return csi_pb2.DeleteVolumeResponse()

        try:
            delete_volume(
                request.volume_id,
                lock_timeout=volume_operation_timeout(context),
            )
        except UnsupportedReclaimPolicyError as err:
            context.set_details(str(err))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return csi_pb2.DeleteVolumeResponse()
        except CommandException as err:
            context.set_details(f"Storage operation failed: {err}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return csi_pb2.DeleteVolumeResponse()
        except (OSError, ValueError) as err:
            set_storage_error(context, err)
            return csi_pb2.DeleteVolumeResponse()
        logging.info(logf(
            "Delete Volume response completed",
            name=request.volume_id,
            duration_seconds=time.time() - start_time
        ))
        return csi_pb2.DeleteVolumeResponse()

    @translate_storage_errors(csi_pb2.ValidateVolumeCapabilitiesResponse)
    def ValidateVolumeCapabilities(self, request, context):

        if not valid_volume_identifier(request.volume_id, "Volume ID", context):
            return csi_pb2.ValidateVolumeCapabilitiesResponse()

        volume = search_volume(request.volume_id)
        if not volume or volume.extra.get("state") == "creating":
            errmsg = "Requested volume does not exist"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return csi_pb2.ValidateVolumeCapabilitiesResponse()

        if not request.volume_capabilities:
            errmsg = "Volume Capabilities is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.ValidateVolumeCapabilitiesResponse()

        volume_id = request.volume_id
        volume_capabilities = request.volume_capabilities

        logging.info(logf(
            "Validating Volume capabilities for volume",
            volume_id=volume_id,
            volume_capabilities=volume_capabilities
        ))

        single_node_writer = getattr(csi_pb2.VolumeCapability.AccessMode,
                                     "SINGLE_NODE_WRITER")

        multi_node_multi_writer = getattr(csi_pb2.VolumeCapability.AccessMode,
                                          "MULTI_NODE_MULTI_WRITER")

        modes = [single_node_writer, multi_node_multi_writer]

        for volume_capability in volume_capabilities:
            if volume_capability.access_mode.mode not in modes:

                errmsg = "Requested volume capability not supported"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                return csi_pb2.ValidateVolumeCapabilitiesResponse()

        return csi_pb2.ValidateVolumeCapabilitiesResponse(
            confirmed={
                "volume_capabilities": volume_capabilities,
            }
        )



    @translate_storage_errors(csi_pb2.ListVolumesResponse)
    def ListVolumes(self, request, context):
        """Returns list of all PVCs with sizes existing in Kadalu Storage"""

        logging.debug(logf("ListVolumes request received", request=request))
        global GEN
        # Need to check for no hostvol creation only once
        if GEN is None:
            # Handle no hostvol creation, with ~10s timeout
            volumes = get_pv_hosting_volumes(iteration=3)
            if not volumes:
                errmsg = "No PV hosting volume is created yet"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.ABORTED)
                return csi_pb2.ListVolumesResponse()

        starting_token = request.starting_token or '0'
        try:
            starting_token = int(starting_token)
        except ValueError as errmsg:
            # We are using tokens which can be converted to integer's
            errmsg = "Invalid starting token supplied"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ABORTED)
            return csi_pb2.ListVolumesResponse()

        if not request.starting_token:
            # This is the first call and so start the generator
            max_entries = request.max_entries or 0
            if not max_entries:
                # In worst case limit ourselves with custom max_entries and
                # set next_token
                max_entries = LIMIT
            GEN = yield_list_of_pvcs(max_entries)

        # Run and wait for 'send'
        try:
            next(GEN)
        except StopIteration as errmsg:
            # Handle no PVC created from a storage volume yet
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ABORTED)
            return csi_pb2.ListVolumesResponse()

        try:
            # Get list of PVCs limited at max_entries by suppling the token
            pvcs, next_token = GEN.send(starting_token)
        except StopIteration as errmsg:
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ABORTED)
            return csi_pb2.ListVolumesResponse()

        entries = [{
            "volume": {
                "volume_id": value.get("name"),
                "capacity_bytes": value.get("size"),
            }
        } for value in pvcs if value is not None]

        return csi_pb2.ListVolumesResponse(entries=entries,
                                           next_token=next_token)

    def ControllerGetCapabilities(self, request, context):
        # using getattr to avoid Pylint error
        capability_type = getattr(
            csi_pb2.ControllerServiceCapability.RPC, "Type").Value

        return csi_pb2.ControllerGetCapabilitiesResponse(
            capabilities=[
                {
                    "rpc": {
                        "type": capability_type("CREATE_DELETE_VOLUME")
                    }
                },
                {
                    "rpc": {
                        "type": capability_type("LIST_VOLUMES")
                    }
                },
                {
                    "rpc": {
                        "type": capability_type("EXPAND_VOLUME")
                    }
                }
            ]
        )

    @serialized_capacity_operation
    @translate_storage_errors(csi_pb2.ControllerExpandVolumeResponse)
    @serialized_existing_volume_operation(
        csi_pb2.ControllerExpandVolumeResponse,
        valid_expansion_request_shape,
    )
    def ControllerExpandVolume(self, request, context):
        """
        Controller plugin RPC call implementation of EXPAND_VOLUME
        """

        start_time = time.time()
        logging.debug(logf(
            "Expand Volume request",
            request=request
        ))

        if not valid_volume_identifier(request.volume_id, "Volume ID", context):
            return csi_pb2.ControllerExpandVolumeResponse()

        if not request.HasField("capacity_range"):
            errmsg = "Capacity Range is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.ControllerExpandVolumeResponse()

        expansion_requested_pvsize = requested_capacity(
            request.capacity_range,
            context,
        )
        if expansion_requested_pvsize is None:
            return csi_pb2.ControllerExpandVolumeResponse()

        # Get existing volume
        try:
            existing_volume = search_volume(request.volume_id)
        except (CommandException, OSError, ValueError) as err:
            set_storage_error(context, err)
            return csi_pb2.ControllerExpandVolumeResponse()
        if not existing_volume:
            errmsg = logf(
                "Unable to find volume",
                volume_id=request.volume_id
            )
            logging.error(errmsg)
            context.set_details(str(errmsg))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return csi_pb2.ControllerExpandVolumeResponse()
        volume_state = existing_volume.extra.get("state")
        if volume_state is not None:
            errmsg = (
                "Requested volume has an incomplete lifecycle transition: "
                f"{volume_state}"
            )
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return csi_pb2.ControllerExpandVolumeResponse()
        if (
                existing_volume.single_pv_per_pool
                and existing_volume.extra.get("single_pv_claim_state")
                != "active"):
            errmsg = "Single-PV pool is being deleted or is retired"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return csi_pb2.ControllerExpandVolumeResponse()

        existing_pvsize = existing_volume.size
        if (
                not isinstance(existing_pvsize, int)
                or isinstance(existing_pvsize, bool)
                or existing_pvsize <= 0):
            set_storage_error(
                context,
                ValueError("Volume metadata contains an invalid size"),
            )
            return csi_pb2.ControllerExpandVolumeResponse()
        if (
                request.capacity_range.limit_bytes
                and existing_pvsize > request.capacity_range.limit_bytes):
            errmsg = "Existing volume exceeds the requested capacity limit"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            return csi_pb2.ControllerExpandVolumeResponse(
                capacity_bytes=int(existing_pvsize),
                node_expansion_required=False,
            )
        pvname = existing_volume.volname
        hostvol = existing_volume.hostvol
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)

        # Expansion is idempotent and must never shrink an existing volume.
        # A stale smaller request still reconciles the existing absolute size,
        # repairing any interrupted quota or accounting update without using
        # the stale requested size.
        if (
                expansion_requested_pvsize > existing_pvsize
                and existing_volume.single_pv_per_pool):
            errmsg = "PV with single_pv_per_pool doesn't support expansion"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return csi_pb2.ControllerExpandVolumeResponse(
                capacity_bytes=int(existing_pvsize),
                node_expansion_required=False,
            )

        if (
                expansion_requested_pvsize > existing_pvsize
                and existing_volume.voltype in (
                    PV_TYPE_VIRTBLOCK,
                    PV_TYPE_RAWBLOCK,
                )):
            errmsg = (
                "Block volume expansion is unavailable until node-side "
                "loop-device and filesystem growth is implemented"
            )
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return csi_pb2.ControllerExpandVolumeResponse(
                capacity_bytes=int(existing_pvsize),
                node_expansion_required=False,
            )

        pending_expansion = None
        if (
                not existing_volume.single_pv_per_pool
                and existing_volume.voltype == PV_TYPE_SUBVOL):
            pending_expansion = read_volume_expansion_intent(
                mntdir,
                pvname,
                existing_volume.voltype,
                existing_volume.volpath,
            )
            if pending_expansion is not None and (
                    expansion_requested_pvsize
                    != pending_expansion["to_size"]):
                errmsg = "Request does not match the pending expansion target"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.ControllerExpandVolumeResponse(
                    capacity_bytes=int(existing_pvsize),
                    node_expansion_required=False,
                )
            if (
                    pending_expansion is not None
                    and existing_pvsize not in (
                        pending_expansion["from_size"],
                        pending_expansion["to_size"],
                    )):
                raise ValueError(
                    "Pending expansion does not match committed metadata"
                )

        if not existing_volume.single_pv_per_pool:
            # Metadata records the committed allocation. stat.db is a
            # rebuildable index and must be repaired even if the pool is now
            # overcommitted, otherwise subsequent admission sees false space.
            reconcile_committed_capacity(
                hostvol,
                pvname,
                (
                    pending_expansion["to_size"]
                    if pending_expansion is not None
                    else existing_pvsize
                ),
            )

        if (
                pending_expansion is None
                and expansion_requested_pvsize <= existing_pvsize):
            # Metadata is committed only after quota, but the process can still
            # fail before stat.db is updated. Reapplying an absolute quota and
            # accounting record makes a retry repair either legacy
            # ordering or that interrupted final step.
            if (
                    not existing_volume.single_pv_per_pool
                    and existing_volume.voltype == PV_TYPE_SUBVOL):
                if not apply_subdir_quota(
                        existing_volume,
                        existing_pvsize,
                        context,
                        update_metadata=False):
                    return csi_pb2.ControllerExpandVolumeResponse(
                        capacity_bytes=int(existing_pvsize),
                        node_expansion_required=False,
                    )
            logging.info(logf(
                "Reconciled volume at requested capacity",
                size=existing_pvsize,
                volume=pvname,
            ))
            return csi_pb2.ControllerExpandVolumeResponse(
                capacity_bytes=int(existing_pvsize),
                node_expansion_required=False,
            )

        target_size = (
            pending_expansion["to_size"]
            if pending_expansion is not None
            else expansion_requested_pvsize
        )
        from_size = (
            pending_expansion["from_size"]
            if pending_expansion is not None
            else existing_pvsize
        )
        additional_pvsize_required = target_size - from_size

        logging.info(logf(
            "Existing PV size and Expansion requested PV size",
            existing_pvsize=existing_pvsize,
            expansion_requested_pvsize=target_size,
            additional_size_required=additional_pvsize_required
        ))

        # reuse the data that was set while creating a volume
        pvtype = existing_volume.voltype

        logging.debug(logf(
            "Found PV type",
            pvtype=pvtype,
            capability=request.volume_capability
        ))

        pending_expansion = reserve_volume_expansion(
            hostvol,
            pvname,
            (
                existing_volume.voltype,
                existing_volume.volpath,
                from_size,
                target_size,
            ),
        )
        if pending_expansion is None:
            context.set_details("Host volume resource is exhausted")
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            logging.error(logf(
                "Hosting volume is full. Add more storage",
                volume=hostvol
            ))
            return csi_pb2.ControllerExpandVolumeResponse(
                capacity_bytes=int(existing_pvsize),
                node_expansion_required=False,
            )

        if not apply_subdir_quota(
                existing_volume,
                target_size,
                context,
                update_metadata=True):
            return csi_pb2.ControllerExpandVolumeResponse(
                capacity_bytes=int(existing_pvsize),
                node_expansion_required=False,
            )

        logging.info(logf(
            "Volume expanded",
            name=pvname,
            size=target_size,
            hostvol=hostvol,
            pvtype=pvtype,
            volpath=existing_volume.volpath,
            duration_seconds=time.time() - start_time
        ))

        finish_volume_expansion(
            mntdir,
            pvname,
            existing_volume.voltype,
            existing_volume.volpath,
            pending_expansion,
        )

        # if not hostvoltype:
        #     hostvoltype = "unknown"

        # send_analytics_tracker("pvc-%s" % hostvoltype, uid)
        return csi_pb2.ControllerExpandVolumeResponse(
            capacity_bytes=int(target_size),
            node_expansion_required=False,
        )
