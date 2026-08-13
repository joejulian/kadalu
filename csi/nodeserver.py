"""
nodeserver implementation
"""
from contextlib import contextmanager, nullcontext
import errno
import functools
import json
import logging
import os
import stat
import threading
import time

import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import (PV_TYPE_RAWBLOCK, PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK,
                       CommandException, get_legacy_volume_path,
                       get_single_pv_per_pool, get_volname_hash,
                       get_volume_path, is_safe_path_component,
                       is_valid_csi_identifier, logf)
from volumeutils import (MountTargetConflictError, ensure_single_pv_claim,
                         legacy_mount_fallback_authorized, mount_glusterfs,
                         mount_identity_token, mount_volume, single_pv_claim_matches,
                         single_pv_operation_lock, target_path_is_mounted,
                         unmount_volume)

HOSTVOL_MOUNTDIR = "/mnt"
VOLINFO_DIR = "/var/lib/gluster"
KUBELET_DIR = os.environ.get("KUBELET_DIR", "/var/lib/kubelet")
CSI_MOUNT_DIR = os.environ.get("CSI_MOUNT_DIR")
VALID_PV_TYPES = (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK)
TARGET_LOCKS = {}
TARGET_LOCKS_GUARD = threading.Lock()


class VolumeSourceNotFoundError(Exception):
    """Raised when a controller-managed volume source no longer exists."""


@contextmanager
def _target_operation_lock(target_path):
    """Serialize publish lifecycle changes for one normalized target path."""
    key = os.path.normpath(os.path.abspath(target_path or os.curdir))
    with TARGET_LOCKS_GUARD:
        entry = TARGET_LOCKS.get(key)
        if entry is None:
            entry = [threading.Lock(), 0]
            TARGET_LOCKS[key] = entry
        entry[1] += 1

    target_lock = entry[0]
    target_lock.acquire()
    try:
        yield
    finally:
        target_lock.release()
        with TARGET_LOCKS_GUARD:
            entry[1] -= 1
            if entry[1] == 0 and TARGET_LOCKS.get(key) is entry:
                del TARGET_LOCKS[key]


def serialized_target_operation(method):
    """Serialize a node RPC against other operations on the same target."""
    @functools.wraps(method)
    def locked_method(self, request, context):
        with _target_operation_lock(request.target_path):
            return method(self, request, context)

    return locked_method


def _is_within(path, parent):
    """Return whether an absolute path is contained by an absolute parent."""
    try:
        return os.path.commonpath((path, parent)) == parent
    except ValueError:
        return False


def _is_safe_component(value):
    """Return whether a CSI identifier is one safe path component."""
    return is_safe_path_component(value)


def _pool_record(hostvol):
    """Return a validated operator-managed hosting-pool record."""
    if not _is_safe_component(hostvol):
        raise ValueError("hostvol must be a non-empty path component")
    info_path = os.path.join(VOLINFO_DIR, f"{hostvol}.info")
    with open(info_path, encoding="utf-8") as info_file:
        record = json.load(info_file)
    if not isinstance(record, dict):
        raise ValueError("hosting-pool record must be an object")
    get_single_pv_per_pool(record)
    return record


def _validate_volume_paths(volume_id, hostvol, pvpath, pvtype, pool_record=None):
    """Build mount paths after rejecting unsafe CSI volume context values."""
    if not _is_safe_component(hostvol):
        raise ValueError("hostvol must be a non-empty path component")

    if not is_valid_csi_identifier(volume_id):
        raise ValueError("volume ID does not satisfy the CSI identifier contract")

    if pvtype not in VALID_PV_TYPES:
        raise ValueError("pvtype is not supported")

    if pool_record is None:
        pool_record = _pool_record(hostvol)
    single_pv_pool = get_single_pv_per_pool(pool_record)
    if pvpath and single_pv_pool:
        raise ValueError("single-PV pools require an empty volume path")
    if pvpath:
        expected_paths = {get_volume_path(
            pvtype,
            get_volname_hash(volume_id),
            volume_id,
        )}
        legacy_path = get_legacy_volume_path(
            pvtype,
            get_volname_hash(volume_id),
            volume_id,
        )
        if legacy_path is not None:
            expected_paths.add(legacy_path)
        if pvpath not in expected_paths:
            raise ValueError("path does not match the controller-generated path")
    elif not single_pv_pool:
        raise ValueError("empty path requires an authoritative single-PV pool")

    mount_root = os.path.abspath(HOSTVOL_MOUNTDIR)
    real_mount_root = os.path.realpath(mount_root)
    mntdir = os.path.join(mount_root, hostvol)
    if not target_path_is_mounted(mntdir):
        real_mntdir = os.path.realpath(mntdir)
        expected_real_mntdir = os.path.join(real_mount_root, hostvol)
        if real_mntdir != expected_real_mntdir:
            raise ValueError("hostvol must not resolve through a symbolic link")

    pvpath_full = os.path.normpath(os.path.join(mntdir, pvpath))
    if not _is_within(pvpath_full, mntdir):
        raise ValueError("path escapes its hosting volume")

    return mntdir, pvpath_full


def _validate_resolved_volume_path(mntdir, pvpath_full):
    """Reject links exposed by a hosting volume after it has been mounted."""
    real_mntdir = os.path.realpath(mntdir)
    real_mount_root = os.path.realpath(os.path.dirname(mntdir))
    expected_real_mntdir = os.path.join(
        real_mount_root,
        os.path.basename(mntdir),
    )
    if real_mntdir != expected_real_mntdir:
        raise ValueError("hostvol must not resolve through a symbolic link")

    real_pvpath = os.path.realpath(pvpath_full)
    expected_real_pvpath = os.path.normpath(os.path.join(
        real_mntdir,
        os.path.relpath(pvpath_full, mntdir),
    ))
    if real_pvpath != expected_real_pvpath:
        raise ValueError("path must not contain symbolic links")


def _expected_source_type(pvtype, source_stat):
    """Return whether a pinned source has the type created by the controller."""
    if pvtype == PV_TYPE_SUBVOL:
        return stat.S_ISDIR(source_stat.st_mode)
    return stat.S_ISREG(source_stat.st_mode)


@contextmanager
def _pinned_volume_source(mntdir, pvpath_full, pvtype):
    """Pin a symlink-free volume source across validation and mount."""
    relative_path = os.path.relpath(pvpath_full, mntdir)
    components = [] if relative_path == os.curdir else relative_path.split(os.sep)
    opened_fds = []
    path_flags = os.O_PATH | os.O_NOFOLLOW

    try:
        current_fd = os.open(
            mntdir,
            path_flags | os.O_DIRECTORY,
        )
        opened_fds.append(current_fd)

        for index, component in enumerate(components):
            flags = path_flags
            if index < len(components) - 1 or pvtype == PV_TYPE_SUBVOL:
                flags |= os.O_DIRECTORY
            current_fd = os.open(component, flags, dir_fd=current_fd)
            opened_fds.append(current_fd)

        source_stat = os.fstat(current_fd)
        if stat.S_ISLNK(source_stat.st_mode):
            raise ValueError("path must not contain symbolic links")
        if not _expected_source_type(pvtype, source_stat):
            raise ValueError("path has the wrong source type for pvtype")

        yield f"/proc/{os.getpid()}/fd/{current_fd}"
    except FileNotFoundError as err:
        raise VolumeSourceNotFoundError(
            "volume source path does not exist"
        ) from err
    except NotADirectoryError as err:
        raise ValueError("volume source path has an invalid component") from err
    finally:
        for descriptor in reversed(opened_fds):
            os.close(descriptor)


def _target_roots():
    """Return the host-backed roots mounted into the node plugin."""
    if CSI_MOUNT_DIR:
        return (os.path.abspath(CSI_MOUNT_DIR),)

    kubelet_dir = os.path.abspath(KUBELET_DIR)
    return (
        os.path.join(kubelet_dir, "pods"),
        os.path.join(kubelet_dir, "plugins", "kubernetes.io", "csi"),
    )


def _validate_target_path(target_path):
    """Reject targets outside the kubelet host mounts or through symlinks."""
    if not target_path or "\0" in target_path or not os.path.isabs(target_path):
        raise ValueError("target path must be absolute")

    normalized_target = os.path.normpath(target_path)
    if normalized_target != target_path:
        raise ValueError("target path must be normalized")

    for root in _target_roots():
        if not _is_within(normalized_target, root):
            continue

        real_root = os.path.realpath(root)
        relative_path = os.path.relpath(normalized_target, root)
        if relative_path == os.curdir:
            raise ValueError("target path must be below a kubelet mount root")
        relative_parent = os.path.dirname(relative_path)
        target_parent = os.path.dirname(normalized_target)
        expected_real_parent = os.path.normpath(os.path.join(
            real_root,
            relative_parent,
        ))
        if os.path.realpath(target_parent) != expected_real_parent:
            raise ValueError("target path must not contain symbolic links")
        if not target_path_is_mounted(normalized_target):
            try:
                target_stat = os.lstat(normalized_target)
            except (FileNotFoundError, PermissionError):
                target_stat = None
            if target_stat is not None and stat.S_ISLNK(target_stat.st_mode):
                raise ValueError("target path must not be a symbolic link")
        # Kubelet creates these root-owned target parents. Unlike the source,
        # pinning a not-yet-created mount target would break CSI idempotency;
        # an actor able to rename these host paths already has node-level
        # privilege, outside the unprivileged workload threat boundary.
        return normalized_target

    raise ValueError("target path is outside the kubelet mount roots")


# noqa # pylint: disable=too-many-locals
# noqa # pylint: disable=too-many-statements

class NodeServer(csi_pb2_grpc.NodeServicer):
    """
    NodeServer object is responsible for handling host
    volume mount and PV mounts.
    Ref:https://github.com/container-storage-interface/spec/blob/master/spec.md
    """
    # Each invalid CSI field is reported through the gRPC context immediately.
    # pylint: disable=too-many-branches,too-many-return-statements
    @serialized_target_operation
    def NodePublishVolume(self, request, context):
        start_time = time.time()
        if not request.volume_id:
            errmsg = "Volume ID is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        if not request.target_path:
            errmsg = "Target path is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        if not request.volume_capability:
            errmsg = "Volume capability is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        if not request.volume_context:
            errmsg = "Volume context is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        hostvol = request.volume_context.get("hostvol", "")
        pvpath = request.volume_context.get("path", "")
        pvtype = request.volume_context.get("pvtype", "")
        voltype = request.volume_context.get("type", "")
        access_type = request.volume_capability.WhichOneof("access_type")
        expected_access_type = (
            "block" if pvtype == PV_TYPE_RAWBLOCK else "mount"
        )
        if access_type != expected_access_type:
            errmsg = f"{pvtype} requires {expected_access_type} capability"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return csi_pb2.NodePublishVolumeResponse()

        fstype = None
        mount_flags = []
        if access_type == "mount":
            fstype = request.volume_capability.mount.fs_type or None
            mount_flags = list(request.volume_capability.mount.mount_flags)
            if pvtype == PV_TYPE_VIRTBLOCK and fstype not in (None, "xfs"):
                errmsg = "virtblock volumes require the xfs filesystem type"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return csi_pb2.NodePublishVolumeResponse()

        try:
            target_path = _validate_target_path(request.target_path)
            pool_record = _pool_record(hostvol)
            mntdir, pvpath_full = _validate_volume_paths(
                request.volume_id,
                hostvol,
                pvpath,
                pvtype,
                pool_record,
            )
        except FileNotFoundError as err:
            errmsg = f"Hosting-pool configuration was not found: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()
        except ValueError as err:
            errmsg = f"Invalid volume context: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        logging.debug(logf(
            "Received a valid mount request",
            request=request,
            voltype=voltype,
            hostvol=hostvol,
            pvpath=pvpath,
            pvtype=pvtype,
            pvpath_full=pvpath_full
        ))

        if pool_record.get("type") != voltype:
            errmsg = "Volume context does not match the hosting-pool type"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            return csi_pb2.NodePublishVolumeResponse()

        # Backend endpoints and options are operator-owned configuration. A
        # stale or forged PV context must never redirect the node plugin.
        volume = {
            **pool_record,
            'name': hostvol,
            'g_volname': pool_record.get('gluster_volname'),
            'g_host': pool_record.get('gluster_hosts'),
            'g_options': pool_record.get('gluster_options', ''),
            'mount_identity': pool_record.get('mount_identity'),
            'type': pool_record['type'],
        }

        try:
            mount_glusterfs(volume, mntdir, True)
        except MountTargetConflictError as err:
            errmsg = f"Hosting-volume target is already occupied: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            return csi_pb2.NodePublishVolumeResponse()
        except CommandException as err:
            errmsg = f"Unable to mount hosting volume: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return csi_pb2.NodePublishVolumeResponse()
        except ValueError as err:
            errmsg = f"Invalid hosting-pool configuration: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return csi_pb2.NodePublishVolumeResponse()
        except OSError as err:
            errmsg = f"Unable to mount hosting volume: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            transient_errors = {
                errno.EIO,
                errno.ENOTCONN,
                errno.ESTALE,
                errno.ETIMEDOUT,
                errno.EHOSTUNREACH,
            }
            context.set_code(
                grpc.StatusCode.UNAVAILABLE
                if err.errno in transient_errors
                else grpc.StatusCode.INTERNAL
            )
            return csi_pb2.NodePublishVolumeResponse()

        if not pvpath:
            try:
                claim_matches = ensure_single_pv_claim(
                    mntdir,
                    request.volume_id,
                    pool_record.get("legacy_single_pv_volume_id"),
                )
            except OSError as err:
                errmsg = f"Unable to read single-PV ownership: {err}"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return csi_pb2.NodePublishVolumeResponse()
            except ValueError as err:
                errmsg = f"Invalid single-PV ownership: {err}"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.INTERNAL)
                return csi_pb2.NodePublishVolumeResponse()

            if not claim_matches:
                errmsg = "Single-PV pool is not owned by this volume ID"
                logging.error(errmsg)
                context.set_details(errmsg)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return csi_pb2.NodePublishVolumeResponse()

        try:
            _validate_resolved_volume_path(mntdir, pvpath_full)
        except ValueError as err:
            errmsg = f"Invalid volume context: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        if voltype == "External":
            logging.debug(logf(
                "Mounted Volume for PV",
                volume=volume,
                mntdir=mntdir
            ))
            # return csi_pb2.NodePublishVolumeResponse()

        logging.debug(logf(
            "Mounted Hosting Volume",
            pv=request.volume_id,
            hostvol=hostvol,
            mntdir=mntdir
        ))
        try:
            claim_guard = (
                single_pv_operation_lock(mntdir)
                if not pvpath
                else nullcontext()
            )
            with claim_guard:
                if (
                        not pvpath
                        and not single_pv_claim_matches(
                            mntdir,
                            request.volume_id,
                        )):
                    raise VolumeSourceNotFoundError(
                        "single-PV ownership changed before publish"
                    )

                # Hold the source inode open so it cannot be replaced between
                # validation and mount(8) resolving the source path.
                with _pinned_volume_source(
                        mntdir, pvpath_full, pvtype) as source:
                    if (
                            not pvpath
                            and not single_pv_claim_matches(
                                mntdir,
                                request.volume_id,
                            )):
                        raise VolumeSourceNotFoundError(
                            "single-PV ownership changed before mount"
                        )
                    mounted = mount_volume(
                        source,
                        target_path,
                        pvtype,
                        fstype=fstype,
                        readonly=request.readonly,
                        mount_flags=mount_flags,
                        logical_source_path=pvpath_full,
                        volume_path=pvpath,
                        host_volume_name=(
                            pool_record.get("gluster_volname")
                            if pool_record.get("type") == "External"
                            else hostvol
                        ),
                        host_mount_identity=mount_identity_token(pool_record),
                        volume_id=request.volume_id,
                        allow_legacy_host_mount=(
                            legacy_mount_fallback_authorized(pool_record)
                        ),
                    )
        except VolumeSourceNotFoundError as err:
            errmsg = f"Volume source was not found: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return csi_pb2.NodePublishVolumeResponse()
        except MountTargetConflictError as err:
            errmsg = f"Target already has an incompatible mount: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            return csi_pb2.NodePublishVolumeResponse()
        except CommandException as err:
            errmsg = f"Unable to publish volume: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return csi_pb2.NodePublishVolumeResponse()
        except ValueError as err:
            errmsg = f"Invalid volume source: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()
        except OSError as err:
            errmsg = f"Unable to access volume source: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            transient_errors = {
                errno.EIO,
                errno.ENOTCONN,
                errno.ESTALE,
                errno.ETIMEDOUT,
                errno.EHOSTUNREACH,
            }
            status = (
                grpc.StatusCode.UNAVAILABLE
                if err.errno in transient_errors
                else grpc.StatusCode.INTERNAL
            )
            context.set_code(status)
            return csi_pb2.NodePublishVolumeResponse()

        # Mount the PV
        if not mounted:
            errmsg = "Volume source was not found"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return csi_pb2.NodePublishVolumeResponse()

        logging.info(logf(
            "Mounted PV",
            volume=request.volume_id,
            pvpath=pvpath,
            pvtype=pvtype,
            hostvol=hostvol,
            target_path=target_path,
            duration_seconds=time.time() - start_time
        ))
        return csi_pb2.NodePublishVolumeResponse()


    @serialized_target_operation
    def NodeUnpublishVolume(self, request, context):
        # TODO: Validation and handle target_path failures

        if not request.volume_id:
            errmsg = "Volume ID is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodeUnpublishVolumeResponse()

        if not request.target_path:
            errmsg = "Target path is empty and must be provided"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodeUnpublishVolumeResponse()

        try:
            target_path = _validate_target_path(request.target_path)
        except ValueError as err:
            errmsg = f"Invalid target path: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodeUnpublishVolumeResponse()

        logging.debug(logf(
            "Received the unmount request",
            request=request,
        ))

        try:
            unmount_volume(target_path)
        except CommandException as err:
            errmsg = f"Unable to unpublish volume: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INTERNAL)
            return csi_pb2.NodeUnpublishVolumeResponse()
        except OSError as err:
            errmsg = f"Unable to remove publish target: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            transient_errors = {
                errno.EIO,
                errno.ENOTCONN,
                errno.ESTALE,
                errno.ETIMEDOUT,
                errno.EHOSTUNREACH,
            }
            context.set_code(
                grpc.StatusCode.UNAVAILABLE
                if err.errno in transient_errors
                else grpc.StatusCode.INTERNAL
            )
            return csi_pb2.NodeUnpublishVolumeResponse()
        # Hosting-volume Gluster mounts are intentionally retained. A
        # concurrent NodePublishVolume may already be using the shared client,
        # and eager "last user" detection cannot be made atomic with kubelet's
        # independent publish requests.
        return csi_pb2.NodeUnpublishVolumeResponse()

    def NodeGetCapabilities(self, request, context):
        return csi_pb2.NodeGetCapabilitiesResponse()

    def NodeGetInfo(self, request, context):
        return csi_pb2.NodeGetInfoResponse(
            node_id=os.environ["NODE_ID"],
        )

    def NodeExpandVolume(self, request, context):

        logging.warning(logf(
            "NodeExpandVolume called, which is not implemented."
        ))

        return csi_pb2.NodeExpandVolumeResponse()
