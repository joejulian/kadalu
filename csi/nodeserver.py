"""
nodeserver implementation
"""
from contextlib import contextmanager
import json
import logging
import os
import stat
import time

import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import (PV_TYPE_RAWBLOCK, PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK,
                       get_single_pv_per_pool, get_volname_hash,
                       get_volume_path, logf)
from volumeutils import mount_glusterfs, mount_volume, unmount_volume

HOSTVOL_MOUNTDIR = "/mnt"
VOLINFO_DIR = "/var/lib/gluster"
KUBELET_DIR = os.environ.get("KUBELET_DIR", "/var/lib/kubelet")
VALID_PV_TYPES = (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK)


def _is_within(path, parent):
    """Return whether an absolute path is contained by an absolute parent."""
    try:
        return os.path.commonpath((path, parent)) == parent
    except ValueError:
        return False


def _is_safe_component(value):
    """Return whether a CSI identifier is one safe path component."""
    separators = (os.sep,) if os.altsep is None else (os.sep, os.altsep)
    return bool(value) and not (
        "\0" in value
        or value in (os.curdir, os.pardir)
        or os.path.isabs(value)
        or any(separator in value for separator in separators)
    )


def _single_pv_pool_is_authoritative(hostvol):
    """Read the operator-managed pool record, not the CSI request flag."""
    info_path = os.path.join(VOLINFO_DIR, f"{hostvol}.info")
    try:
        with open(info_path, encoding="utf-8") as info_file:
            return get_single_pv_per_pool(json.load(info_file)) is True
    except (OSError, ValueError, TypeError):
        return False


def _validate_volume_paths(volume_id, hostvol, pvpath, pvtype):
    """Build mount paths after rejecting unsafe CSI volume context values."""
    if not _is_safe_component(hostvol):
        raise ValueError("hostvol must be a non-empty path component")

    if not _is_safe_component(volume_id):
        raise ValueError("volume ID must be a non-empty path component")

    if pvtype not in VALID_PV_TYPES:
        raise ValueError("pvtype is not supported")

    if pvpath:
        expected_path = get_volume_path(
            pvtype,
            get_volname_hash(volume_id),
            volume_id,
        )
        if pvpath != expected_path:
            raise ValueError("path does not match the controller-generated path")
    elif not _single_pv_pool_is_authoritative(hostvol):
        raise ValueError("empty path requires an authoritative single-PV pool")

    mount_root = os.path.abspath(HOSTVOL_MOUNTDIR)
    real_mount_root = os.path.realpath(mount_root)
    mntdir = os.path.join(mount_root, hostvol)
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
        raise ValueError("volume source path does not exist") from err
    except NotADirectoryError as err:
        raise ValueError("volume source path has an invalid component") from err
    finally:
        for descriptor in reversed(opened_fds):
            os.close(descriptor)


def _target_roots():
    """Return the host-backed roots mounted into the node plugin."""
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
        expected_real_target = os.path.join(real_root, relative_path)
        if os.path.realpath(normalized_target) != expected_real_target:
            raise ValueError("target path must not contain symbolic links")
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
    # pylint: disable=too-many-return-statements
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
        gserver = request.volume_context.get("gserver", None)
        gvolname = request.volume_context.get("gvolname", None)
        options = request.volume_context.get("options", None)

        try:
            target_path = _validate_target_path(request.target_path)
            mntdir, pvpath_full = _validate_volume_paths(
                request.volume_id,
                hostvol,
                pvpath,
                pvtype,
            )
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

        volume = {
            'name': hostvol,
            'g_volname': gvolname,
            'g_host': gserver,
            'g_options': options,
            'type': voltype,
        }

        mount_glusterfs(volume, mntdir, True)

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
            # Hold the source inode open so it cannot be replaced between the
            # checks above and mount(8) resolving the source path.
            with _pinned_volume_source(mntdir, pvpath_full, pvtype) as source:
                mounted = mount_volume(
                    source,
                    target_path,
                    pvtype,
                    fstype=None,
                )
        except (OSError, ValueError) as err:
            errmsg = f"Invalid volume source: {err}"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return csi_pb2.NodePublishVolumeResponse()

        # Mount the PV
        # TODO: Handle Volume capability mount flags
        if not mounted:
            errmsg = "Unable to bind PV to target path"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
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

        unmount_volume(target_path)
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
