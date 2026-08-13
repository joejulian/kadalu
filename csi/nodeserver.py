"""
nodeserver implementation
"""
import logging
import os
import re
import time

import csi_pb2
import csi_pb2_grpc
import grpc
from kadalulib import logf
from volumeutils import mount_glusterfs, unmount_glusterfs, mount_volume, unmount_volume

HOSTVOL_MOUNTDIR = "/mnt"
MOUNTS_FILE = "/proc/mounts"


def _is_within(path, parent):
    """Return whether an absolute path is contained by an absolute parent."""
    try:
        return os.path.commonpath((path, parent)) == parent
    except ValueError:
        return False


def _validate_volume_paths(hostvol, pvpath):
    """Build mount paths after rejecting unsafe CSI volume context values."""
    separators = (os.sep,) if os.altsep is None else (os.sep, os.altsep)
    if (not hostvol
            or "\0" in hostvol
            or hostvol in (os.curdir, os.pardir)
            or os.path.isabs(hostvol)
            or any(separator in hostvol for separator in separators)):
        raise ValueError("hostvol must be a non-empty path component")

    if "\0" in pvpath or os.path.isabs(pvpath):
        raise ValueError("path must be relative to its hosting volume")

    if pvpath == os.curdir or os.pardir in pvpath.split(os.sep):
        raise ValueError("path must not contain parent-directory references")

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
    if not _is_within(real_pvpath, real_mntdir):
        raise ValueError("path resolves outside its hosting volume")


def _unescape_mount_field(value):
    """Decode the octal escapes used in /proc/mounts fields."""
    return re.sub(
        r"\\([0-7]{3})",
        lambda match: chr(int(match.group(1), 8)),
        value,
    )


def _read_gluster_mounts():
    """Return Gluster mount source and target pairs without invoking a shell."""
    mounts = []
    with open(MOUNTS_FILE, encoding="utf-8") as mounts_file:
        for line in mounts_file:
            fields = line.split()
            if len(fields) < 3 or fields[2] != "fuse.glusterfs":
                continue
            mounts.append((
                _unescape_mount_field(fields[0]),
                _unescape_mount_field(fields[1]),
            ))
    return mounts


def _gluster_volume_name(source):
    """Extract the volume name from a Gluster mount source."""
    _, separator, remote_path = source.rpartition(":")
    if not separator:
        return None
    return remote_path.lstrip("/").split("/", maxsplit=1)[0] or None

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
            mntdir, pvpath_full = _validate_volume_paths(hostvol, pvpath)
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
        # Mount the PV
        # TODO: Handle Volume capability mount flags
        if mount_volume(pvpath_full, request.target_path, pvtype, fstype=None):
            logging.info(logf(
                "Mounted PV",
                volume=request.volume_id,
                pvpath=pvpath,
                pvtype=pvtype,
                hostvol=hostvol,
                target_path=request.target_path,
                duration_seconds=time.time() - start_time
            ))
        else:
            errmsg = "Unable to bind PV to target path"
            logging.error(errmsg)
            context.set_details(errmsg)
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
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

        logging.debug(logf(
            "Received the unmount request",
            request=request,
        ))

        mounts = _read_gluster_mounts()
        source = next((
            mount_source
            for mount_source, target in mounts
            if target == request.target_path
        ), None)
        gvolname = _gluster_volume_name(source) if source else None

        logging.debug(logf(
            f"Got gluster volume name {gvolname}"
        ))

        unmount_volume(request.target_path)

        if gvolname is None:
            return csi_pb2.NodeUnpublishVolumeResponse()

        remaining_mounts = [
            (mount_source, target)
            for mount_source, target in _read_gluster_mounts()
            if _gluster_volume_name(mount_source) == gvolname
        ]

        # If only the hosting-volume mount remains, unmount it too.
        if len(remaining_mounts) == 1:
            _, mntdir = remaining_mounts[0]
            if os.path.dirname(mntdir) != HOSTVOL_MOUNTDIR:
                return csi_pb2.NodeUnpublishVolumeResponse()
            logging.debug(logf(
                f"Only one mount left, going to unmount {mntdir}"
            ))

            unmount_glusterfs(mntdir, gvolname)

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
