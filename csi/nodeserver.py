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

        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)

        pvpath_full = os.path.join(mntdir, pvpath)

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
