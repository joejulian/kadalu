"""
Starting point of CSI driver GRP server
"""
import logging
import os
import signal
import threading
import time
from concurrent import futures

import csi_pb2_grpc
import grpc
from controllerserver import ControllerServer
from identityserver import IdentityServer
from kadalulib import CommandException, logf, logging_setup
from nodeserver import NodeServer
from volumeutils import (HOSTVOL_MOUNTDIR, MountTargetConflictError,
                         get_pv_hosting_volumes, mount_glusterfs)

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
STORAGE_MOUNT_RETRY_SECONDS = 5
STORAGE_READY_FILE = "/plugin/storage-ready"


def _handle_sighup(_signum, _frame):
    """Ignore legacy reload signals; volfile servers push configuration."""
    logging.info("Received SIGHUP; volfile servers push configuration updates")


def mount_storage():
    """Mount every required provisioner hosting volume in the current config."""
    if os.environ.get("CSI_ROLE", "-") != "provisioner":
        logging.debug("Volume need to be mounted on only provisioner pod")
        return True

    try:
        host_volumes = get_pv_hosting_volumes({}, iteration=0)
    except (OSError, ValueError) as err:
        logging.error(logf(
            "Unable to discover storage volumes",
            error=err,
        ))
        return False

    all_mounted = True
    for volume in host_volumes:
        if volume["single_pv_per_pool"]:
            # Need to skip mounting external non-native mounts in-order for
            # kadalu-quotad not to set quota xattrs
            continue
        hvol = volume["name"]
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hvol)
        try:
            mount_glusterfs(volume, mntdir)
            logging.debug(logf("Volume mount is available", hvol=hvol))
        except (
                CommandException,
                MountTargetConflictError,
                OSError,
                ValueError,
        ) as err:
            all_mounted = False
            # Do not leave the pod Ready while a later hosting pool performs
            # a potentially slow reachability or mount attempt. Marker I/O
            # failure must not prevent healthy pools from being attempted.
            if storage_ready_marker_exists():
                try:
                    clear_storage_ready()
                except Exception:  # pylint: disable=broad-exception-caught
                    logging.exception(
                        "Unable to clear readiness after storage mount failure"
                    )
            logging.error(logf(
                "Unable to mount volume",
                hvol=hvol,
                error=err,
            ))
    return all_mounted


def mark_storage_ready():
    """Publish the provisioner's readiness after every required mount works."""
    ready_fd = os.open(
        STORAGE_READY_FILE,
        os.O_CREAT | os.O_TRUNC | os.O_WRONLY,
        0o600,
    )
    os.close(ready_fd)


def clear_storage_ready():
    """Remove readiness left by an earlier CSI subprocess incarnation."""
    try:
        os.unlink(STORAGE_READY_FILE)
    except FileNotFoundError:
        pass


def storage_ready_marker_exists():
    """Return whether the readiness probe can see its regular-file marker."""
    return os.path.isfile(STORAGE_READY_FILE)


def reconcile_storage_mounts_once(previous_ready):
    """Reconcile one mount pass and publish readiness state transitions."""
    all_mounted = mount_storage()
    marker_ready = storage_ready_marker_exists()

    if all_mounted:
        if not marker_ready:
            mark_storage_ready()
        if previous_ready is not True:
            logging.info("All required storage volumes are mounted")
        elif not marker_ready:
            logging.warning("Restored missing storage readiness marker")
    else:
        if marker_ready:
            clear_storage_ready()
        if previous_ready is not False:
            logging.warning(logf(
                "Required storage is not mounted; retrying",
                retry_seconds=STORAGE_MOUNT_RETRY_SECONDS,
            ))
        elif marker_ready:
            logging.warning("Removed unexpected storage readiness marker")
    return all_mounted


def reconcile_storage_mounts():
    """Continuously restore required mounts without blocking CSI service."""
    mounts_ready = None
    while True:
        try:
            mounts_ready = reconcile_storage_mounts_once(mounts_ready)
        # This is the daemon thread's last exception boundary. An unexpected
        # config shape or marker I/O error must not permanently stop repair.
        except Exception:  # pylint: disable=broad-exception-caught
            logging.exception("Storage mount reconciliation failed")
            try:
                clear_storage_ready()
            except Exception:  # pylint: disable=broad-exception-caught
                logging.exception(
                    "Unable to clear storage readiness after reconcile error"
                )
            # Marker cleanup was best-effort, so the published state is now
            # unknown. Force the next pass to publish its observed state.
            mounts_ready = None
        time.sleep(STORAGE_MOUNT_RETRY_SECONDS)


def run_storage_mount_reconciler():
    """Run mount reconciliation and clear readiness on every terminal path."""
    try:
        reconcile_storage_mounts()
    finally:
        clear_storage_ready()


def start_storage_mount_reconciler():
    """Start provisioner mount reconciliation without blocking CSI service."""
    reconciler = threading.Thread(
        target=run_storage_mount_reconciler,
        name="storage-mount-reconciler",
        daemon=True,
    )
    reconciler.start()
    return reconciler


def ensure_storage_mount_reconciler(reconciler):
    """Restart a terminated reconciler only after clearing stale readiness."""
    if reconciler is not None and reconciler.is_alive():
        return reconciler

    logging.error("Storage mount reconciler stopped; restarting")
    # Fail closed if marker cleanup itself cannot complete. The CSI subprocess
    # will exit and its process monitor can restart it with socket health down.
    clear_storage_ready()
    return start_storage_mount_reconciler()


def main():
    """
    Register Controller Server, Node server and Identity Server and start
    the GRPC server in required endpoint
    """
    logging_setup()
    signal.signal(signal.SIGHUP, _handle_sighup)

    is_provisioner = os.environ.get("CSI_ROLE", "-") == "provisioner"
    if is_provisioner:
        clear_storage_ready()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csi_pb2_grpc.add_ControllerServicer_to_server(ControllerServer(), server)
    csi_pb2_grpc.add_NodeServicer_to_server(NodeServer(), server)
    csi_pb2_grpc.add_IdentityServicer_to_server(IdentityServer(), server)

    server.add_insecure_port(os.environ.get("CSI_ENDPOINT", "unix://plugin/csi.sock"))
    logging.info("Server started")
    server.start()
    reconciler = None
    if is_provisioner:
        # A failed hosting pool must not take the controller or healthy pools
        # offline. Readiness remains false until this reconciler succeeds.
        reconciler = start_storage_mount_reconciler()
    try:
        while True:
            time.sleep(
                STORAGE_MOUNT_RETRY_SECONDS
                if is_provisioner
                else _ONE_DAY_IN_SECONDS
            )
            if is_provisioner:
                reconciler = ensure_storage_mount_reconciler(reconciler)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    main()
