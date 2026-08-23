"""
Utility functions for Volume management
"""

import errno
import hashlib
import fcntl
import json
import logging
import os
import re
import shlex
import shutil
import sqlite3
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from errno import ENOTCONN
from pathlib import Path

from kadalulib import (PV_TYPE_RAWBLOCK, PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK,
                       CommandException, SizeAccounting, execute,
                       get_legacy_volume_path, get_volname_hash,
                       get_volume_path, volume_name_from_component,
                       is_gluster_mount_proc_running, logf, makedirs,
                       reachable_host, retry_errors, get_single_pv_per_pool,
                       is_safe_path_component, is_server_pod_reachable,
                       is_valid_csi_identifier)

GLUSTERFS_CMD = "/opt/sbin/glusterfs"
MOUNT_CMD = "/bin/mount"
UNMOUNT_CMD = "/bin/umount"
MKFS_XFS_CMD = "/sbin/mkfs.xfs"
XFS_GROWFS_CMD = "/sbin/xfs_growfs"
RESERVED_SIZE_PERCENTAGE = 10
HOSTVOL_MOUNTDIR = "/mnt"
VOLFILES_DIR = "/kadalu/volfiles"
VOLINFO_DIR = "/var/lib/gluster"
MOUNTS_FILE = "/proc/self/mounts"
MOUNTINFO_FILE = "/proc/self/mountinfo"
SINGLE_PV_CLAIM_XATTR = "trusted.kadalu.single-pv-claim"
SINGLE_PV_CLAIM_VERSION = 1
ARCHIVE_PREFIX = "archived-"
VALID_RECLAIM_POLICIES = ("archive", "delete", "retain")
CREATION_INTENT_VERSION = 1
# Keep the old name while callers and on-disk version-1 block intents migrate
# to the shared subvolume/block creation-intent contract.
BLOCK_CREATION_INTENT_VERSION = CREATION_INTENT_VERSION
EXPANSION_INTENT_VERSION = 1
PUBLISH_STATE_VERSION = 1

statfile_lock = threading.Lock()    # noqa # pylint: disable=invalid-name
mount_lock = threading.Lock()    # noqa # pylint: disable=invalid-name
single_pv_lock_state = threading.local()  # noqa # pylint: disable=invalid-name
volume_operation_lock_state = threading.local()  # noqa # pylint: disable=invalid-name


class SinglePVPoolClaimedError(Exception):
    """Raised when a single-PV pool already belongs to another volume."""


class UnsupportedReclaimPolicyError(Exception):
    """Raised when a reclaim policy cannot be safe for a volume mode."""


class MountTargetConflictError(OSError):
    """Raised when a publish target has an incompatible existing mount."""


class SinglePVPoolRetiredError(Exception):
    """Raised when a deleted dedicated pool requires administrator reset."""


class LegacySinglePVPoolUnclaimedError(Exception):
    """Raised when an old whole-pool PV has no authoritative owner."""


class UnsupportedCapacityRangeError(ValueError):
    """Raised when a dedicated pool cannot satisfy a CSI capacity range."""


class SinglePVPoolNotEmptyError(ValueError):
    """Raised when shared-volume metadata prevents whole-pool conversion."""


class VolumeOperationLockTimeoutError(TimeoutError):
    """Raised when another controller owns a volume lifecycle operation."""


class VolumeOperationConflictError(ValueError):
    """Raised when a volume changes while a lifecycle lock is acquired."""


class HostingVolumeReconciliationError(ValueError):
    """Raised when one pool cannot provide trustworthy capacity state."""


class HostingVolumeSelectionError(CommandException):
    """Raised when failed checks prevent a definitive capacity result."""

    def __init__(self, failures):
        self.failures = tuple(failures)
        details = "; ".join(
            f"{hostvol}: {error}"
            for hostvol, error in self.failures
        )
        return_code = (
            124
            if self.failures and all(
                isinstance(error, CommandException) and error.ret == 124
                for _hostvol, error in self.failures
            )
            else -1
        )
        super().__init__(
            return_code,
            "mount_and_select_hosting_volume",
            "No hosting volume could satisfy the request while availability "
            f"checks failed: {details}",
        )


class Volume():
    """Hosting Volume object"""
    # noqa # pylint: disable=too-many-instance-attributes
    def __init__(self, volname, voltype, hostvol, **kwargs):
        self.voltype = voltype
        self.volname = volname
        self.volhash = kwargs.get("volhash", None)
        self.volpath = kwargs.get("volpath", None)
        self.hostvol = hostvol
        self.single_pv_per_pool = kwargs.get("single_pv_per_pool", False)
        self.size = kwargs.get("size", None)
        self.extra = {}
        self.extra['ghost'] = kwargs.get("ghost", None)
        self.extra['hostvoltype'] = kwargs.get("hostvoltype", None)
        self.extra['gvolname'] = kwargs.get("gvolname", None)
        self.extra['goptions'] = kwargs.get("goptions", "")
        self.extra['node_affinity'] = kwargs.get("node_affinity", None)
        self.setpath()

    def setpath(self):
        """Set Volume path based on hash and volume name"""
        if self.volpath is None:
            self.volpath = get_volume_path(
                self.voltype,
                self.volhash,
                self.volname
            )

    def get(self):
        """Get Volume name"""
        return self.volname


def filter_node_affinity(volume, filters):
    """
    Filter volume based on node affinity provided
    """
    node_name = filters.get("node_affinity", None)
    if node_name is not None:
        # Node affinity is only applicable for Replica1 Volumes
        if volume["type"] != "Replica1":
            return None

        # Volume is not from the requested node
        if node_name != volume["bricks"][0]["kube_hostname"]:
            return None

    return volume


def filter_storage_name(volume, filters):
    """
    filter volume based on the name provided in filter
    """
    storage_name = filters.get("storage_name", None)
    if storage_name is not None and storage_name != volume["volname"]:
        return None

    return volume


def filter_storage_type(volume, filters):
    """
    If Host Volume type is specified then only get the hosting
    volumes which belongs to requested types
    """
    hvoltype = filters.get(
        "storage_type",
        filters.get("hostvol_type", None)
    )
    if hvoltype is not None and hvoltype != volume["type"]:
        return None

    return volume


def filter_supported_pvtype(volume, filters):
    """
    If a storageclass created by specifying supported_pvtype
    then only include those hosting Volumes.
    This is useful when different Volume option needs to be
    set to host virtblock PVs
    """
    f_supported_pvtype = filters.get("supported_pvtype", None)
    supported_pvtype = volume.get("supported_pvtype", "all")
    if supported_pvtype == "all":
        return volume

    if f_supported_pvtype is not None \
       and f_supported_pvtype != supported_pvtype:
        return None

    return volume


def new_volume_hosting_candidates(volumes):
    """Exclude operator-fenced pools only from fresh volume placement."""
    return [
        volume
        for volume in volumes
        if volume.get("provisioning_disabled") is not True
    ]


# Disabled pylint here because filters argument is used as
# readonly in all functions
# noqa # pylint: disable=dangerous-default-value
def get_pv_hosting_volumes(filters={}, iteration=40):
    """Get list of pv hosting volumes"""
    volumes = []
    total_volumes = 0

    filter_funcs = [
        filter_node_affinity, filter_storage_type, filter_supported_pvtype
    ]

    for filename in os.listdir(VOLINFO_DIR):
        if filename.endswith(".info"):
            total_volumes += 1
            volname = filename.removesuffix(".info")

            filtered = filter_storage_name({"volname": volname}, filters)
            if filtered is None:
                logging.debug(
                    logf("Volume doesn't match the filter",
                         volname=volname,
                         **filters))
                continue

            data = {}
            with open(os.path.join(VOLINFO_DIR, filename)) as info_file:
                data = json.load(info_file)

            filtered_data = True
            for filter_func in filter_funcs:
                filtered = filter_func(data, filters)
                # Node affinity is not matching for this Volume,
                # Try other volumes
                if filtered is None:
                    filtered_data = False
                    logging.debug(
                        logf("Volume doesn't match the filter",
                             volname=data["volname"],
                             **filters))
                    break

            if not filtered_data:
                continue

            volume = {
                **data,
                "name": volname,
                "type": data["type"],
                "g_volname": data.get("gluster_volname", None),
                "g_host": data.get("gluster_hosts", None),
                "g_options": data.get("gluster_options", ""),
                "single_pv_per_pool": get_single_pv_per_pool(data),
                "single_pv_claim_version": data.get(
                    "single_pv_claim_version"
                ),
                "legacy_single_pv_volume_id": data.get(
                    "legacy_single_pv_volume_id"
                ),
                "mount_identity": data.get("mount_identity"),
                "mount_config_fingerprint": data.get(
                    "mount_config_fingerprint"
                ),
                "node_affinity": (
                    data.get("bricks", [{}])[0].get("kube_hostname")
                    if data.get("bricks") else None
                ),
            }

            volumes.append(volume)

    # Need a different way to get external-kadalu volumes

    # If volume file is not yet available, ConfigMap may not be ready
    # or synced. Wait for some time and try again
    # Lets just give maximum 2 minutes for the config map to come up!
    if total_volumes == 0 and iteration > 0:
        time.sleep(3)
        iteration -= 1
        return get_pv_hosting_volumes(filters, iteration)

    return volumes


def _canonical_gluster_hosts(hosts):
    """Normalize Gluster endpoints without making their order identity."""
    if isinstance(hosts, str):
        hosts = hosts.split(",")
    if not isinstance(hosts, (list, tuple)):
        return []
    return sorted({str(host).strip() for host in hosts if str(host).strip()})


def mount_config_fingerprint(volume):
    """Return the operator-compatible digest of one backend configuration."""
    canonical = {
        "schema": 1,
        "type": volume.get("type"),
        "volname": volume.get("volname", volume.get("name")),
        "volume_id": volume.get("volume_id"),
        "single_pv_per_pool": get_single_pv_per_pool(volume),
    }
    if volume.get("type") == "External":
        canonical.update({
            "gluster_hosts": _canonical_gluster_hosts(
                volume.get("gluster_hosts", volume.get("g_host", ""))
            ),
            "gluster_volname": volume.get(
                "gluster_volname",
                volume.get("g_volname"),
            ),
            "gluster_options": volume.get(
                "gluster_options",
                volume.get("g_options", ""),
            ),
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
                for brick in volume.get("bricks", [])
            ],
            "disperse": volume.get("disperse", {}),
            "options": volume.get("options", {}),
            "tiebreaker": volume.get("tiebreaker", {}),
        })

    serialized = json.dumps(
        canonical,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _mount_identity(volume):
    """Return a process/display identity for this exact pool configuration."""
    generation = volume.get("mount_identity")
    if generation is None:
        return None
    try:
        generation = str(uuid.UUID(generation))
    except (AttributeError, TypeError, ValueError) as err:
        raise ValueError("Pool mount identity is not a UUID") from err

    computed_fingerprint = mount_config_fingerprint(volume)
    fingerprint = volume.get("mount_config_fingerprint")
    if fingerprint is None:
        fingerprint = computed_fingerprint
    elif (
            not isinstance(fingerprint, str)
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
            or fingerprint != computed_fingerprint):
        raise ValueError("Pool mount configuration fingerprint does not match")
    return f"{generation}-{fingerprint}"


def mount_identity_token(volume):
    """Return the validated mount identity exposed to publish recovery."""
    return _mount_identity(volume)


def _mount_identity_generation(identity):
    """Return the stable UUID from one validated UUID-fingerprint token."""
    if not isinstance(identity, str):
        return None
    generation, separator, fingerprint = identity.rpartition("-")
    if (
            not separator
            or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None):
        return None
    try:
        canonical_generation = str(uuid.UUID(generation))
    except ValueError:
        return None
    return canonical_generation if generation == canonical_generation else None


def _same_mount_identity_generation(first, second):
    """Return whether exact mount tokens share one stable pool generation."""
    first_generation = _mount_identity_generation(first)
    return (
        first_generation is not None
        and first_generation == _mount_identity_generation(second)
    )


def legacy_mount_fallback_authorized(volume):
    """Return whether unchanged migrated metadata permits legacy recovery."""
    legacy_fingerprint = volume.get("legacy_mount_config_fingerprint")
    return (
        isinstance(legacy_fingerprint, str)
        and re.fullmatch(r"[0-9a-f]{64}", legacy_fingerprint) is not None
        and legacy_fingerprint == mount_config_fingerprint(volume)
    )


def update_free_size(hostvol, pvname, sizechange):
    """Update the free size in respective host volume's stats.db file"""

    mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)

    # Check for mount availability before updating the free size
    retry_errors(os.statvfs, [mntdir], [ENOTCONN])

    with statfile_lock:
        with SizeAccounting(hostvol, mntdir) as acc:
            # Reclaim space
            if sizechange > 0:
                acc.remove_pv_record(pvname)
            else:
                acc.update_pv_record(pvname, -sizechange)


def get_accounted_pv_size(hostvol, pvname):
    """Return the absolute size currently reserved for a PV in stat.db."""
    mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
    retry_errors(os.statvfs, [mntdir], [ENOTCONN])
    with statfile_lock:
        with SizeAccounting(hostvol, mntdir) as acc:
            return acc.get_pv_size(pvname)


def read_single_pv_claim(hostvol_mnt):
    """Read and validate a single-PV pool's protected ownership marker."""
    try:
        claim = json.loads(os.getxattr(
            hostvol_mnt,
            SINGLE_PV_CLAIM_XATTR,
        ).decode("utf-8"))
    except OSError as err:
        missing_xattr_errors = {errno.ENODATA}
        if hasattr(errno, "ENOATTR"):
            missing_xattr_errors.add(errno.ENOATTR)
        if err.errno in missing_xattr_errors:
            return None
        raise

    valid_claim = (
        isinstance(claim, dict)
        and claim.get("version") == SINGLE_PV_CLAIM_VERSION
        and is_valid_csi_identifier(claim.get("volume_id"))
        and isinstance(claim.get("size"), int)
        and claim["size"] > 0
        and claim.get("pvtype") == PV_TYPE_SUBVOL
        and get_single_pv_per_pool(claim)
        and claim.get("state") in ("active", "deleting", "retired")
    )
    if not valid_claim:
        raise ValueError("Invalid single-PV ownership marker")
    return claim


def _acquire_operation_flock(lock_fd, timeout, message):
    """Acquire one flock, optionally failing within a bounded wait."""
    if timeout is None:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        return

    deadline = time.monotonic() + max(timeout, 0)
    while True:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError as err:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise VolumeOperationLockTimeoutError(message) from err
            time.sleep(min(0.05, remaining))


@contextmanager
def single_pv_operation_lock(hostvol_mnt, timeout=None):
    """Serialize whole-pool ownership operations across CSI clients.

    The lock is placed on the mounted storage root itself, rather than a file
    inside it, so purging a dedicated pool cannot remove the lock target. The
    small thread-local reentrancy layer lets helpers safely compose one
    ownership transaction without attempting a second blocking ``flock`` on
    another file descriptor in the same thread.
    """
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])
    lock_key = os.path.realpath(hostvol_mnt)
    held_locks = getattr(single_pv_lock_state, "held_locks", None)
    if held_locks is None:
        held_locks = {}
        single_pv_lock_state.held_locks = held_locks

    held = held_locks.get(lock_key)
    if held is not None:
        lock_fd, depth = held
        held_locks[lock_key] = (lock_fd, depth + 1)
        try:
            yield
        finally:
            held_locks[lock_key] = (lock_fd, depth)
        return

    lock_fd = os.open(hostvol_mnt, os.O_RDONLY | os.O_DIRECTORY)
    try:
        _acquire_operation_flock(
            lock_fd,
            timeout,
            "Another hosting-pool operation is still in progress",
        )
        held_locks[lock_key] = (lock_fd, 1)
        try:
            yield
        finally:
            held_locks.pop(lock_key, None)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        os.close(lock_fd)


@contextmanager
def volume_operation_lock(hostvol_mnt, volume_id, timeout=None):
    """Serialize one managed PV lifecycle across controller processes."""
    if not is_valid_csi_identifier(volume_id):
        raise ValueError("Volume operation lock has an invalid volume ID")
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])
    lock_dir = os.path.join(hostvol_mnt, "info", ".locks")
    makedirs(lock_dir)
    lock_path = os.path.join(
        lock_dir,
        f"{hashlib.sha256(volume_id.encode('utf-8')).hexdigest()}.lock",
    )
    lock_key = os.path.realpath(lock_path)
    held_locks = getattr(volume_operation_lock_state, "held_locks", None)
    if held_locks is None:
        held_locks = {}
        volume_operation_lock_state.held_locks = held_locks

    held = held_locks.get(lock_key)
    if held is not None:
        lock_fd, depth = held
        held_locks[lock_key] = (lock_fd, depth + 1)
        try:
            yield
        finally:
            held_locks[lock_key] = (lock_fd, depth)
        return

    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        _acquire_operation_flock(
            lock_fd,
            timeout,
            "Another lifecycle operation is still in progress "
            f"for volume {volume_id}",
        )
        held_locks[lock_key] = (lock_fd, 1)
        try:
            yield
        finally:
            held_locks.pop(lock_key, None)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        os.close(lock_fd)


def _pool_has_unclaimed_data(hostvol_mnt):
    """Return whether a pool contains data predating its ownership marker.

    A newly mounted pool may already have the empty bookkeeping directories
    used by the CSI driver. Everything else is evidence that the pool is not
    safe to assign to a new whole-pool volume, even when its ConfigMap record
    was lost or recreated.
    """
    with os.scandir(hostvol_mnt) as entries:
        for entry in entries:
            if (
                    entry.name in {"info", "archive"}
                    and entry.is_dir(follow_symlinks=False)):
                with os.scandir(entry.path) as internal_entries:
                    contents = list(internal_entries)
                    if not contents:
                        continue
                    if (
                            entry.name == "info"
                            and len(contents) == 1
                            and contents[0].name == ".locks"
                            and contents[0].is_dir(follow_symlinks=False)):
                        continue
            return True
    return False


def claim_single_pv_volume(hostvol_mnt, volume_id, size):
    """Atomically assign an otherwise dedicated pool to one CSI volume."""
    with single_pv_operation_lock(hostvol_mnt):
        return _claim_single_pv_volume(hostvol_mnt, volume_id, size)


def _claim_single_pv_volume(
        hostvol_mnt, volume_id, size, allow_existing_data=False):
    """Assign one pool while its cross-client operation lock is held."""
    claim = {
        "version": SINGLE_PV_CLAIM_VERSION,
        "volume_id": volume_id,
        "size": int(size),
        "pvtype": PV_TYPE_SUBVOL,
        "single_pv_per_pool": True,
        "state": "active",
    }

    existing = read_single_pv_claim(hostvol_mnt)
    if existing is not None:
        if existing["state"] != "active":
            raise SinglePVPoolRetiredError(volume_id)
        if existing["volume_id"] != volume_id:
            raise SinglePVPoolClaimedError(existing["volume_id"])
        return existing

    if not allow_existing_data and _pool_has_unclaimed_data(hostvol_mnt):
        raise SinglePVPoolNotEmptyError(
            "A non-empty pool cannot be assigned to a new single-PV volume"
        )

    try:
        os.setxattr(
            hostvol_mnt,
            SINGLE_PV_CLAIM_XATTR,
            json.dumps(claim, sort_keys=True).encode("utf-8"),
            flags=os.XATTR_CREATE,
        )
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise
        existing = read_single_pv_claim(hostvol_mnt)
        if existing is None:
            raise
        if existing["state"] != "active":
            raise SinglePVPoolRetiredError(volume_id) from err
        if existing["volume_id"] != volume_id:
            raise SinglePVPoolClaimedError(existing["volume_id"]) from err
        return existing
    return claim


def single_pv_claim_matches(hostvol_mnt, volume_id):
    """Return whether a mounted single-PV pool belongs to this volume ID."""
    claim = read_single_pv_claim(hostvol_mnt)
    return (
        claim is not None
        and claim["state"] == "active"
        and claim["volume_id"] == volume_id
    )


def ensure_single_pv_claim(hostvol_mnt, volume_id, legacy_volume_id=None):
    """Return ownership, adopting only an operator-identified legacy PV."""
    with single_pv_operation_lock(hostvol_mnt):
        return _ensure_single_pv_claim(
            hostvol_mnt,
            volume_id,
            legacy_volume_id,
        )


def _ensure_single_pv_claim(hostvol_mnt, volume_id, legacy_volume_id=None):
    """Ensure ownership while the cross-client pool lock is held."""
    claim = read_single_pv_claim(hostvol_mnt)
    if claim is None:
        if legacy_volume_id != volume_id:
            return False
        stats = retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])
        capacity = stats.f_blocks * stats.f_frsize
        try:
            claim = _claim_single_pv_volume(
                hostvol_mnt,
                volume_id,
                capacity,
                allow_existing_data=True,
            )
        except (SinglePVPoolClaimedError, SinglePVPoolRetiredError):
            return False

    return (
        claim["state"] == "active"
        and claim["volume_id"] == volume_id
    )


def begin_single_pv_delete(hostvol_mnt, volume_id):
    """Block new publishes before any data is removed from a whole pool."""
    claim = read_single_pv_claim(hostvol_mnt)
    if claim is None:
        raise ValueError("Single-PV ownership marker is missing")
    if claim["volume_id"] != volume_id:
        raise SinglePVPoolClaimedError(claim["volume_id"])
    if claim["state"] == "retired":
        return claim
    if claim["state"] == "active":
        claim["state"] = "deleting"
        os.setxattr(
            hostvol_mnt,
            SINGLE_PV_CLAIM_XATTR,
            json.dumps(claim, sort_keys=True).encode("utf-8"),
            flags=os.XATTR_REPLACE,
        )
    return claim


def retire_single_pv_claim(hostvol_mnt, volume_id):
    """Tombstone a deleted pool so stale clients can never cross tenants."""
    claim = read_single_pv_claim(hostvol_mnt)
    if claim is None:
        return
    if claim["volume_id"] != volume_id:
        raise SinglePVPoolClaimedError(claim["volume_id"])
    if claim["state"] == "active":
        raise ValueError("Single-PV ownership was not marked deleting")
    claim["state"] = "retired"
    os.setxattr(
        hostvol_mnt,
        SINGLE_PV_CLAIM_XATTR,
        json.dumps(claim, sort_keys=True).encode("utf-8"),
        flags=os.XATTR_REPLACE,
    )


def purge_single_pv_pool(hostvol_mnt):
    """Delete all data in a dedicated pool without deleting its mount root."""
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])
    with os.scandir(hostvol_mnt) as entries:
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    shutil.rmtree(entry.path)
                else:
                    os.unlink(entry.path)
            except FileNotFoundError:
                continue

    # Never release ownership while concurrently-created data remains. A retry
    # can purge it after the workload has stopped writing to the deleted PV.
    with os.scandir(hostvol_mnt) as entries:
        if next(entries, None) is not None:
            raise OSError(errno.ENOTEMPTY, "single-PV pool is not empty")


def archive_pv_reservation(hostvol, old_name, archived_name, size):
    """Move accounting to the archived name without freeing its capacity."""
    mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
    retry_errors(os.statvfs, [mntdir], [ENOTCONN])
    with statfile_lock:
        with SizeAccounting(hostvol, mntdir) as acc:
            acc.rename_pv_record(old_name, archived_name, size)


def _metadata_path_matches(path, expected):
    """Return whether a metadata path is exactly the expected regular file."""
    return (
        os.path.abspath(path) == os.path.abspath(expected)
        and os.path.realpath(path) == os.path.realpath(expected)
        and not os.path.islink(path)
    )


def _path_parent_is_contained(path, root):
    """Return whether a path's resolved parent remains below a trusted root."""
    try:
        return os.path.commonpath((
            os.path.realpath(os.path.dirname(path)),
            os.path.realpath(root),
        )) == os.path.realpath(root)
    except ValueError:
        return False


def _legacy_archive_details(hostvol_mnt, metadata_path, metadata):
    """Return canonical legacy archive details, or None for an active PV.

    Old releases represented an archive only by prepending ``archived-`` to
    the filename while retaining the original volume's hash directories. A
    live CSI volume is allowed to begin with the same prefix, so the prefix by
    itself is never enough to authorize cleanup.
    """
    state = metadata.get("state")
    legacy_reclaim = (
        state == "reclaiming"
        and metadata.get("legacy_archive") is True
    )
    if (
            state is not None
            and not legacy_reclaim
            or metadata.get("archive_name") is not None):
        return None

    filename = os.path.basename(metadata_path)
    if not filename.endswith(".json"):
        return None
    archived_name = filename[:-len(".json")]
    if not archived_name.startswith(ARCHIVE_PREFIX):
        return None

    original_volume_id = archived_name[len(ARCHIVE_PREFIX):]
    if not is_safe_path_component(original_volume_id):
        return None
    if (
            legacy_reclaim
            and metadata.get("original_volume_id") != original_volume_id):
        raise ValueError("Legacy archive tombstone has an invalid identity")

    for pvtype in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK):
        original_path = get_volume_path(
            pvtype,
            get_volname_hash(original_volume_id),
            original_volume_id,
        )
        legacy_prefix = os.path.dirname(original_path)
        active_prefix = os.path.dirname(get_volume_path(
            pvtype,
            get_volname_hash(archived_name),
            archived_name,
        ))
        # With only the old four-hex-digit directory hash, a collision is
        # indistinguishable from a legitimate active `archived-*` volume. Fail
        # closed and leave it for an administrator rather than deleting data.
        if legacy_prefix == active_prefix:
            continue
        if metadata.get("path_prefix") != legacy_prefix:
            continue

        expected_metadata = os.path.join(
            hostvol_mnt,
            "info",
            legacy_prefix,
            f"{archived_name}.json",
        )
        if not _metadata_path_matches(metadata_path, expected_metadata):
            continue
        payload_path = os.path.join(
            hostvol_mnt,
            legacy_prefix,
            archived_name,
        )
        if (
                not _path_parent_is_contained(
                    expected_metadata,
                    os.path.join(hostvol_mnt, "info"),
                )
                or not _path_parent_is_contained(payload_path, hostvol_mnt)):
            raise ValueError("Legacy archive path escapes its storage pool")
        return {
            "archive_name": archived_name,
            "metadata_path": expected_metadata,
            "original_volume_id": original_volume_id,
            "payload_path": payload_path,
            "legacy": True,
        }

    return None


def archive_metadata_details(hostvol_mnt, metadata_path, metadata):
    """Return canonical archive paths and identity, or None for an active PV."""
    state = metadata.get("state")
    if metadata.get("legacy_archive") is True:
        return _legacy_archive_details(hostvol_mnt, metadata_path, metadata)
    if state not in ("archived", "archiving", "reclaiming"):
        return _legacy_archive_details(hostvol_mnt, metadata_path, metadata)

    archived_name = metadata.get("archive_name")
    original_volume_id = metadata.get("original_volume_id")
    if (
            not is_safe_path_component(archived_name)
            or not archived_name.startswith(ARCHIVE_PREFIX)
            or not is_valid_csi_identifier(original_volume_id)):
        raise ValueError("Archive metadata contains an invalid identity")

    expected_metadata = os.path.join(
        hostvol_mnt,
        "info",
        "archive",
        f"{archived_name}.json",
    )
    if not _metadata_path_matches(metadata_path, expected_metadata):
        # `archiving` metadata remains at the active path until the payload and
        # accounting moves have committed. It is not cleanup-authorizing yet.
        if state == "archiving":
            return None
        raise ValueError(
            "Conflicting archive metadata is outside its canonical path"
        )
    original_prefixes = {
        os.path.dirname(get_volume_path(
            pvtype,
            get_volname_hash(original_volume_id),
            original_volume_id,
        ))
        for pvtype in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK)
    }
    valid_prefixes = {"archive"}
    if state == "archiving":
        # The metadata rename is the archive commit point. If a client dies
        # immediately after it, the canonical record still contains its old
        # active prefix and must remain discoverable for a retry.
        valid_prefixes.update(original_prefixes)
    if metadata.get("path_prefix") not in valid_prefixes:
        raise ValueError("Archive metadata contains an invalid path prefix")
    payload_path = os.path.join(hostvol_mnt, "archive", archived_name)
    if (
            not _path_parent_is_contained(
                expected_metadata,
                os.path.join(hostvol_mnt, "info"),
            )
            or not _path_parent_is_contained(payload_path, hostvol_mnt)):
        raise ValueError("Archive path escapes its storage pool")
    return {
        "archive_name": archived_name,
        "metadata_path": expected_metadata,
        "original_volume_id": original_volume_id,
        "payload_path": payload_path,
        "legacy": False,
    }


# pylint: disable=too-many-locals
def _capacity_record_identity(
        hostvol_mnt, metadata_path, metadata, filename):
    """Return a committed reservation identity and any legacy alias."""
    filename_volume_id = filename[:-len(".json")]
    archived_name = metadata.get("archive_name")
    original_volume_id = metadata.get("original_volume_id")

    if archived_name is not None:
        if (
                not is_safe_path_component(archived_name)
                or not archived_name.startswith(ARCHIVE_PREFIX)
                or not is_valid_csi_identifier(original_volume_id)):
            raise ValueError("PV metadata contains an invalid archive identity")
        state = metadata.get("state")
        if state not in ("archiving", "archived", "reclaiming"):
            raise ValueError("PV metadata contains an invalid archive state")
        archive = archive_metadata_details(
            hostvol_mnt,
            metadata_path,
            metadata,
        )
        if archive is None:
            if state != "archiving":
                raise ValueError("PV archive metadata is not canonical")
            pvtypes = (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK)
            declared_pvtype = metadata.get("pvtype")
            if declared_pvtype is not None:
                if declared_pvtype not in pvtypes:
                    raise ValueError("Prepared archive has an invalid PV type")
                pvtypes = (declared_pvtype,)
            canonical_active = False
            for pvtype in pvtypes:
                original_path = get_volume_path(
                    pvtype,
                    get_volname_hash(original_volume_id),
                    original_volume_id,
                )
                expected_active_metadata = os.path.join(
                    hostvol_mnt,
                    "info",
                    f"{original_path}.json",
                )
                if (
                        _metadata_path_matches(
                            metadata_path,
                            expected_active_metadata,
                        )
                        and metadata.get("path_prefix")
                        == os.path.dirname(original_path)):
                    canonical_active = True
                    break
            if not canonical_active:
                raise ValueError("Prepared archive metadata is not canonical")
        return archived_name, original_volume_id

    volume_id = metadata.get("volume_id", filename_volume_id)
    legacy_alias = None
    legacy_archive = _legacy_archive_details(
        hostvol_mnt,
        metadata_path,
        metadata,
    )
    if legacy_archive is not None:
        # Releases before unique archive identities stored metadata as
        # archived-<original ID>, but left stat.db under the original ID.
        volume_id = legacy_archive["archive_name"]
        legacy_alias = legacy_archive["original_volume_id"]

    if not is_valid_csi_identifier(volume_id):
        raise ValueError("PV metadata contains an invalid volume ID")
    return volume_id, legacy_alias


def _delete_tombstone_payload_path(
        hostvol_mnt, metadata_path, metadata, volume_id):
    """Return the canonical payload guarded by a delete tombstone."""
    state = metadata.get("state")
    if state == "reclaiming":
        archive = archive_metadata_details(
            hostvol_mnt,
            metadata_path,
            metadata,
        )
        if archive is None:
            raise ValueError("Archive reclaim tombstone is not canonical")
        return archive["payload_path"]

    if state != "deleting":
        raise ValueError("PV metadata is not a delete tombstone")

    candidate_paths = set()
    volume_hash = get_volname_hash(volume_id)
    for pvtype in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK):
        volume_paths = {get_volume_path(pvtype, volume_hash, volume_id)}
        legacy_path = get_legacy_volume_path(pvtype, volume_hash, volume_id)
        if legacy_path is not None:
            volume_paths.add(legacy_path)
        for volume_path in volume_paths:
            expected_metadata = os.path.join(
                hostvol_mnt,
                "info",
                f"{volume_path}.json",
            )
            if (
                    _metadata_path_matches(metadata_path, expected_metadata)
                    and metadata.get("path_prefix")
                    == os.path.dirname(volume_path)):
                candidate_paths.add(os.path.join(hostvol_mnt, volume_path))

    if len(candidate_paths) != 1:
        raise ValueError("Delete tombstone is outside its canonical path")
    payload_path = candidate_paths.pop()
    if not _path_parent_is_contained(payload_path, hostvol_mnt):
        raise ValueError("Delete tombstone payload escapes its storage pool")
    return payload_path


def _payload_exists(path):
    """Check a canonical payload without hiding backend errors as absence."""
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    return True


# pylint: disable=too-many-locals,too-many-branches,too-many-statements
def _capacity_metadata_records(hostvol_mnt):
    """Return durable reservations, obsolete aliases, and release tombstones."""
    info_dir = os.path.join(hostvol_mnt, "info")
    if not os.path.isdir(info_dir):
        return {}, {}, set()

    reservations = {}
    aliases = {}
    tombstones = set()
    seen_ids = set()
    creation_intents = {}
    expansion_intents = {}
    for root, _dirs, files in os.walk(info_dir):
        for filename in files:
            metadata_path = os.path.join(root, filename)
            if filename.endswith(".create-intent"):
                with open(metadata_path, encoding="utf-8") as intent_file:
                    intent = json.load(intent_file)
                try:
                    volume_id, reserved_size = _validate_creation_intent(
                        hostvol_mnt,
                        metadata_path,
                        intent,
                    )
                except ValueError as err:
                    raise ValueError(f"{err}: {metadata_path}") from err
                if volume_id in creation_intents:
                    raise ValueError(
                        "Duplicate creation intent for volume ID: "
                        f"{volume_id}"
                    )
                creation_intents[volume_id] = (intent, reserved_size)
                continue
            if filename.endswith(".expand-intent"):
                with open(metadata_path, encoding="utf-8") as intent_file:
                    intent = json.load(intent_file)
                try:
                    volume_id, target_size = _validate_expansion_intent(
                        hostvol_mnt,
                        metadata_path,
                        intent,
                    )
                except ValueError as err:
                    raise ValueError(f"{err}: {metadata_path}") from err
                if volume_id in expansion_intents:
                    raise ValueError(
                        "Duplicate expansion intent for volume ID: "
                        f"{volume_id}"
                    )
                expansion_intents[volume_id] = (intent, target_size)
                continue
            if not filename.endswith(".json"):
                continue

            with open(metadata_path, encoding="utf-8") as metadata_file:
                metadata = json.load(metadata_file)

            size = metadata.get("size")
            if (
                    not isinstance(size, int)
                    or isinstance(size, bool)
                    or size <= 0):
                raise ValueError(
                    f"PV metadata contains an invalid size: {metadata_path}"
                )

            try:
                volume_id, legacy_alias = _capacity_record_identity(
                    hostvol_mnt,
                    metadata_path,
                    metadata,
                    filename,
                )
            except ValueError as err:
                raise ValueError(f"{err}: {metadata_path}") from err

            state = metadata.get("state")
            if volume_id in seen_ids:
                raise ValueError(
                    "Duplicate PV metadata for volume ID: "
                    f"{volume_id}"
                )
            seen_ids.add(volume_id)
            if state in ("deleting", "reclaiming"):
                payload_path = _delete_tombstone_payload_path(
                    hostvol_mnt,
                    metadata_path,
                    metadata,
                    volume_id,
                )
                if not _payload_exists(payload_path):
                    tombstones.add(volume_id)
                    if legacy_alias is not None:
                        tombstones.add(legacy_alias)
                    continue

            reservations[volume_id] = size
            if legacy_alias is not None:
                aliases.setdefault(volume_id, set()).add(legacy_alias)

    for volume_id, (_intent, reserved_size) in creation_intents.items():
        if volume_id in tombstones:
            continue
        committed_size = reservations.get(volume_id)
        if committed_size is not None and committed_size != reserved_size:
            raise ValueError(
                "Creation intent does not match committed metadata for "
                f"volume ID: {volume_id}"
            )
        # The intent is the durable admission commit point. It reserves the
        # exact final size even when the process has not created the payload or
        # committed discoverable PV metadata yet.
        reservations[volume_id] = reserved_size

    for volume_id, (intent, target_size) in expansion_intents.items():
        if volume_id in tombstones:
            continue
        committed_size = reservations.get(volume_id)
        if (
                committed_size is not None
                and committed_size not in (
                    intent["from_size"],
                    intent["to_size"],
                )):
            raise ValueError(
                "Expansion intent does not match committed metadata for "
                f"volume ID: {volume_id}"
            )
        # An intent is itself a durable reservation. If metadata is temporarily
        # missing, leaking the target reservation is safer than admitting an
        # overlapping volume while recovery is investigated.
        reservations[volume_id] = target_size

    return reservations, aliases, tombstones


def _committed_capacity_records(hostvol_mnt):
    """Return durable reservations and obsolete accounting aliases."""
    reservations, aliases, _tombstones = _capacity_metadata_records(hostvol_mnt)
    return reservations, aliases


def _rebuild_committed_capacity(accounting, hostvol_mnt):
    """Overlay durable PV metadata onto the rebuildable stat.db index."""
    reservations, aliases, tombstones = _capacity_metadata_records(hostvol_mnt)
    committed_ids = set(reservations)
    for volume_id in sorted(tombstones - committed_ids):
        accounting.remove_pv_record(volume_id)
    for volume_id, size in reservations.items():
        obsolete_aliases = sorted(
            alias
            for alias in aliases.get(volume_id, set())
            if alias not in committed_ids
        )
        if obsolete_aliases:
            accounting.rename_pv_record(
                obsolete_aliases[0],
                volume_id,
                size,
            )
            for alias in obsolete_aliases[1:]:
                accounting.remove_pv_record(alias)

        if accounting.get_pv_size(volume_id) != size:
            logging.warning(logf(
                "Repairing PV capacity accounting before admission",
                pvname=volume_id,
                committed_size=size,
            ))
            accounting.update_pv_record(volume_id, size)


def _reconcile_hosting_volume_capacity(accounting, hostvol_mnt, total_size):
    """Rebuild one pool's capacity index or mark that pool unavailable."""
    try:
        accounting.update_summary(total_size)
        _rebuild_committed_capacity(accounting, hostvol_mnt)
    except (OSError, sqlite3.Error, ValueError) as err:
        raise HostingVolumeReconciliationError(str(err)) from err


def _hosting_volume_capacity_stats(accounting):
    """Read validated pool capacity or mark that pool unavailable."""
    try:
        return accounting.get_stats()
    except (sqlite3.Error, ValueError) as err:
        raise HostingVolumeReconciliationError(str(err)) from err


@contextmanager
def _selection_size_accounting(hostvol, hostvol_mnt):
    """Open capacity accounting while classifying pool-local open failures."""
    accounting = SizeAccounting(hostvol, hostvol_mnt)
    entered = False
    try:
        with accounting as opened_accounting:
            entered = True
            yield opened_accounting
    except sqlite3.Error as err:
        if entered:
            raise
        if accounting.conn is not None:
            accounting.conn.close()
        raise HostingVolumeReconciliationError(str(err)) from err


@contextmanager
def _selection_capacity_transaction(accounting):
    """Begin admission while classifying a pre-mutation SQLite lock failure."""
    entered = False
    try:
        with accounting.immediate_transaction():
            entered = True
            yield
    except sqlite3.Error as err:
        if entered:
            raise
        raise HostingVolumeReconciliationError(str(err)) from err


def release_archived_pv_reservation(
        hostvol, archived_name, original_volume_id=None):
    """Release an archive while preserving any recreated active volume."""
    del original_volume_id  # Rebuild resolves legacy aliases from one snapshot.
    mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
    retry_errors(os.statvfs, [mntdir], [ENOTCONN])
    with statfile_lock:
        with SizeAccounting(hostvol, mntdir) as accounting:
            # BEGIN IMMEDIATE orders this metadata snapshot against accounting
            # writes from a concurrent CreateVolume in another process. The
            # creator either commits first and is visible here, or writes its
            # new reservation after this release commits.
            with accounting.immediate_transaction():
                _rebuild_committed_capacity(accounting, mntdir)
                accounting.remove_pv_record(archived_name)


def _admit_volume_creation(
        accounting, hostvol_mnt, total_size, creation):
    """Atomically check capacity and persist one create reservation."""
    volume_id, pvtype, required_size = creation
    _reconcile_hosting_volume_capacity(
        accounting,
        hostvol_mnt,
        total_size,
    )

    volpath = get_volume_path(
        pvtype,
        get_volname_hash(volume_id),
        volume_id,
    )
    existing = read_volume_creation_intent(
        hostvol_mnt,
        volume_id,
        pvtype,
        volpath,
    )
    if existing is not None:
        if not _creation_intent_matches(
                existing,
                volume_id,
                pvtype,
                volpath,
                required_size):
            raise ValueError(
                "Pending create does not match the requested identity, type, "
                "path, or size"
            )
        return True

    # A reservation for this name without its exact intent is committed PV
    # metadata, a conflicting intent at another type/path, or conservative
    # stale accounting. None authorizes a new payload mutation.
    if accounting.get_pv_size(volume_id) != 0:
        raise ValueError(
            "Volume capacity is reserved without a matching creation intent"
        )

    pv_stats = _hosting_volume_capacity_stats(accounting)
    reserved_size = (
        pv_stats["free_size_bytes"] * RESERVED_SIZE_PERCENTAGE / 100
    )
    if required_size >= pv_stats["free_size_bytes"] - reserved_size:
        return False

    # BEGIN IMMEDIATE in the caller orders this intent and its absolute SQLite
    # record against every other controller process admitting on this pool.
    prepare_volume_creation(
        hostvol_mnt,
        volume_id,
        pvtype,
        volpath,
        required_size,
    )
    accounting.update_pv_record(volume_id, required_size)
    return True


def mount_and_select_hosting_volume(
        pv_hosting_volumes, required_size, volume_id=None, pvtype=None):
    """Mount each hosting volume to find available space"""
    if (volume_id is None) != (pvtype is None):
        raise ValueError("Creation admission requires both volume ID and PV type")
    failures = []
    for volume in pv_hosting_volumes:
        if get_single_pv_per_pool(volume):
            logging.warning(logf(
                "Skipping dedicated pool for shared PV provisioning",
                hostvol=volume.get("name"),
            ))
            continue
        hvol = volume['name']
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hvol)
        try:
            mount_glusterfs(volume, mntdir)
        except (CommandException, OSError, ValueError) as err:
            failures.append((hvol, err))
            logging.warning(logf(
                "Skipping unavailable hosting volume",
                hostvol=hvol,
                stage="mount",
                error=err,
            ))
            continue

        with statfile_lock:
            # Stat done before `os.path.exists` to prevent ignoring
            # file not exists even in case of ENOTCONN
            try:
                mntdir_stat = retry_errors(
                    os.statvfs,
                    [mntdir],
                    [ENOTCONN],
                )
            except OSError as err:
                failures.append((hvol, err))
                logging.warning(logf(
                    "Skipping unavailable hosting volume",
                    hostvol=hvol,
                    stage="stat",
                    error=err,
                ))
                continue

            try:
                with _selection_size_accounting(hvol, mntdir) as acc:
                    total_size = (
                        mntdir_stat.f_blocks * mntdir_stat.f_frsize
                    )
                    if volume_id is not None:
                        with _selection_capacity_transaction(acc):
                            if _admit_volume_creation(
                                    acc,
                                    mntdir,
                                    total_size,
                                    (volume_id, pvtype, required_size)):
                                return hvol
                        continue

                    _reconcile_hosting_volume_capacity(
                        acc,
                        mntdir,
                        total_size,
                    )
                    pv_stats = _hosting_volume_capacity_stats(acc)
                    reserved_size = (
                        pv_stats["free_size_bytes"]
                        * RESERVED_SIZE_PERCENTAGE / 100
                    )
            except HostingVolumeReconciliationError as err:
                failures.append((hvol, err))
                logging.warning(logf(
                    "Skipping unavailable hosting volume",
                    hostvol=hvol,
                    stage="capacity-reconciliation",
                    error=err,
                ))
                continue

            logging.debug(logf(
                "pv stats",
                hostvol=hvol,
                total_size_bytes=pv_stats["total_size_bytes"],
                used_size_bytes=pv_stats["used_size_bytes"],
                free_size_bytes=pv_stats["free_size_bytes"],
                number_of_pvs=pv_stats["number_of_pvs"],
                required_size=required_size,
                reserved_size=reserved_size
            ))

            if required_size < (pv_stats["free_size_bytes"] - reserved_size):
                return hvol

    if failures:
        raise HostingVolumeSelectionError(failures)
    return None


def mount_single_pv_hosting_volume(
        pv_hosting_volumes, required_size=0, limit_size=0):
    """Mount one dedicated pool and return its actual usable capacity."""
    dedicated = [
        volume for volume in pv_hosting_volumes
        if get_single_pv_per_pool(volume)
    ]
    eligible = [
        volume for volume in dedicated
        if volume.get("single_pv_claim_version") == SINGLE_PV_CLAIM_VERSION
    ]
    if dedicated and not eligible:
        raise LegacySinglePVPoolUnclaimedError(
            "legacy single-PV pool has no authoritative ownership marker"
        )
    if len(eligible) != 1:
        raise ValueError(
            "single_pv_per_pool requires exactly one hosting volume"
        )

    volume = eligible[0]
    mntdir = os.path.join(HOSTVOL_MOUNTDIR, volume["name"])
    mount_glusterfs(volume, mntdir)
    stats = retry_errors(os.statvfs, [mntdir], [ENOTCONN])
    capacity = stats.f_blocks * stats.f_frsize
    if capacity < required_size or (limit_size and capacity > limit_size):
        raise UnsupportedCapacityRangeError(
            "dedicated pool capacity is outside the requested range"
        )
    return volume["name"], capacity


def _volume_creation_intent(
        volname, pvtype, volpath, size, incarnation=None):
    """Return the exact durable reservation for one volume create."""
    intent = {
        "path": volpath,
        "pvtype": pvtype,
        "size": size,
        "version": CREATION_INTENT_VERSION,
        "volume_id": volname,
    }
    if incarnation is not None:
        intent["incarnation"] = incarnation
    return intent


def _creation_intent_path(hostvol_mnt, volpath):
    """Return the durable admission record for one volume create."""
    return os.path.join(
        hostvol_mnt,
        "info",
        f"{volpath}.create-intent",
    )


def _block_creation_intent(volname, pvtype, volpath, size):
    """Return the version-1 creation intent used by legacy block callers."""
    return _volume_creation_intent(volname, pvtype, volpath, size)


def _block_creation_intent_path(hostvol_mnt, volpath):
    """Return the shared creation-intent path for legacy block callers."""
    return _creation_intent_path(hostvol_mnt, volpath)


def _expansion_intent_path(hostvol_mnt, volpath):
    """Return the durable capacity reservation for one expansion."""
    return os.path.join(
        hostvol_mnt,
        "info",
        f"{volpath}.expand-intent",
    )


def _expansion_intent(volume_id, pvtype, volpath, from_size, to_size):
    """Return the exact identity and sizes authorized for one expansion."""
    return {
        "from_size": from_size,
        "path": volpath,
        "pvtype": pvtype,
        "to_size": to_size,
        "version": EXPANSION_INTENT_VERSION,
        "volume_id": volume_id,
    }


def _canonical_volume_paths(volume_id, pvtype):
    """Return canonical and safe legacy paths for one volume identity."""
    volume_hash = get_volname_hash(volume_id)
    paths = {get_volume_path(pvtype, volume_hash, volume_id)}
    legacy_path = get_legacy_volume_path(pvtype, volume_hash, volume_id)
    if legacy_path is not None:
        paths.add(legacy_path)
    return paths


def _positive_int(value):
    """Return whether a capacity value is a positive non-boolean integer."""
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _canonical_uuid(value):
    """Return whether a value is one canonical UUID string."""
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _creation_intent_matches(intent, volume_id, pvtype, volpath, size):
    """Return whether one validated intent has the requested identity."""
    return intent == _volume_creation_intent(
        volume_id,
        pvtype,
        volpath,
        size,
        intent.get("incarnation"),
    )


def validate_committed_creation(metadata, intent):
    """Require committed metadata to belong to the pending generation."""
    if (
            not isinstance(metadata, dict)
            or not isinstance(intent, dict)
            or metadata.get("size") != intent.get("size")):
        raise ValueError("Creation intent does not match committed metadata")

    metadata_incarnation = metadata.get("incarnation")
    intent_incarnation = intent.get("incarnation")
    # Version-1 reservations and metadata did not carry an incarnation. Keep
    # those upgrade-compatible, while never allowing two modern generations
    # with the same CSI ID to finalize each other's metadata.
    if (
            metadata_incarnation is not None
            and intent_incarnation is not None
            and metadata_incarnation != intent_incarnation):
        raise ValueError(
            "Creation intent belongs to a different volume generation"
        )


def _validate_creation_intent(hostvol_mnt, intent_path, intent):
    """Validate a create reservation and return its identity and size."""
    if not isinstance(intent, dict):
        raise ValueError("Creation intent must be an object")
    volume_id = intent.get("volume_id")
    pvtype = intent.get("pvtype")
    volpath = intent.get("path")
    size = intent.get("size")
    incarnation = intent.get("incarnation")
    if (
            intent.get("version") != CREATION_INTENT_VERSION
            or not is_valid_csi_identifier(volume_id)
            or pvtype not in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK)
            or not _positive_int(size)):
        raise ValueError("Creation intent contains invalid identity or size")
    if incarnation is not None and not _canonical_uuid(incarnation):
        raise ValueError("Creation intent contains an invalid incarnation")

    canonical_path = get_volume_path(
        pvtype,
        get_volname_hash(volume_id),
        volume_id,
    )
    if volpath != canonical_path:
        raise ValueError("Creation intent volume path is not canonical")
    expected_path = _creation_intent_path(hostvol_mnt, canonical_path)
    if not _metadata_path_matches(intent_path, expected_path):
        raise ValueError("Creation intent is outside its canonical path")
    return volume_id, size


def _creation_intent_records(hostvol_mnt):
    """Return all validated create reservations in one storage pool."""
    info_dir = os.path.join(hostvol_mnt, "info")
    if not os.path.isdir(info_dir):
        return {}

    intents = {}
    for root, _dirs, files in os.walk(info_dir):
        for filename in files:
            if not filename.endswith(".create-intent"):
                continue
            intent_path = os.path.join(root, filename)
            with open(intent_path, encoding="utf-8") as intent_file:
                intent = json.load(intent_file)
            volume_id, _size = _validate_creation_intent(
                hostvol_mnt,
                intent_path,
                intent,
            )
            if volume_id in intents:
                raise ValueError(
                    "Duplicate creation intent for volume ID: "
                    f"{volume_id}"
                )
            intents[volume_id] = intent
    return intents


def read_volume_creation_intent(hostvol_mnt, volume_id, pvtype, volpath):
    """Return a validated pending create for one volume, if present."""
    intent_path = _creation_intent_path(hostvol_mnt, volpath)
    intent = _read_optional_json(intent_path)
    if intent is None:
        return None
    _validate_creation_intent(hostvol_mnt, intent_path, intent)
    if not _creation_intent_matches(
            intent, volume_id, pvtype, volpath, intent.get("size")):
        raise ValueError("Creation intent does not match the volume")
    return intent


def prepare_volume_creation(hostvol_mnt, volume_id, pvtype, volpath, size):
    """Durably reserve an exact volume create before payload mutation."""
    expected_identity = _volume_creation_intent(
        volume_id,
        pvtype,
        volpath,
        size,
    )
    canonical_path = get_volume_path(
        pvtype,
        get_volname_hash(volume_id),
        volume_id,
    )
    if volpath != canonical_path or not _positive_int(size):
        raise ValueError("Creation intent contains invalid path or size")

    intent_path = _creation_intent_path(hostvol_mnt, volpath)
    makedirs(os.path.dirname(intent_path))
    existing = _read_optional_json(intent_path)
    if existing is not None:
        _validate_creation_intent(hostvol_mnt, intent_path, existing)
        if not _creation_intent_matches(
                existing, volume_id, pvtype, volpath, size):
            raise ValueError(
                "Pending create does not match the requested identity, type, "
                "path, or size"
            )
        return existing

    expected = {
        **expected_identity,
        "incarnation": str(uuid.uuid4()),
    }
    try:
        _atomic_create_json(intent_path, expected)
    except FileExistsError:
        existing = _read_optional_json(intent_path)
        _validate_creation_intent(hostvol_mnt, intent_path, existing)
        if not _creation_intent_matches(
                existing, volume_id, pvtype, volpath, size):
            raise ValueError(
                "Pending create does not match the requested identity, type, "
                "path, or size"
            ) from None
        return existing
    return expected


def finish_volume_creation(hostvol_mnt, volume_id, pvtype, volpath, intent):
    """Remove an exact create intent after authoritative metadata commits."""
    current = read_volume_creation_intent(
        hostvol_mnt,
        volume_id,
        pvtype,
        volpath,
    )
    if current != intent:
        raise ValueError("Pending create changed before finalization")
    _durable_unlink(_creation_intent_path(hostvol_mnt, volpath))


def _validate_expansion_intent(hostvol_mnt, intent_path, intent):
    """Validate an expansion reservation and return its volume and target."""
    if not isinstance(intent, dict):
        raise ValueError("Expansion intent must be an object")
    volume_id = intent.get("volume_id")
    pvtype = intent.get("pvtype")
    volpath = intent.get("path")
    from_size = intent.get("from_size")
    to_size = intent.get("to_size")
    valid_identity = (
        intent.get("version") == EXPANSION_INTENT_VERSION
        and is_valid_csi_identifier(volume_id)
        and pvtype in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK)
        and volpath in _canonical_volume_paths(volume_id, pvtype)
    )
    valid_sizes = (
        _positive_int(from_size)
        and _positive_int(to_size)
        and to_size > from_size
    )
    if not valid_identity or not valid_sizes:
        raise ValueError("Expansion intent contains invalid identity or sizes")
    expected_path = _expansion_intent_path(hostvol_mnt, volpath)
    if not _metadata_path_matches(intent_path, expected_path):
        raise ValueError("Expansion intent is outside its canonical path")
    return volume_id, to_size


def read_volume_expansion_intent(hostvol_mnt, volume_id, pvtype, volpath):
    """Return a validated pending expansion for one volume, if present."""
    intent_path = _expansion_intent_path(hostvol_mnt, volpath)
    intent = _read_optional_json(intent_path)
    if intent is None:
        return None
    _validate_expansion_intent(hostvol_mnt, intent_path, intent)
    if (
            intent.get("volume_id") != volume_id
            or intent.get("pvtype") != pvtype
            or intent.get("path") != volpath):
        raise ValueError("Expansion intent does not match the volume")
    return intent


# pylint: disable=too-many-arguments,too-many-positional-arguments
def prepare_volume_expansion(
        hostvol_mnt, volume_id, pvtype, volpath, from_size, to_size):
    """Durably reserve an exact target size before increasing its quota."""
    if volpath not in _canonical_volume_paths(volume_id, pvtype):
        raise ValueError("Expansion volume path is not canonical")
    metadata_path = os.path.join(hostvol_mnt, "info", f"{volpath}.json")
    metadata = _read_optional_json(metadata_path)
    if not isinstance(metadata, dict):
        raise ValueError("Expansion metadata is missing")
    if (
            metadata.get("size") not in (from_size, to_size)
            or metadata.get("path_prefix") != os.path.dirname(volpath)
            or metadata.get("volume_id", volume_id) != volume_id
            or metadata.get("state") is not None):
        raise ValueError("Expansion metadata does not match the request")

    expected = _expansion_intent(
        volume_id,
        pvtype,
        volpath,
        from_size,
        to_size,
    )
    intent_path = _expansion_intent_path(hostvol_mnt, volpath)
    makedirs(os.path.dirname(intent_path))
    try:
        _atomic_create_json(intent_path, expected)
    except FileExistsError:
        existing = _read_optional_json(intent_path)
        if existing != expected:
            raise ValueError(
                "Pending expansion does not match the requested identity or sizes"
            ) from None
    return expected


def reserve_volume_expansion(hostvol, volume_id, expansion):
    """Atomically admit and durably reserve one absolute expansion target."""
    pvtype, volpath, from_size, to_size = expansion
    if not _positive_int(from_size) or not _positive_int(to_size):
        raise ValueError("Expansion sizes must be positive integers")
    if to_size <= from_size:
        raise ValueError("Expansion target must be larger than its source")

    hostvol_mnt = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
    with statfile_lock:
        mount_stats = retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])
        total_size = mount_stats.f_blocks * mount_stats.f_frsize
        with SizeAccounting(hostvol, hostvol_mnt) as accounting:
            with accounting.immediate_transaction():
                accounting.update_summary(total_size)
                _rebuild_committed_capacity(accounting, hostvol_mnt)

                existing = read_volume_expansion_intent(
                    hostvol_mnt,
                    volume_id,
                    pvtype,
                    volpath,
                )
                expected = _expansion_intent(
                    volume_id,
                    pvtype,
                    volpath,
                    from_size,
                    to_size,
                )
                if existing is not None:
                    if existing != expected:
                        raise ValueError(
                            "Pending expansion does not match the requested "
                            "identity or sizes"
                        )
                    accounting.update_pv_record(volume_id, to_size)
                    return existing

                committed_size = accounting.get_pv_size(volume_id)
                if committed_size == to_size:
                    # Another controller may have committed metadata and
                    # removed its intent after this caller observed the old
                    # state. Recreate the exact intent so this retry can
                    # harmlessly reapply the absolute quota and finalize it.
                    intent = prepare_volume_expansion(
                        hostvol_mnt,
                        volume_id,
                        pvtype,
                        volpath,
                        from_size,
                        to_size,
                    )
                    accounting.update_pv_record(volume_id, to_size)
                    return intent
                if committed_size != from_size:
                    raise ValueError(
                        "Committed capacity does not match the expansion source"
                    )

                stats = accounting.get_stats()
                reserved_size = (
                    stats["free_size_bytes"]
                    * RESERVED_SIZE_PERCENTAGE / 100
                )
                additional_size = to_size - from_size
                if additional_size >= (
                        stats["free_size_bytes"] - reserved_size):
                    return None

                intent = prepare_volume_expansion(
                    hostvol_mnt,
                    volume_id,
                    pvtype,
                    volpath,
                    from_size,
                    to_size,
                )
                accounting.update_pv_record(volume_id, to_size)
                return intent


def finish_volume_expansion(hostvol_mnt, volume_id, pvtype, volpath, intent):
    """Remove an exact expansion intent after metadata and accounting commit."""
    current = read_volume_expansion_intent(
        hostvol_mnt,
        volume_id,
        pvtype,
        volpath,
    )
    if current != intent:
        raise ValueError("Pending expansion changed before finalization")
    _durable_unlink(_expansion_intent_path(hostvol_mnt, volpath))


def _read_optional_json(path):
    """Read JSON or return None only when the path is absent."""
    try:
        with open(path, encoding="utf-8") as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        return None


def volume_incarnation(volume):
    """Return immutable evidence for one observed volume generation."""
    if volume is None:
        return None
    if volume.single_pv_per_pool:
        return (
            "single-pv",
            volume.hostvol,
            volume.volname,
            volume.extra.get("single_pv_claim_version"),
            volume.extra.get("single_pv_claim_state"),
            volume.size,
        )
    if volume.extra.get("state") == "creating":
        return (
            "creating",
            volume.hostvol,
            volume.voltype,
            volume.volpath,
            json.dumps(
                volume.extra.get("creation_intent"),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    metadata_path = volume.extra.get(
        "metadata_path",
        os.path.join(
            HOSTVOL_MOUNTDIR,
            volume.hostvol,
            "info",
            f"{volume.volpath}.json",
        ),
    )
    try:
        with open(metadata_path, "rb") as metadata_file:
            metadata = metadata_file.read()
    except FileNotFoundError:
        # Some API unit tests inject a synthetic committed Volume without a
        # backing metadata file. Production search_volume results always carry
        # a real path, so this identity is only a compatibility fallback.
        return (
            "synthetic",
            volume.hostvol,
            volume.voltype,
            volume.volpath,
            volume.size,
        )
    return (
        "metadata",
        volume.hostvol,
        volume.voltype,
        volume.volpath,
        hashlib.sha256(metadata).hexdigest(),
    )


def _validate_block_metadata(metadata, volpath, size):
    """Fail closed when committed metadata does not match a create retry."""
    if (
            not isinstance(metadata, dict)
            or metadata.get("size") != size
            or metadata.get("path_prefix") != os.path.dirname(volpath)):
        raise ValueError("Block volume metadata does not match the request")


def _require_regular_backing(path):
    """Reject links and non-files before treating backing data as a volume."""
    backing_stat = os.lstat(path)
    if not stat.S_ISREG(backing_stat.st_mode):
        raise ValueError("Block volume backing path is not a regular file")


def create_block_volume(pvtype, hostvol_mnt, volname, size):
    """Create or safely resume a virtual/raw block volume."""
    if pvtype not in (PV_TYPE_RAWBLOCK, PV_TYPE_VIRTBLOCK):
        raise ValueError("Block volume type is not supported")
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise ValueError("Block volume size must be a positive integer")

    volhash = get_volname_hash(volname)
    volpath = get_volume_path(pvtype, volhash, volname)
    volpath_full = os.path.join(hostvol_mnt, volpath)
    logging.debug(logf(
        "Volume hash",
        volhash=volhash
    ))

    # Check for mount availability before creating virtblock volume
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])

    # Create the hashed data and metadata parents. Directory creation alone is
    # not authorization to modify an existing backing file.
    makedirs(os.path.dirname(volpath_full))
    logging.debug(logf(
        f"Created {pvtype} directory",
        path=os.path.dirname(volpath)
    ))

    metadata_path = os.path.join(hostvol_mnt, "info", f"{volpath}.json")
    metadata = _read_optional_json(metadata_path)
    intent = read_volume_creation_intent(
        hostvol_mnt,
        volname,
        pvtype,
        volpath,
    )

    if metadata is not None:
        _validate_block_metadata(metadata, volpath, size)
        _require_regular_backing(volpath_full)
        if intent is not None:
            validate_committed_creation(metadata, intent)
            finish_volume_creation(
                hostvol_mnt,
                volname,
                pvtype,
                volpath,
                intent,
            )
    else:
        backing_exists = os.path.lexists(volpath_full)
        if backing_exists and intent is None:
            raise FileExistsError(
                errno.EEXIST,
                "Block backing exists without a matching creation intent",
                volpath_full,
            )

        if intent is None:
            intent = prepare_volume_creation(
                hostvol_mnt,
                volname,
                pvtype,
                volpath,
                size,
            )

        flags = os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW
        volpath_fd = os.open(volpath_full, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(volpath_fd).st_mode):
                raise ValueError(
                    "Block volume backing path is not a regular file"
                )
            os.ftruncate(volpath_fd, size)
            os.fsync(volpath_fd)
        finally:
            os.close(volpath_fd)
        logging.debug(logf(
            "Truncated file to required size",
            path=volpath,
            size=size
        ))

        if pvtype == PV_TYPE_VIRTBLOCK:
            # TODO: Multiple FS support based on volume_capability mount option
            # A matching creation intent proves this backing file has never
            # been committed or published. Force makes a retry idempotent if a
            # prior mkfs completed immediately before the process crashed.
            execute(MKFS_XFS_CMD, "-f", volpath_full)
            logging.debug(logf(
                "Created Filesystem",
                path=volpath,
                command=MKFS_XFS_CMD
            ))

        save_pv_metadata(
            hostvol_mnt,
            volpath,
            size,
            incarnation=intent.get("incarnation"),
        )
        finish_volume_creation(
            hostvol_mnt,
            volname,
            pvtype,
            volpath,
            intent,
        )

    return Volume(
        volname=volname,
        voltype=pvtype,
        volhash=volhash,
        hostvol=os.path.basename(hostvol_mnt),
        size=size,
        volpath=volpath,
    )


def _atomic_write_json(path, data):
    """Durably replace JSON metadata without exposing partial contents."""
    parent = os.path.dirname(path)
    temporary_path = os.path.join(
        parent,
        f".{os.path.basename(path)}.{os.getpid()}.{uuid.uuid4().hex}.tmp",
    )
    try:
        with open(temporary_path, "x", encoding="utf-8") as info_file:
            json.dump(data, info_file, sort_keys=True)
            info_file.flush()
            os.fsync(info_file.fileno())
        os.replace(temporary_path, path)
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass


def _atomic_create_json(path, data):
    """Durably create JSON while refusing to replace an existing file."""
    parent = os.path.dirname(path)
    temporary_path = os.path.join(
        parent,
        f".{os.path.basename(path)}.{os.getpid()}.{uuid.uuid4().hex}.tmp",
    )
    try:
        with open(temporary_path, "x", encoding="utf-8") as info_file:
            json.dump(data, info_file, sort_keys=True)
            info_file.flush()
            os.fsync(info_file.fileno())
        os.link(temporary_path, path)
        directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass


def _durable_unlink(path):
    """Remove a file and durably record its absence in the parent."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        return

    parent = os.path.dirname(path)
    directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def save_pv_metadata(hostvol_mnt, pvpath, pvsize, incarnation=None):
    """Save PV metadata in info file"""
    # Create info dir if not exists
    info_file_path = os.path.join(hostvol_mnt, "info", pvpath)
    info_file_dir = os.path.dirname(info_file_path)

    retry_errors(makedirs, [info_file_dir], [ENOTCONN])
    logging.debug(logf(
        "Created metadata directory",
        metadata_dir=info_file_dir
    ))

    metadata = {
        "incarnation": incarnation or str(uuid.uuid4()),
        "size": pvsize,
        "path_prefix": os.path.dirname(pvpath),
    }
    component = os.path.basename(pvpath)
    if component.startswith(".kadalu~"):
        metadata["volume_id"] = volume_name_from_component(component)
    _atomic_write_json(info_file_path + ".json", metadata)
    logging.debug(logf(
        "Metadata saved",
        metadata_file=info_file_path,
    ))


def _wait_for_simple_quota(hostvol_mnt, volpath, size, operation):
    """Wait until statvfs exposes the requested simple-quota size."""
    pvsize_buffer = size * 0.05  # 5%
    pvsize_min = size - pvsize_buffer
    pvsize_max = size + pvsize_buffer
    logging.debug(logf(
        "Watching df of pv directory",
        pvdir=volpath,
        pvsize_buffer=pvsize_buffer,
    ))

    volpath_full = os.path.join(hostvol_mnt, volpath)
    for count in range(1, 7):
        pvstat = retry_errors(os.statvfs, [volpath_full], [ENOTCONN])
        volsize = pvstat.f_blocks * pvstat.f_frsize
        if pvsize_min < volsize < pvsize_max:
            logging.debug(logf(
                "Quota set successfully",
                operation=operation,
                volsize=volsize,
                pvsize=size,
                num_tries=count,
            ))
            return

        if count < 6:
            time.sleep(1)

    raise TimeoutError(
        "Simple quota %s did not expose requested size %s for %s; got %s"
        % (operation, size, volpath, volsize)
    )


def _set_simple_quota(
        hostvol_mnt, volpath, size, initialize_namespace=False):
    """Apply and verify the simple-quota xattrs for a subvolume."""
    volpath_full = os.path.join(hostvol_mnt, volpath)
    if initialize_namespace:
        retry_errors(
            os.setxattr,
            [
                volpath_full,
                "trusted.glusterfs.namespace",
                b"true",
            ],
            [ENOTCONN],
        )

    retry_errors(
        os.setxattr,
        [
            volpath_full,
            "trusted.gfs.squota.limit",
            str(size).encode(),
        ],
        [ENOTCONN],
    )
    operation = "creation" if initialize_namespace else "expansion"
    _wait_for_simple_quota(hostvol_mnt, volpath, size, operation)


def create_subdir_volume(
        hostvol_mnt, volname, size, use_gluster_quota,
        save_metadata=True):
    """Create or safely resume a quota-backed subdirectory volume."""
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise ValueError("Subvolume size must be a positive integer")
    volhash = get_volname_hash(volname)
    volpath = get_volume_path(PV_TYPE_SUBVOL, volhash, volname)
    volpath_full = os.path.join(hostvol_mnt, volpath)
    metadata_path = os.path.join(hostvol_mnt, "info", f"{volpath}.json")
    logging.debug(logf(
        "Volume hash",
        volhash=volhash
    ))

    # Check for mount availability before creating subdir volume
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])

    metadata = _read_optional_json(metadata_path)
    intent = read_volume_creation_intent(
        hostvol_mnt,
        volname,
        PV_TYPE_SUBVOL,
        volpath,
    )
    if metadata is not None:
        _validate_block_metadata(metadata, volpath, size)
        payload_stat = os.lstat(volpath_full)
        if not stat.S_ISDIR(payload_stat.st_mode):
            raise ValueError("Subvolume payload path is not a directory")
        if intent is not None:
            if not _creation_intent_matches(
                    intent,
                    volname,
                    PV_TYPE_SUBVOL,
                    volpath,
                    size):
                raise ValueError("Creation intent does not match committed metadata")
            validate_committed_creation(metadata, intent)
            finish_volume_creation(
                hostvol_mnt,
                volname,
                PV_TYPE_SUBVOL,
                volpath,
                intent,
            )
        return Volume(
            volname=volname,
            voltype=PV_TYPE_SUBVOL,
            volhash=volhash,
            hostvol=os.path.basename(hostvol_mnt),
            size=size,
            volpath=volpath,
        )

    if os.path.lexists(volpath_full) and intent is None:
        raise FileExistsError(
            errno.EEXIST,
            "Subvolume payload exists without a matching creation intent",
            volpath_full,
        )
    if intent is None:
        intent = prepare_volume_creation(
            hostvol_mnt,
            volname,
            PV_TYPE_SUBVOL,
            volpath,
            size,
        )
    elif not _creation_intent_matches(
            intent,
            volname,
            PV_TYPE_SUBVOL,
            volpath,
            size):
        raise ValueError(
            "Pending create does not match the requested identity, type, "
            "path, or size"
        )

    # The durable intent above is the authorization to create or resume this
    # exact payload. It also keeps the requested bytes reserved across crashes.
    makedirs(volpath_full)
    logging.debug(logf(
        "Created PV directory",
        pvdir=volpath
    ))

    if use_gluster_quota is True:
        # The caller must apply the remote quota and only then persist metadata.
        return Volume(
            volname=volname,
            voltype=PV_TYPE_SUBVOL,
            volhash=volhash,
            hostvol=os.path.basename(hostvol_mnt),
            size=size,
            volpath=volpath,
        )

    # Quota must be observable before metadata makes the volume discoverable.
    # If quota application or verification fails, a retry can safely repeat it.
    _set_simple_quota(
        hostvol_mnt,
        volpath,
        size,
        initialize_namespace=True,
    )
    if save_metadata:
        save_pv_metadata(
            hostvol_mnt,
            volpath,
            size,
            incarnation=intent.get("incarnation"),
        )
        finish_volume_creation(
            hostvol_mnt,
            volname,
            PV_TYPE_SUBVOL,
            volpath,
            intent,
        )

    return Volume(
        volname=volname,
        voltype=PV_TYPE_SUBVOL,
        volhash=volhash,
        hostvol=os.path.basename(hostvol_mnt),
        size=size,
        volpath=volpath,
    )


def is_hosting_volume_free(
        hostvol, requested_pvsize, volume_id=None, pvtype=None):
    """Check if host volume is free to expand or create (external)volume"""

    if (volume_id is None) != (pvtype is None):
        raise ValueError("Creation admission requires both volume ID and PV type")

    mntdir = os.path.join(HOSTVOL_MOUNTDIR, hostvol)
    with statfile_lock:

        # Stat done before `os.path.exists` to prevent ignoring
        # file not exists even in case of ENOTCONN
        mntdir_stat = retry_errors(os.statvfs, [mntdir], [ENOTCONN])
        with SizeAccounting(hostvol, mntdir) as acc:
            total_size = mntdir_stat.f_blocks * mntdir_stat.f_frsize
            if volume_id is not None:
                with acc.immediate_transaction():
                    return _admit_volume_creation(
                        acc,
                        mntdir,
                        total_size,
                        (volume_id, pvtype, requested_pvsize),
                    )

            acc.update_summary(total_size)
            _rebuild_committed_capacity(acc, mntdir)
            pv_stats = acc.get_stats()
            reserved_size = (
                pv_stats["free_size_bytes"] * RESERVED_SIZE_PERCENTAGE / 100
            )

        logging.debug(logf(
            "pv stats",
            hostvol=hostvol,
            total_size_bytes=pv_stats["total_size_bytes"],
            used_size_bytes=pv_stats["used_size_bytes"],
            free_size_bytes=pv_stats["free_size_bytes"],
            number_of_pvs=pv_stats["number_of_pvs"],
            required_size=requested_pvsize,
            reserved_size=reserved_size
        ))

        if requested_pvsize < (pv_stats["free_size_bytes"] - reserved_size):
            return True

        return False


def update_subdir_volume(
        hostvol_mnt, _hostvoltype, volname, expansion_requested_pvsize,
        update_metadata=True):
    """Update sub directory Volume"""

    volhash = get_volname_hash(volname)
    volpath = _existing_or_canonical_volume_path(
        hostvol_mnt,
        PV_TYPE_SUBVOL,
        volhash,
        volname,
    )
    logging.debug(logf(
        "Volume hash",
        volhash=volhash
    ))

    # Check for mount availability before updating subdir volume
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])

    # Create a subdir
    makedirs(os.path.join(hostvol_mnt, volpath))
    logging.debug(logf(
        "Updated PV directory",
        pvdir=volpath
    ))

    # External Kadalu-format pools without SSH credentials use the same
    # simple-quota interface as native pools.
    _set_simple_quota(
        hostvol_mnt,
        volpath,
        expansion_requested_pvsize,
    )
    if update_metadata:
        update_pv_metadata(
            hostvol_mnt,
            volpath,
            expansion_requested_pvsize,
        )

    return Volume(
        volname=volname,
        voltype=PV_TYPE_SUBVOL,
        volhash=volhash,
        hostvol=os.path.basename(hostvol_mnt),
        size=expansion_requested_pvsize,
        volpath=volpath,
    )


def update_block_volume(pvtype, hostvol_mnt, volname, expansion_requested_pvsize):
    """Update block volume"""

    volhash = get_volname_hash(volname)
    volpath = _existing_or_canonical_volume_path(
        hostvol_mnt,
        pvtype,
        volhash,
        volname,
    )
    volpath_full = os.path.join(hostvol_mnt, volpath)
    logging.debug(logf(
        "Volume hash",
        volhash=volhash
    ))

    # Check for mount availability before updating virtblock volume
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])

    # Update the file with required size
    makedirs(os.path.dirname(volpath_full))
    logging.debug(logf(
        "Updated virtblock directory",
        path=os.path.dirname(volpath)
    ))

    volpath_fd = os.open(volpath_full, os.O_CREAT | os.O_RDWR)
    os.close(volpath_fd)
    os.truncate(volpath_full, expansion_requested_pvsize)

    logging.debug(logf(
        "Truncated file to required size",
        path=volpath,
        size=expansion_requested_pvsize
    ))

    update_pv_metadata(hostvol_mnt, volpath, expansion_requested_pvsize)
    return Volume(
        volname=volname,
        voltype=pvtype,
        volhash=volhash,
        hostvol=os.path.basename(hostvol_mnt),
        size=expansion_requested_pvsize,
        volpath=volpath,
    )


def _existing_or_canonical_volume_path(
        hostvol_mnt, pvtype, volhash, volname):
    """Prefer an existing legacy metadata path over a new encoded path."""
    canonical = get_volume_path(pvtype, volhash, volname)
    candidates = [canonical]
    legacy = get_legacy_volume_path(pvtype, volhash, volname)
    if legacy is not None and legacy != canonical:
        candidates.append(legacy)

    for candidate in candidates:
        metadata_path = os.path.join(
            hostvol_mnt,
            "info",
            f"{candidate}.json",
        )
        if os.path.isfile(metadata_path) and not os.path.islink(metadata_path):
            return candidate
    return canonical


def update_pv_metadata(hostvol_mnt, pvpath, expansion_requested_pvsize):
    """Update PV metadata in info file"""

    # Create info dir if not exists
    info_file_path = os.path.join(hostvol_mnt, "info", pvpath)
    info_file_dir = os.path.dirname(info_file_path)

    retry_errors(makedirs, [info_file_dir], [ENOTCONN])
    logging.debug(logf(
        "Updated metadata directory",
        metadata_dir=info_file_dir
    ))

    # Update existing PV contents
    with open(info_file_path + ".json", "r") as info_file:
        data = json.load(info_file)

    # Update PV contents
    data["size"] = expansion_requested_pvsize
    data["path_prefix"] = os.path.dirname(pvpath)

    # Atomically commit the new authoritative size.
    _atomic_write_json(info_file_path + ".json", data)

    logging.debug(logf(
        "Metadata updated",
        metadata_file=info_file_path
    ))


def _prune_empty_parents(path, stop):
    """Remove empty hash directories without hiding storage I/O failures."""
    parent = os.path.dirname(path)
    while parent != stop:
        try:
            os.rmdir(parent)
        except FileNotFoundError:
            pass
        except OSError as err:
            if err.errno in (errno.ENOTEMPTY, errno.EEXIST):
                break
            raise
        parent = os.path.dirname(parent)


def _rename_for_archive(source, destination):
    """Perform or resume one side of an archive rename."""
    if os.path.abspath(source) == os.path.abspath(destination):
        if not os.path.lexists(destination):
            raise FileNotFoundError(destination)
        return
    source_exists = os.path.lexists(source)
    destination_exists = os.path.lexists(destination)
    if source_exists and destination_exists:
        raise FileExistsError(
            errno.EEXIST,
            "archive destination already exists",
            destination,
        )
    if destination_exists:
        return
    if not source_exists:
        raise FileNotFoundError(source)
    try:
        os.rename(source, destination)
    except FileNotFoundError:
        if not os.path.lexists(destination):
            raise
    _fsync_directory(os.path.dirname(destination))
    source_parent = os.path.dirname(source)
    if os.path.abspath(source_parent) != os.path.abspath(
            os.path.dirname(destination)):
        _fsync_directory(source_parent)


def _fsync_directory(path):
    """Durably record directory-entry changes below a mounted pool."""
    directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _archive_paths(vol, archived_name):
    """Return payload and metadata destinations for an archive identity."""
    hostvol_mnt = os.path.join(HOSTVOL_MOUNTDIR, vol.hostvol)
    return (
        os.path.join(hostvol_mnt, "archive", archived_name),
        os.path.join(hostvol_mnt, "info", "archive", f"{archived_name}.json"),
    )


def _prepare_archive(vol, info_file_path, metadata):
    """Persist and return one unique archive identity before moving data."""
    archived_name = metadata.get("archive_name")
    if archived_name is not None:
        if (
                not is_safe_path_component(archived_name)
                or not archived_name.startswith(ARCHIVE_PREFIX)
                or metadata.get("original_volume_id") != vol.volname
                or metadata.get("state") != "archiving"):
            raise ValueError("Volume metadata contains an invalid archive identity")
        return archived_name, metadata

    for _attempt in range(10):
        candidate = f"{ARCHIVE_PREFIX}{uuid.uuid4().hex}"
        archived_payload_path, archived_info_path = _archive_paths(
            vol,
            candidate,
        )
        if not (
                os.path.lexists(archived_payload_path)
                or os.path.lexists(archived_info_path)):
            archived_name = candidate
            break
    else:
        raise FileExistsError("Unable to allocate a unique archive identity")

    prepared_metadata = metadata.copy()
    prepared_metadata.update({
        "archive_name": archived_name,
        "original_volume_id": vol.volname,
        "state": "archiving",
    })
    _atomic_write_json(info_file_path, prepared_metadata)
    return archived_name, prepared_metadata


def _archive_volume(vol, info_file_path, metadata, size):
    """Archive payload, accounting, and metadata in retry-safe order."""
    archived_name, metadata = _prepare_archive(
        vol,
        info_file_path,
        metadata,
    )
    payload_path = os.path.join(HOSTVOL_MOUNTDIR, vol.hostvol, vol.volpath)
    archived_payload_path, archived_info_path = _archive_paths(
        vol,
        archived_name,
    )
    makedirs(os.path.dirname(archived_payload_path))
    makedirs(os.path.dirname(archived_info_path))

    _rename_for_archive(payload_path, archived_payload_path)
    archive_pv_reservation(
        vol.hostvol,
        vol.volname,
        archived_name,
        size,
    )
    # Rename, rather than copy/create/unlink, so there is exactly one durable
    # reservation-authorizing metadata record at every commit point.
    _rename_for_archive(info_file_path, archived_info_path)
    archived_metadata = metadata.copy()
    archived_metadata.update({
        "path_prefix": "archive",
        "state": "archived",
    })
    _atomic_write_json(archived_info_path, archived_metadata)
    _prune_empty_parents(
        payload_path,
        os.path.join(HOSTVOL_MOUNTDIR, vol.hostvol, vol.voltype),
    )
    _prune_empty_parents(
        info_file_path,
        os.path.join(HOSTVOL_MOUNTDIR, vol.hostvol, "info", vol.voltype),
    )
    logging.info(logf(
        "Volume archived",
        old_volname=vol.volname,
        new_archived_volname=archived_name,
        volpath=archived_payload_path,
    ))


def _delete_single_pv_volume(vol, hostvol_mnt, storage_data):
    """Purge a whole-pool PV after durably blocking further publishes."""
    with single_pv_operation_lock(hostvol_mnt):
        _delete_single_pv_volume_locked(vol, hostvol_mnt, storage_data)


def _delete_single_pv_volume_locked(vol, hostvol_mnt, storage_data):
    """Complete a whole-pool delete while its operation lock is held."""
    claim = read_single_pv_claim(hostvol_mnt)
    if claim is None:
        if not ensure_single_pv_claim(
                hostvol_mnt,
                vol.volname,
                storage_data.get("legacy_single_pv_volume_id")):
            raise ValueError("Single-PV pool ownership could not be verified")
    elif claim["volume_id"] != vol.volname:
        raise SinglePVPoolClaimedError(claim["volume_id"])

    begin_single_pv_delete(hostvol_mnt, vol.volname)
    purge_single_pv_pool(hostvol_mnt)
    retire_single_pv_claim(hostvol_mnt, vol.volname)
    logging.info(logf(
        "Single-PV pool ownership retired",
        volname=vol.volname,
        hostvol=vol.hostvol,
    ))


def _delete_pending_creation(vol, hostvol_mnt):
    """Cancel an uncommitted create without releasing another volume's data."""
    intent = read_volume_creation_intent(
        hostvol_mnt,
        vol.volname,
        vol.voltype,
        vol.volpath,
    )
    if intent is None or intent != vol.extra.get("creation_intent"):
        raise ValueError("Pending create changed before deletion")

    canonical_path = get_volume_path(
        vol.voltype,
        get_volname_hash(vol.volname),
        vol.volname,
    )
    payload_path = os.path.join(hostvol_mnt, canonical_path)
    metadata_path = os.path.join(
        hostvol_mnt,
        "info",
        f"{canonical_path}.json",
    )
    if (
            vol.volpath != canonical_path
            or not _path_parent_is_contained(payload_path, hostvol_mnt)):
        raise ValueError("Pending create payload path is not canonical")
    if os.path.lexists(metadata_path):
        raise ValueError("Pending create has committed metadata")

    try:
        if vol.voltype == PV_TYPE_SUBVOL:
            shutil.rmtree(payload_path)
        else:
            os.unlink(payload_path)
    except FileNotFoundError:
        pass

    # Keep the intent visible until the SQLite removal and intent unlink are
    # ordered under one cross-process write transaction. A crash before commit
    # can leak capacity, but can never expose bytes for another create early.
    with statfile_lock:
        with SizeAccounting(vol.hostvol, hostvol_mnt) as accounting:
            with accounting.immediate_transaction():
                _rebuild_committed_capacity(accounting, hostvol_mnt)
                accounting.remove_pv_record(vol.volname)
                finish_volume_creation(
                    hostvol_mnt,
                    vol.volname,
                    vol.voltype,
                    vol.volpath,
                    intent,
                )


def _delete_managed_volume(vol, hostvol_mnt, reclaim_policy):
    """Delete or archive a metadata-backed subvolume or block volume."""
    payload_path = os.path.join(hostvol_mnt, vol.volpath)
    info_file_path = vol.extra.get(
        "metadata_path",
        os.path.join(hostvol_mnt, "info", f"{vol.volpath}.json"),
    )
    with open(info_file_path, encoding="utf-8") as info_file:
        metadata = json.load(info_file)
    expansion_intent = read_volume_expansion_intent(
        hostvol_mnt,
        vol.volname,
        vol.voltype,
        vol.volpath,
    )
    if expansion_intent is not None:
        if metadata.get("size") not in (
                expansion_intent["from_size"],
                expansion_intent["to_size"]):
            raise ValueError("Expansion intent does not match volume metadata")
        # Reclaim can safely over-reserve an expansion that had not reached its
        # quota step. Make normal metadata authoritative before dropping the
        # intent, so every subsequent delete/archive crash point remains safe.
        metadata = metadata.copy()
        metadata["size"] = expansion_intent["to_size"]
        _atomic_write_json(info_file_path, metadata)
        finish_volume_expansion(
            hostvol_mnt,
            vol.volname,
            vol.voltype,
            vol.volpath,
            expansion_intent,
        )
    size = metadata.get("size")
    if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
        raise ValueError("Volume metadata contains an invalid size")

    creation_intent = read_volume_creation_intent(
        hostvol_mnt,
        vol.volname,
        vol.voltype,
        vol.volpath,
    )
    if creation_intent is not None:
        validate_committed_creation(metadata, creation_intent)
        # Committed metadata remains capacity-authoritative at every later
        # archive/delete crash point, so the admission intent is now redundant.
        finish_volume_creation(
            hostvol_mnt,
            vol.volname,
            vol.voltype,
            vol.volpath,
            creation_intent,
        )

    if reclaim_policy == "archive":
        _archive_volume(vol, info_file_path, metadata, size)
        return

    canonical_volpath = get_volume_path(
        vol.voltype,
        get_volname_hash(vol.volname),
        vol.volname,
    )
    expected_volpaths = {canonical_volpath}
    legacy_volpath = get_legacy_volume_path(
        vol.voltype,
        get_volname_hash(vol.volname),
        vol.volname,
    )
    if legacy_volpath is not None:
        expected_volpaths.add(legacy_volpath)
    if (
            vol.volpath not in expected_volpaths
            or not _path_parent_is_contained(payload_path, hostvol_mnt)
            or not _metadata_path_matches(
                info_file_path,
                os.path.join(
                    hostvol_mnt,
                    "info",
                    f"{vol.volpath}.json",
                ),
            )):
        raise ValueError("Volume delete paths are not canonical")

    state = metadata.get("state")
    if state is None:
        metadata = metadata.copy()
        metadata.update({
            "state": "deleting",
            "volume_id": vol.volname,
        })
        _atomic_write_json(info_file_path, metadata)
    elif (
            state != "deleting"
            or metadata.get("volume_id") != vol.volname):
        raise ValueError("Volume metadata is not a valid delete tombstone")

    try:
        if vol.voltype == PV_TYPE_SUBVOL:
            shutil.rmtree(payload_path)
        else:
            os.unlink(payload_path)
    except FileNotFoundError:
        pass

    _prune_empty_parents(
        payload_path,
        os.path.join(hostvol_mnt, vol.voltype),
    )
    # Keep the durable tombstone visible while accounting is released. A
    # concurrent stat.db rebuild therefore removes, rather than resurrects,
    # this reservation after its payload is gone.
    update_free_size(vol.hostvol, vol.volname, size)
    _durable_unlink(info_file_path)
    _prune_empty_parents(
        info_file_path,
        os.path.join(hostvol_mnt, "info", vol.voltype),
    )

    logging.info(logf(
        "Volume deleted",
        volpath=payload_path,
        voltype=vol.voltype,
    ))


def _delete_volume_locked(vol, hostvol_mnt):
    """Complete one delete after its pool and identity are locked."""
    retry_errors(os.statvfs, [hostvol_mnt], [ENOTCONN])
    storage_filename = os.path.join(VOLINFO_DIR, f"{vol.hostvol}.info")
    with open(storage_filename, encoding="utf-8") as info_file:
        storage_data = json.load(info_file)
    reclaim_policy = storage_data.get("pvReclaimPolicy", "delete")
    if reclaim_policy not in VALID_RECLAIM_POLICIES:
        raise UnsupportedReclaimPolicyError(
            f"Unsupported reclaim policy: {reclaim_policy}"
        )
    if reclaim_policy == "retain":
        logging.info(logf(
            "'retain' reclaim policy, volume not deleted",
            volname=vol.volname,
            hostvol=vol.hostvol,
        ))
        return
    if vol.extra.get("state") == "creating":
        _delete_pending_creation(vol, hostvol_mnt)
        return
    if vol.single_pv_per_pool:
        if reclaim_policy == "archive":
            raise UnsupportedReclaimPolicyError(
                "archive is not supported for single-PV pools"
            )
        _delete_single_pv_volume(vol, hostvol_mnt, storage_data)
        return
    _delete_managed_volume(vol, hostvol_mnt, reclaim_policy)


def delete_volume(volname, lock_timeout=None):
    """Delete a volume while keeping data and accounting crash-consistent."""
    initial_volume = search_volume(volname)
    if initial_volume is None:
        logging.warning(logf("Volume not found for delete", volname=volname))
        return

    hostvol_mnt = os.path.join(HOSTVOL_MOUNTDIR, initial_volume.hostvol)
    if initial_volume.single_pv_per_pool:
        # Whole-pool ownership already uses the mounted root as its durable
        # distributed lock. Avoid reversing the CreateVolume root->volume lock
        # order when that pool also participates in global create admission.
        with single_pv_operation_lock(hostvol_mnt, timeout=lock_timeout):
            vol = search_volume(volname)
            if vol is None:
                return
            if (
                    not vol.single_pv_per_pool
                    or volume_incarnation(vol)
                    != volume_incarnation(initial_volume)):
                raise VolumeOperationConflictError(
                    "Volume was replaced while waiting for deletion"
                )
            _delete_volume_locked(vol, hostvol_mnt)
        return
    initial_incarnation = volume_incarnation(initial_volume)
    with volume_operation_lock(
            hostvol_mnt, volname, timeout=lock_timeout):
        # Search again under the distributed lifecycle lock. An expansion or
        # an earlier delete may have committed since the first pool lookup.
        vol = search_volume(volname)
        if vol is None:
            logging.warning(logf(
                "Volume disappeared while waiting to delete",
                volname=volname,
            ))
            return
        if vol.hostvol != initial_volume.hostvol:
            raise ValueError("Volume hosting pool changed during deletion")
        if volume_incarnation(vol) != initial_incarnation:
            raise VolumeOperationConflictError(
                "Volume was replaced while waiting for deletion"
            )
        _delete_volume_locked(vol, hostvol_mnt)


def _single_pv_volume(volume, mntdir, volname, volhash):
    """Return a matching whole-pool volume, including safe legacy claims."""
    claim = read_single_pv_claim(mntdir)
    if claim is None:
        legacy_volume_id = volume.get("legacy_single_pv_volume_id")
        if legacy_volume_id != volname:
            return None
        stats = retry_errors(os.statvfs, [mntdir], [ENOTCONN])
        claim = {
            "state": "active",
            "volume_id": legacy_volume_id,
            "size": stats.f_blocks * stats.f_frsize,
        }
    if claim["volume_id"] != volname:
        return None

    found = Volume(
        volname=volname,
        voltype=PV_TYPE_SUBVOL,
        volhash=volhash,
        hostvol=volume["name"],
        size=claim["size"],
        volpath="",
        single_pv_per_pool=True,
        hostvoltype=volume.get('type'),
        ghost=volume.get('g_host'),
        gvolname=volume.get('g_volname'),
        goptions=volume.get('g_options', ""),
        node_affinity=volume.get('node_affinity'),
    )
    found.extra["single_pv_claim_state"] = claim["state"]
    found.extra["single_pv_claim_version"] = volume.get(
        "single_pv_claim_version"
    )
    return found


def _metadata_volume(volume, mntdir, volname, volhash, info_paths):
    """Return the first metadata-backed volume matching a CSI ID."""
    for info_path in info_paths:
        info_path_full = os.path.join(mntdir, "info", info_path + ".json")
        try:
            with open(info_path_full, encoding="utf-8") as info_file:
                data = json.load(info_file)
        except FileNotFoundError:
            continue

        state = data.get("state")
        if state in ("archived", "reclaiming"):
            raise ValueError("Archive state found at an active metadata path")
        if state not in (None, "archiving", "deleting"):
            raise ValueError("Volume metadata contains an invalid state")

        metadata_volume_id = data.get("volume_id")
        if (
                metadata_volume_id is not None
                and metadata_volume_id != volname):
            raise ValueError("Volume metadata identity does not match its path")

        voltype = info_path.split(os.sep, maxsplit=1)[0]
        if voltype not in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK):
            raise ValueError("Volume metadata has an invalid volume type path")
        found = Volume(
            volname=volname,
            voltype=voltype,
            volhash=volhash,
            hostvol=volume["name"],
            size=data["size"],
            volpath=info_path,
            single_pv_per_pool=get_single_pv_per_pool(data),
            hostvoltype=volume.get('type'),
            ghost=volume.get('g_host'),
            gvolname=volume.get('g_volname'),
            goptions=volume.get('g_options', ""),
            node_affinity=volume.get('node_affinity'),
        )
        found.extra["state"] = state
        found.extra["metadata_path"] = info_path_full
        incarnation = data.get("incarnation")
        if incarnation is not None and not _canonical_uuid(incarnation):
            raise ValueError("Volume metadata has an invalid incarnation")
        found.extra["incarnation"] = incarnation
        return found
    return _transitional_archive_volume(
        volume,
        mntdir,
        volname,
        volhash,
        info_paths,
    )


def _pending_creation_volume(volume, mntdir, volname, volhash):
    """Return one validated, not-yet-committed volume create."""
    intent = _creation_intent_records(mntdir).get(volname)
    if intent is None:
        return None

    found = Volume(
        volname=volname,
        voltype=intent["pvtype"],
        volhash=volhash,
        hostvol=volume["name"],
        size=intent["size"],
        volpath=intent["path"],
        hostvoltype=volume.get('type'),
        ghost=volume.get('g_host'),
        gvolname=volume.get('g_volname'),
        goptions=volume.get('g_options', ""),
        node_affinity=volume.get('node_affinity'),
    )
    found.extra["state"] = "creating"
    found.extra["creation_intent"] = intent
    return found


def _transitional_archive_volume(
        volume, mntdir, volname, volhash, info_paths):
    """Find an archive handoff interrupted after its metadata rename."""
    archive_dir = os.path.join(mntdir, "info", "archive")
    if not os.path.isdir(archive_dir):
        return None

    found = None
    for entry in os.scandir(archive_dir):
        if not entry.name.endswith(".json") or not entry.is_file(
                follow_symlinks=False):
            continue
        with open(entry.path, encoding="utf-8") as info_file:
            data = json.load(info_file)
        if (
                data.get("state") != "archiving"
                or data.get("original_volume_id") != volname):
            continue
        archive_metadata_details(mntdir, entry.path, data)

        pvtype = data.get("pvtype")
        if pvtype not in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK):
            matching_paths = [
                info_path
                for info_path in info_paths
                if os.path.dirname(info_path) == data.get("path_prefix")
            ]
            if len(matching_paths) != 1:
                raise ValueError(
                    "Interrupted archive metadata has no valid volume type"
                )
            volpath = matching_paths[0]
            pvtype = volpath.split(os.sep, 1)[0]
        else:
            volpath = get_volume_path(pvtype, volhash, volname)

        if found is not None:
            raise ValueError(
                "Duplicate interrupted archives for volume ID: "
                f"{volname}"
            )
        found = Volume(
            volname=volname,
            voltype=pvtype,
            volhash=volhash,
            hostvol=volume["name"],
            size=data["size"],
            volpath=volpath,
            hostvoltype=volume.get("type"),
            ghost=volume.get("g_host"),
            gvolname=volume.get("g_volname"),
            goptions=volume.get("g_options", ""),
            node_affinity=volume.get("node_affinity"),
        )
        found.extra["state"] = "archiving"
        found.extra["metadata_path"] = entry.path
        found.extra["archive_name"] = data.get("archive_name")

    return found


def search_volume(volname):
    """Search for a Volume by name in all Hosting Volumes"""
    volhash = get_volname_hash(volname)
    paths = []
    for pvtype in (PV_TYPE_SUBVOL, PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK):
        canonical_path = get_volume_path(pvtype, volhash, volname)
        paths.append(canonical_path)
        legacy_path = get_legacy_volume_path(pvtype, volhash, volname)
        if legacy_path is not None and legacy_path != canonical_path:
            paths.append(legacy_path)

    host_volumes = get_pv_hosting_volumes({})
    found_volume = None
    for volume in host_volumes:
        hvol = volume['name']
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hvol)
        mount_glusterfs(volume, mntdir)
        # Check for mount availability before checking the info file
        retry_errors(os.statvfs, [mntdir], [ENOTCONN])

        if get_single_pv_per_pool(volume):
            found = _single_pv_volume(volume, mntdir, volname, volhash)
        else:
            found = _metadata_volume(
                volume,
                mntdir,
                volname,
                volhash,
                paths,
            )
        if found is not None:
            if found_volume is not None:
                raise ValueError(
                    "Duplicate volume identity across hosting volumes: "
                    f"{volname}"
                )
            found_volume = found
            continue

        if not get_single_pv_per_pool(volume):
            pending = _pending_creation_volume(
                volume,
                mntdir,
                volname,
                volhash,
            )
            if pending is not None:
                if found_volume is not None:
                    raise ValueError(
                        "Duplicate volume identity across hosting volumes: "
                        f"{volname}"
                    )
                found_volume = pending
    return found_volume


# TODO: Not being used, revisit and remove
def get_subdir_virtblock_vols(mntdir, volumes, pvtype):
    """Get virtual block and subdir volumes list"""
    for dir1 in os.listdir(os.path.join(mntdir, pvtype)):
        for dir2 in os.listdir(os.path.join(mntdir, pvtype, dir1)):
            for pvdir in os.listdir(os.path.join(mntdir, pvtype, dir2)):
                volumes.append(Volume(
                    volname=pvdir,
                    voltype=pvtype,
                    hostvol=os.path.basename(mntdir),
                    volpath=os.path.join(pvtype, dir1, dir2, pvdir)
                ))


# TODO: Not being used, revisit and remove
def volume_list(voltype=None):
    """List of Volumes"""
    host_volumes = get_pv_hosting_volumes({})
    volumes = []
    for volume in host_volumes:
        hvol = volume['name']
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hvol)
        mount_glusterfs(volume, mntdir)

        # Check for mount availability before listing the Volumes
        retry_errors(os.statvfs, [mntdir], [ENOTCONN])

        if voltype is None or voltype == PV_TYPE_SUBVOL:
            get_subdir_virtblock_vols(mntdir, volumes, PV_TYPE_SUBVOL)
        if voltype is None or voltype == PV_TYPE_VIRTBLOCK:
            get_subdir_virtblock_vols(mntdir, volumes, PV_TYPE_VIRTBLOCK)
        if voltype is None or voltype == PV_TYPE_RAWBLOCK:
            get_subdir_virtblock_vols(mntdir, volumes, PV_TYPE_RAWBLOCK)

    return volumes


def _validated_mount_flags(mount_flags):
    """Return argv-safe mount options accepted from CSI."""
    flags = list(mount_flags or [])
    driver_controlled = {
        "bind",
        "move",
        "private",
        "rbind",
        "remount",
        "rprivate",
        "rshared",
        "rslave",
        "runbindable",
        "rw",
        "ro",
        "shared",
        "slave",
        "unbindable",
    }
    for flag in flags:
        if (
                not isinstance(flag, str)
                or re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9._=:-]*",
                    flag,
                ) is None):
            raise ValueError("mount flags contain unsupported characters")
        if flag.split("=", maxsplit=1)[0].lower() in driver_controlled:
            raise ValueError(
                f"mount flag {flag!r} is controlled by the CSI driver"
            )
    return flags


def _remount_bind(mountpoint, readonly, mount_flags):
    """Apply bind-mount flags in the required second mount operation."""
    options = ["remount", "bind"]
    if readonly:
        options.append("ro")
    options.extend(mount_flags)
    if len(options) > 2:
        execute(MOUNT_CMD, "-o", ",".join(options), mountpoint)


def _mount_raw_block(pvpath, mountpoint, readonly, mount_flags):
    """Attach and bind-mount one raw-block backing file."""
    command = ["losetup", "-f", "--show"]
    if readonly:
        command.append("--read-only")
    command.append(pvpath)
    loop, _, _ = execute(*command)
    loop = loop.strip()

    makedirs(os.path.dirname(mountpoint))
    Path(mountpoint).touch(mode=0o777)
    bind_mounted = False
    try:
        execute(MOUNT_CMD, "--bind", loop, mountpoint)
        bind_mounted = True
        _remount_bind(mountpoint, readonly, mount_flags)
    except (CommandException, OSError):
        if bind_mounted:
            try:
                execute(UNMOUNT_CMD, "-l", mountpoint)
            except CommandException:
                logging.exception("Unable to roll back raw-block bind mount")
        execute("losetup", "-d", loop)
        raise


def _publish_state_path(mountpoint):
    """Return the host-side sidecar for one kubelet publish target."""
    return f"{mountpoint}.kadalu-publish.json"


def _publish_state(
        volume_id, pvtype, volume_path, host_volume_name,
        host_mount_identity):
    """Build the durable identity required for safe block-target recovery."""
    if None in (
            volume_id,
            volume_path,
            host_volume_name,
            host_mount_identity):
        return None
    return {
        "version": PUBLISH_STATE_VERSION,
        "volume_id": volume_id,
        "pvtype": pvtype,
        "volume_path": volume_path,
        "host_volume_name": host_volume_name,
        "host_mount_identity": host_mount_identity,
    }


def _publish_state_matches(
        mountpoint, expected, allow_mount_config_revision=False):
    """Return whether a root-owned sidecar authorizes stale-target recovery."""
    if expected is None:
        return False
    path = _publish_state_path(mountpoint)
    try:
        path_stat = os.lstat(path)
        if not stat.S_ISREG(path_stat.st_mode):
            return False
        with open(path, encoding="utf-8") as state_file:
            actual = json.load(state_file)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return False

    if actual == expected:
        return True
    if (
            not allow_mount_config_revision
            or not isinstance(actual, dict)
            or not isinstance(expected, dict)
            or not _same_mount_identity_generation(
                actual.get("host_mount_identity"),
                expected.get("host_mount_identity"),
            )):
        return False

    # Stable pool generation alone is insufficient. Every PV and target-path
    # field must remain exact; only the configuration fingerprint may differ.
    revision_adjusted = {
        **actual,
        "host_mount_identity": expected["host_mount_identity"],
    }
    return revision_adjusted == expected


def _record_publish_state(mountpoint, state_data):
    """Durably record a fully-established block publish target."""
    if state_data is not None:
        _atomic_write_json(_publish_state_path(mountpoint), state_data)


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-branches
def mount_volume(
        pvpath, mountpoint, pvtype, fstype=None, readonly=False,
        mount_flags=None, logical_source_path=None, volume_path=None,
        host_volume_name=None, host_mount_identity=None, volume_id=None,
        allow_legacy_host_mount=False):
    """Mount a Volume"""
    mount_flags = _validated_mount_flags(mount_flags)
    publish_state = _publish_state(
        volume_id,
        pvtype,
        volume_path,
        host_volume_name,
        host_mount_identity,
    )

    if _target_mount_entry(mountpoint) is not None:
        source_error = None
        try:
            if mounted_volume_matches(
                    pvpath,
                    mountpoint,
                    pvtype,
                    readonly=readonly,
                    mount_flags=mount_flags,
                    logical_source_path=logical_source_path,
                    volume_path=volume_path):
                _record_publish_state(mountpoint, publish_state)
                return True
            if _mounted_volume_source_matches(
                    pvpath,
                    mountpoint,
                    pvtype,
                    logical_source_path,
                    volume_path):
                raise MountTargetConflictError(
                    "mount target options do not match this request"
                )
        except OSError as err:
            if err.errno not in {
                    errno.EIO, errno.ENOTCONN, errno.ESTALE}:
                raise
            source_error = err

        if not _same_logical_stale_source(
                mountpoint,
                pvtype,
                logical_source_path,
                volume_path,
                host_volume_name,
                host_mount_identity,
                publish_state,
                allow_legacy_host_mount):
            if source_error is not None:
                raise source_error
            raise MountTargetConflictError(
                "mount target is occupied by an incompatible volume"
            )

        logging.warning(logf(
            "Rebuilding stale publish target after hosting mount recovery",
            mountpoint=mountpoint,
            volume_path=volume_path,
        ))
        unmount_volume(mountpoint)

    # CSI publish must never invent a missing controller-managed source.
    try:
        os.stat(pvpath)
    except FileNotFoundError:
        logging.error(logf("Volume source path does not exist", path=pvpath))
        return False

    if pvtype == PV_TYPE_RAWBLOCK:
        _mount_raw_block(pvpath, mountpoint, readonly, mount_flags)
        _record_publish_state(mountpoint, publish_state)
        return True

    # Need this after kube 1.20.0
    makedirs(mountpoint)

    if pvtype == PV_TYPE_VIRTBLOCK:
        fstype = "xfs" if fstype is None else fstype
        options = mount_flags + (["ro"] if readonly else [])
        command = [MOUNT_CMD, "-t", fstype]
        if options:
            command.extend(["-o", ",".join(options)])
        execute(*command, pvpath, mountpoint)
    else:
        execute(MOUNT_CMD, "--bind", pvpath, mountpoint)
        try:
            _remount_bind(mountpoint, readonly, mount_flags)
        except (CommandException, OSError):
            try:
                execute(UNMOUNT_CMD, "-l", mountpoint)
            except CommandException:
                logging.exception("Unable to roll back bind mount")
            raise

    if not readonly:
        os.chmod(mountpoint, 0o777)
    _record_publish_state(mountpoint, publish_state)
    return True


def _path_backing_identity(path):
    """Return the stable device and inode identity of a backing file."""
    path_stat = os.stat(path)
    return (
        path_stat.st_ino,
        os.major(path_stat.st_dev),
        os.minor(path_stat.st_dev),
    )


def _loop_backing_identity(loop_device):
    """Read a loop device's backing identity without reopening its path."""
    output, _, _ = execute(
        "losetup",
        "--noheadings",
        "--raw",
        "--output",
        "BACK-INO,BACK-MAJ:MIN",
        loop_device,
    )
    fields = output.split()
    if len(fields) != 2:
        return None

    device_fields = fields[1].split(":", maxsplit=1)
    if len(device_fields) != 2:
        return None
    try:
        return (
            int(fields[0]),
            int(device_fields[0]),
            int(device_fields[1]),
        )
    except ValueError:
        return None


def _loop_backing_path(loop_device):
    """Return a loop device's recorded backing pathname when available."""
    output, _, _ = execute(
        "losetup",
        "--noheadings",
        "--raw",
        "--output",
        "BACK-FILE",
        loop_device,
    )
    path = output.strip()
    if path.endswith(" (deleted)"):
        path = path[:-len(" (deleted)")]
    return os.path.normpath(path) if path else None


def _target_mount_source(mountpoint, pvtype):
    """Return source, type, and filesystem root without probing the target."""
    entry = _target_mount_entry(mountpoint)
    if entry is None:
        return None, None, None

    if pvtype == PV_TYPE_SUBVOL:
        return entry["source"], entry["fstype"], entry["root"]

    loop_device = _loop_device_from_mountinfo(entry)
    return loop_device, entry["fstype"], entry["root"]


# pylint: disable=too-many-return-statements
def _same_logical_stale_source(
        mountpoint, pvtype, logical_source_path, volume_path,
        host_volume_name=None, host_mount_identity=None,
        publish_state=None, allow_legacy_host_mount=False):
    """Return whether an incompatible target is the same logical old PV."""
    if logical_source_path is None or volume_path is None:
        return False

    source, fstype, source_root = _target_mount_source(mountpoint, pvtype)
    if source is None:
        return False

    if pvtype == PV_TYPE_SUBVOL:
        if fstype != "fuse.glusterfs":
            return False
        expected_root = "/" if not volume_path else f"/{volume_path}"
        if os.path.normpath(source_root) != os.path.normpath(expected_root):
            return False
        if (
                host_volume_name is not None
                and _gluster_volume_name(source) != host_volume_name):
            return False
        if host_mount_identity is not None:
            identity = _gluster_mount_identity(source)
            if identity is None:
                if not (
                        allow_legacy_host_mount
                        and source == f"kadalu:{host_volume_name}"):
                    return False
            elif identity != host_mount_identity:
                if not (
                        _same_mount_identity_generation(
                            identity,
                            host_mount_identity,
                        )
                        and _publish_state_matches(
                            mountpoint,
                            publish_state,
                            allow_mount_config_revision=True,
                        )):
                    return False
        return True

    if re.fullmatch(r"/dev/loop\d+", source) is None:
        return False
    if publish_state is not None:
        # A loop device does not expose the identity of the Gluster client
        # which originally opened its backing file. After a host remount, only
        # the durable node-owned sidecar can prove this is the same logical PV.
        return _publish_state_matches(
            mountpoint,
            publish_state,
            allow_mount_config_revision=True,
        )
    backing_path = _loop_backing_path(source)
    if backing_path is None:
        return False
    expected = os.path.normpath(logical_source_path)
    return backing_path == expected or backing_path.endswith(expected)


def _mounted_volume_source_matches(
        pvpath, mountpoint, pvtype,
        logical_source_path=None, volume_path=None):
    """Return whether an existing target has the exact current backing."""
    entry = _target_mount_entry(mountpoint)
    if entry is None:
        return False

    source_matches = False
    if pvtype == PV_TYPE_SUBVOL:
        if logical_source_path is not None and volume_path is not None:
            source_stat = os.stat(pvpath)
            source_device = "%s:%s" % (
                os.major(source_stat.st_dev),
                os.minor(source_stat.st_dev),
            )
            expected_root = "/" if not volume_path else f"/{volume_path}"
            source_matches = (
                entry["fstype"] == "fuse.glusterfs"
                and entry["device"] == source_device
                and os.path.normpath(entry["root"])
                == os.path.normpath(expected_root)
            )
        else:
            try:
                source_matches = os.path.samefile(pvpath, mountpoint)
            except FileNotFoundError:
                return False
    elif pvtype in (PV_TYPE_VIRTBLOCK, PV_TYPE_RAWBLOCK):
        try:
            loop_device = _loop_device_from_mountinfo(entry)
            if (
                    loop_device is not None
                    and re.fullmatch(r"/dev/loop\d+", loop_device)):
                source_matches = (
                    _loop_backing_identity(loop_device)
                    == _path_backing_identity(pvpath)
                )
        except (CommandException, FileNotFoundError):
            return False
    else:
        return False

    if not source_matches:
        return False

    return True


def mounted_volume_matches(
        pvpath, mountpoint, pvtype, readonly=False, mount_flags=None,
        logical_source_path=None, volume_path=None):
    """Return whether an existing target mount is backed by this PV."""
    if not _mounted_volume_source_matches(
            pvpath,
            mountpoint,
            pvtype,
            logical_source_path,
            volume_path):
        return False

    entry = _target_mount_entry(mountpoint)
    if entry is None:
        return False
    # Bind mounts may be read-only while their shared filesystem superblock is
    # writable. CSI publishes care about the per-mount VFS flags only.
    existing_options = entry["options"]
    existing_readonly = "ro" in existing_options and "rw" not in existing_options
    requested_options = set(_validated_mount_flags(mount_flags))
    return (
        existing_readonly == readonly
        and requested_options.issubset(existing_options)
    )


def _unescape_mount_field(value):
    """Decode the octal escapes used in procfs mount-table fields."""
    return re.sub(
        r"\\([0-7]{3})",
        lambda match: chr(int(match.group(1), 8)),
        value,
    )


def _read_mountinfo():
    """Return decoded mountinfo entries without resolving mounted paths."""
    entries = []
    with open(MOUNTINFO_FILE, encoding="utf-8") as mountinfo_file:
        for line in mountinfo_file:
            fields = line.split()
            try:
                separator = fields.index("-")
            except ValueError:
                continue
            if separator < 6 or len(fields) < separator + 4:
                continue
            entries.append({
                "device": fields[2],
                "root": os.path.normpath(_unescape_mount_field(fields[3])),
                "target": os.path.normpath(_unescape_mount_field(fields[4])),
                "options": set(fields[5].split(",")),
                "fstype": fields[separator + 1],
                "source": _unescape_mount_field(fields[separator + 2]),
                "super_options": set(fields[separator + 3].split(",")),
            })
    return entries


def _target_mount_entry(mountpoint):
    """Return the topmost exact mount-table entry for one target path."""
    expected_target = os.path.normpath(os.path.abspath(mountpoint))
    matches = [
        entry
        for entry in _read_mountinfo()
        if entry["target"] == expected_target
    ]
    return matches[-1] if matches else None


def target_path_is_mounted(mountpoint):
    """Return whether an exact target is mounted without touching its inode."""
    return _target_mount_entry(mountpoint) is not None


def _loop_device_from_mountinfo(entry):
    """Recover a loop device name from a direct or bind mountinfo entry."""
    source = entry["source"]
    if re.fullmatch(r"/dev/loop\d+", source):
        return source
    root = entry["root"]
    if re.fullmatch(r"/loop\d+", root):
        return f"/dev{root}"
    return None


def _gluster_volume_name(source):
    """Return the exact Gluster volume component from a mount source."""
    if source.startswith("kadalu:"):
        fields = source.split(":", maxsplit=2)
        return fields[1] or None if len(fields) >= 2 else None
    _, separator, remote_path = source.rpartition(":")
    if not separator:
        return None
    return remote_path.lstrip("/") or None


def _read_gluster_mounts():
    """Return Gluster mount source and normalized target pairs."""
    mounts = []
    with open(MOUNTS_FILE, encoding="utf-8") as mounts_file:
        for line in mounts_file:
            fields = line.split()
            if len(fields) < 3 or fields[2] != "fuse.glusterfs":
                continue
            mounts.append((
                _unescape_mount_field(fields[0]),
                os.path.normpath(_unescape_mount_field(fields[1])),
            ))
    return mounts


def _gluster_mount_identity(source):
    """Return a Kadalu mount generation embedded in its display name."""
    if not source.startswith("kadalu:"):
        return None
    fields = source.split(":", maxsplit=2)
    return fields[2] if len(fields) == 3 and fields[2] else None


def _has_exact_gluster_mount(volname, mountpoint, mount_identity=None):
    """Return whether procfs records this volume at this exact target."""
    expected_target = os.path.normpath(os.path.abspath(mountpoint))
    return any(
        target == expected_target
        and _gluster_volume_name(source) == volname
        and (
            mount_identity is None
            or _gluster_mount_identity(source) == mount_identity
        )
        for source, target in _read_gluster_mounts()
    )


def _remove_stale_gluster_mount(volname, mountpoint, mount_identity=None):
    """Detach a Gluster mount at this dedicated target without its client."""
    normalized_target = os.path.normpath(os.path.abspath(mountpoint))
    sources = [
        source
        for source, target in _read_gluster_mounts()
        if target == normalized_target
    ]
    if not sources:
        return

    conflicting_sources = [
        source
        for source in sources
        if _gluster_volume_name(source) != volname
    ]
    if conflicting_sources:
        raise MountTargetConflictError(
            "Gluster target is occupied by a different volume: "
            + ", ".join(conflicting_sources)
        )

    if mount_identity is not None:
        current_sources = [
            source for source in sources
            if _gluster_mount_identity(source) == mount_identity
        ]
        if current_sources:
            # The matching mount is present but its client process is gone.
            # It is safe to detach this driver's dedicated host-volume target.
            sources = current_sources

    logging.warning(logf(
        "Removing stale Gluster mount before reconnecting",
        expected_volume=volname,
        mount=normalized_target,
        sources=sources,
    ))
    execute(UNMOUNT_CMD, "-l", normalized_target)


def _remove_stale_gluster_mount_current(
        volname, mountpoint, mount_identity=None):
    """Detach stale legacy or identity-bearing mounts compatibly."""
    if mount_identity is None:
        return _remove_stale_gluster_mount(volname, mountpoint)
    return _remove_stale_gluster_mount(volname, mountpoint, mount_identity)


def _wait_for_gluster_mount(
        volname, mountpoint, mount_identity=None, attempts=20, interval=0.1):
    """Wait briefly for the client process and kernel mount to both appear."""
    for attempt in range(attempts):
        if _gluster_mount_is_current(volname, mountpoint, mount_identity):
            return True
        if attempt + 1 < attempts:
            time.sleep(interval)
    return False


def is_gluster_mount_established(volname, mountpoint, mount_identity=None):
    """Require both the exact Gluster process and kernel mount record.

    Deliberately avoid probing through the FUSE filesystem here. A stat can
    block or return ENOTCONN while an established client reconnects, which
    must not launch a duplicate client during a temporary server outage.
    """
    normalized_target = os.path.normpath(os.path.abspath(mountpoint))
    process_running = (
        is_gluster_mount_proc_running(volname, normalized_target)
        if mount_identity is None
        else is_gluster_mount_proc_running(
            volname, normalized_target, mount_identity
        )
    )
    return (
        process_running
        and _has_exact_gluster_mount(
            volname,
            normalized_target,
            mount_identity,
        )
    )


def _gluster_mount_is_current(volname, mountpoint, mount_identity=None):
    """Call mount verification compatibly for legacy identity-less pools."""
    if mount_identity is None:
        return is_gluster_mount_established(volname, mountpoint)
    return is_gluster_mount_established(volname, mountpoint, mount_identity)


def unmount_glusterfs(mountpoint,volname):
    """Unmount GlusterFS mount"""
    if _has_exact_gluster_mount(volname, mountpoint):
        logging.debug(
            logf("Executing unmount",
                 volname=volname,
                 mountpoint=mountpoint))
        execute(UNMOUNT_CMD, "-l", mountpoint)


def _loop_device_is_configured(loop_device):
    """Return whether sysfs still records backing storage for a loop device."""
    loop_name = os.path.basename(loop_device)
    if re.fullmatch(r"loop\d+", loop_name) is None:
        return False
    return os.path.exists(
        os.path.join("/sys/class/block", loop_name, "loop", "backing_file")
    )


def _detach_loop_device(loop_device):
    """Detach a loop while preserving a mounted target when retry is needed."""
    try:
        # On supported kernels this marks a busy loop autoclear. Doing it before
        # umount means a real detach failure leaves the target discoverable so
        # a later NodeUnpublish can retry with the same loop identity.
        execute("losetup", "-d", loop_device)
    except CommandException:
        # A loop created by mount(8) may already have been auto-cleared. That is
        # the desired end state, so do not turn an idempotent unpublish into an
        # error merely because losetup raced the kernel cleanup.
        if _loop_device_is_configured(loop_device):
            raise


def unmount_volume(mountpoint):
    """Unmount a Volume, detach any loop, and remove the CSI target."""
    loop_device = None
    mount_entry = _target_mount_entry(mountpoint)
    if mount_entry is not None:
        loop_device = _loop_device_from_mountinfo(mount_entry)
        if loop_device is not None:
            _detach_loop_device(loop_device)
        execute(UNMOUNT_CMD, "-l", mountpoint)

    try:
        if os.path.isdir(mountpoint) and not os.path.islink(mountpoint):
            os.rmdir(mountpoint)
        else:
            os.unlink(mountpoint)
    except FileNotFoundError:
        pass
    _durable_unlink(_publish_state_path(mountpoint))


def expand_mounted_volume(mountpoint):
    """Expand a Volume"""
    if os.path.ismount(mountpoint):
        execute("xfs_growfs", "-d", mountpoint)


def mount_glusterfs(volume, mountpoint, is_client=False):
    """Mount Glusterfs Volume"""

    data = {}
    hosts = []
    volname = volume["name"]

    if volume['type'] == 'External':
        return handle_external_volume(volume, mountpoint, is_client, volume['g_host'])

    with open(os.path.join(VOLINFO_DIR, "%s.info" % volname)) as info_file:
        data = json.load(info_file)
    mount_identity = _mount_identity(data)

    # An existing client remains useful while its servers reconnect. Only
    # require a reachable volfile server when a new Gluster client is needed.
    if _gluster_mount_is_current(volname, mountpoint, mount_identity):
        logging.debug(logf(
            "Already mounted",
            mount=mountpoint
        ))
        return mountpoint

    for brick in data["bricks"]:
        hosts.append(brick["node"])

    if not is_server_pod_reachable(hosts, 24007, 20):
        logging.error(logf(
            "None of the server pods are reachable",
            volume=volume
        ))
        err = "Cannot establish a socket connection with any server pod"
        cmd = "sock.connect(hosts, 24007)"
        raise CommandException(-1, cmd, err)

    with mount_lock:
        # Another request may have mounted this volume while this request was
        # waiting for the lock.
        if _gluster_mount_is_current(volname, mountpoint, mount_identity):
            logging.debug(logf(
                "Already mounted after waiting for mount lock",
                mount=mountpoint
            ))
            return mountpoint

        _remove_stale_gluster_mount_current(
            volname,
            mountpoint,
            mount_identity,
        )

        # Do not probe through a stale FUSE mount before detaching it. Such a
        # path lookup can fail with ENOTCONN or block during reconnection.
        makedirs(mountpoint)

        # Fix the log, so we can check it out later
        # log_file = "/var/log/gluster/%s.log" % mountpoint.replace("/", "-")
        log_file = "/var/log/gluster/gluster.log"

        cmd = [
            GLUSTERFS_CMD,
            "--process-name", "fuse",
            "-l", log_file,
            "--volfile-id", volname,
            "--fs-display-name", (
                f"kadalu:{volname}:{mount_identity}"
                if mount_identity is not None
                else f"kadalu:{volname}"
            ),
            mountpoint
        ]

        ## required for 'simple-quota'
        if not is_client:
            cmd.extend(["--client-pid", "-14"])

        # Use volfile server of bricks/storage_unit processes,
        # instead of volfile paths. Since now brick processes
        # supports serving of client volfiles.
        for host in hosts:
            cmd.extend(["--volfile-server", host])

        try:
            if mount_identity is None:
                _execute_glusterfs_mount(cmd, volname, mountpoint)
            else:
                _execute_glusterfs_mount(
                    cmd,
                    volname,
                    mountpoint,
                    mount_identity,
                )
        except CommandException as err:
            logging.error(logf(
                "error to execute command",
                volume=volume,
                cmd=cmd,
                error=format(err)
            ))
            raise

    return mountpoint


def handle_external_volume(volume, mountpoint, is_client, hosts):
    """
    Handle mounting of volume with external host and setting of quota
    """

    volname = volume['g_volname']
    mount_identity = _mount_identity(volume)

    # Try to mount the Host Volume, handle failure if
    # already mounted
    if _gluster_mount_is_current(volname, mountpoint, mount_identity):
        logging.debug(logf(
            "Already mounted",
            mount=mountpoint
        ))
        return mountpoint

    with mount_lock:
        if _gluster_mount_is_current(volname, mountpoint, mount_identity):
            logging.debug(logf(
                "Already mounted after waiting for mount lock",
                mount=mountpoint
            ))
            return mountpoint
        _remove_stale_gluster_mount_current(
            volname,
            mountpoint,
            mount_identity,
        )
        if mount_identity is None:
            mount_glusterfs_with_host(
                volname,
                mountpoint,
                hosts,
                volume['g_options'],
                is_client,
            )
        else:
            mount_glusterfs_with_host(
                volname,
                mountpoint,
                hosts,
                volume['g_options'],
                is_client,
                mount_identity,
            )

    use_gluster_quota = False
    if (os.path.isfile("/etc/secret-volume/ssh-privatekey")
        and "SECRET_GLUSTERQUOTA_SSH_USERNAME" in os.environ):
        use_gluster_quota = True
    secret_private_key = "/etc/secret-volume/ssh-privatekey"
    secret_username = os.environ.get('SECRET_GLUSTERQUOTA_SSH_USERNAME', None)

    if use_gluster_quota is False:
        logging.debug(logf("Do not set quota-deem-statfs"))
    else:
        # SSH into only first reachable host in volume['g_host'] entry
        g_host = reachable_host(hosts)

        if g_host is None:
            logging.error(logf("All hosts are not reachable"))
            return

        logging.debug(logf("Set quota-deem-statfs for gluster directory Quota"))
        remote_command = shlex.join([
            "sudo",
            "gluster",
            "volume",
            "set",
            str(volume['g_volname']),
            "quota-deem-statfs",
            "on",
        ])
        quota_deem_cmd = [
            "ssh",
            "-oStrictHostKeyChecking=no",
            "-i",
            "%s" % secret_private_key,
            "%s@%s" % (secret_username, g_host),
            remote_command,
        ]
        try:
            execute(*quota_deem_cmd)
        except CommandException as err:
            errmsg = "Unable to set quota-deem-statfs via ssh"
            logging.error(logf(errmsg, error=err))
            raise err
    return mountpoint


def _mount_succeeded_after_command_error(
        error, volname, mountpoint, mount_identity=None):
    """Return true only when exit 32 left the exact mount client running."""
    if error.ret != 32:
        return False

    if not _gluster_mount_is_current(volname, mountpoint, mount_identity):
        return False

    logging.info(logf(
        "Gluster mount process started despite command exit status",
        volume=volname,
        mount=mountpoint,
        status=error.ret,
    ))
    return True


def _execute_glusterfs_mount(
        command, volname, mountpoint, mount_identity=None):
    """Execute a Gluster mount and require a verified kernel mount."""
    try:
        execute(*command)
    except CommandException as error:
        if _mount_succeeded_after_command_error(
                error, volname, mountpoint, mount_identity):
            return
        raise

    if not _wait_for_gluster_mount(volname, mountpoint, mount_identity):
        raise CommandException(
            -1,
            " ".join(command),
            "Gluster client exited successfully without establishing its mount",
        )


def _execute_glusterfs_mount_current(
        command, volname, mountpoint, mount_identity=None):
    """Launch legacy or identity-bearing mounts compatibly."""
    if mount_identity is None:
        return _execute_glusterfs_mount(command, volname, mountpoint)
    return _execute_glusterfs_mount(
        command,
        volname,
        mountpoint,
        mount_identity,
    )


# noqa # pylint: disable=unused-argument
def mount_glusterfs_with_host(
        volname, mountpoint, hosts, options=None, is_client=False,
        mount_identity=None):
    """Mount Glusterfs Volume"""

    if not os.path.exists(mountpoint):
        makedirs(mountpoint)

    log_file = "/var/log/gluster/gluster.log"

    cmd = [
        GLUSTERFS_CMD,
        "--process-name", "fuse",
        "-l", "%s" % log_file,
        "--volfile-id", volname,
        "--fs-display-name", (
            f"kadalu:{volname}:{mount_identity}"
            if mount_identity is not None
            else f"kadalu:{volname}"
        ),
    ]
    ## on server component we can mount glusterfs with client-pid
    #if not is_client:
    #    cmd.extend(["--client-pid", "-14"])

    for host in hosts.split(','):
        cmd.extend(["--volfile-server", host])

    g_ops = []
    if options:
        for option in options.split(","):
            g_ops.append(f"--{option}")

    logging.debug(logf(
        "glusterfs command",
        cmd=cmd,
        opts=g_ops,
        mountpoint=mountpoint,
    ))

    command = cmd + g_ops + [mountpoint]
    try:
        _execute_glusterfs_mount_current(
            command,
            volname,
            mountpoint,
            mount_identity,
        )
    except CommandException as excep:
        if ("invalid option" in excep.err
                or "unrecognized option" in excep.err):
            logging.warning(logf(
                "proceeding without supplied incorrect mount options",
                options=g_ops,
                ))
            command = cmd + [mountpoint]
            try:
                _execute_glusterfs_mount_current(
                    command,
                    volname,
                    mountpoint,
                    mount_identity,
                )
            except CommandException as retry_err:
                logging.error(logf(
                    "mount command failed",
                    cmd=command,
                    error=retry_err,
                ))
                raise
            return mountpoint
        logging.error(logf(
            "mount command failed",
            cmd=command,
            error=excep,
        ))
        raise

    return mountpoint


def check_external_volume(pv_request, host_volumes):
    """Mount hosting volume"""
    # Assumption is, this has to have 'hostvol_type' as External.
    params = {}
    for pkey, pvalue in pv_request.parameters.items():
        params[pkey] = pvalue

    mntdir = None
    hvol = None
    for vol in host_volumes:
        if vol["type"] != "External":
            continue

        # For external volume both single_pv_per_pool,
        # g_volname and hosts should match
        # gluster_hosts is flattened to a string and can be compared as such
        # Assumptions:
        # 1. User will not reuse a gluster non-native volume
        if (get_single_pv_per_pool(vol) == get_single_pv_per_pool(params)
                and vol["g_volname"] == params["gluster_volname"]
                and vol["g_host"] == params["gluster_hosts"]):
            mntdir = os.path.join(HOSTVOL_MOUNTDIR, vol["name"])
            hvol = vol
            break

    if not mntdir:
        logging.warning("No host volume found to provide PV")
        return None

    mountpoint = mount_glusterfs(hvol, mntdir)

    if not mountpoint:
        logging.debug(logf(
            "Mount failed",
            hvol=hvol,
            mntdir=mntdir
        ))
        return None

    logging.debug(logf(
        "Mount successful",
        hvol=hvol
    ))

    return hvol


# Methods starting with 'yield_*' upon not a single entry raise StopIteration
# (via return "reason") and upon no entry for a specific scenario yields
# None. Caller should handle None gracefully based on the context the info is
# required, like:
# 1. Is it critical enough to serve the storage to user? Fail fast
# 2. Performing health checks or which can be eventually consistent (listvols)?
# Handle gracefully
def yield_hostvol_mount():
    """Yield each mounted hosting-volume root and its configuration."""
    host_volumes = get_pv_hosting_volumes()
    mount_exists = False
    for volume in host_volumes:
        hvol = volume['name']
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, hvol)
        try:
            mount_glusterfs(volume, mntdir)
        except CommandException as excep:
            logging.error(
                logf("Unable to mount volume", hvol=hvol, excep=excep.args))
            # We aren't able to mount this specific hostvol
            yield None
            continue
        logging.info(logf("Volume is mounted successfully", hvol=hvol))
        mount_exists = True
        yield mntdir, volume
    if not mount_exists:
        # A generator should yield "something", to signal "StopIteration" if
        # there's no info file on any pool, there should be empty yield
        # Note: raise StopIteration =~ return, but return with a reason is
        # better.
        return "No storage pool exists"


def yield_pvc_from_mntdir(
        mntdir, include_archived=True, _info_root=None):
    """Yield PVC metadata, optionally excluding archived identities."""
    # Max recursion depth is two subdirs (/<mntdir>/x/y/<pvc-name.json>)
    # If 'subvol/virtblock/rawblock' exist then max depth will be three subdirs
    if mntdir.endswith("info") and not os.path.isdir(mntdir):
        # There might be a chance that this function is used standalone and so
        # check for 'info' directory exists
        yield None
        return
    if _info_root is None:
        _info_root = os.path.abspath(mntdir)
    for child in os.listdir(mntdir):
        name = os.path.join(mntdir, child)

        if (
                os.path.isdir(name)
                and not os.path.islink(name)
                and len(os.listdir(name))):
            yield from yield_pvc_from_mntdir(
                name,
                include_archived,
                _info_root,
            )
        elif name.endswith('json'):
            # Base case we are interested in, the filename ending with '.json'
            # is 'PVC' name and contains it's size
            file_path = name
            with open(file_path) as handle:
                data = json.loads(handle.read().strip())
            logging.debug(
                logf("Found a PVC at", path=file_path, size=data.get("size")))
            data["name"] = data.get(
                "volume_id",
                name[name.rfind("/") + 1:name.rfind(".json")],
            )
            data["metadata_path"] = file_path
            archive = archive_metadata_details(
                os.path.dirname(_info_root),
                file_path,
                data,
            )
            if not include_archived and (
                    archive is not None
                    or data.get("state") in (
                        "archived",
                        "archiving",
                        "deleting",
                        "reclaiming",
                    )):
                continue
            yield data
        else:
            # If leaf is neither a json file nor a directory with contents
            yield None


def yield_pvc_from_hostvol():
    """Yields a single PVC sequentially from all the hostvolumes"""
    pvc_exist = False
    for mounted in yield_hostvol_mount():
        if mounted is not None:
            mntdir, volume = mounted
            if get_single_pv_per_pool(volume):
                claim = read_single_pv_claim(mntdir)
                if claim is None:
                    legacy_volume_id = volume.get(
                        "legacy_single_pv_volume_id"
                    )
                    if legacy_volume_id:
                        stats = retry_errors(os.statvfs, [mntdir], [ENOTCONN])
                        claim = {
                            "volume_id": legacy_volume_id,
                            "size": stats.f_blocks * stats.f_frsize,
                            "state": "active",
                        }
                if claim is not None and claim["state"] == "active":
                    pvc_exist = True
                    claim["name"] = claim["volume_id"]
                    claim["mntdir"] = mntdir
                    yield claim
                continue

            info_path = os.path.join(mntdir, "info")
            if not os.path.isdir(info_path):
                continue
            # Only yield PVC if we are able to mount corresponding pool
            pvc = yield_pvc_from_mntdir(info_path, include_archived=False)
            for data in pvc:
                if data is not None:
                    pvc_exist = True
                    data["mntdir"] = mntdir
                    yield data
    if not pvc_exist:
        return "No PVC exist in any storage pool"


def wrap_pvc(pvc_gen):
    """Yields a tuple consisting value from gen and bool for last element"""
    # No need to get num of PVCs existing in Kadalu Storage, query them in real
    # time and yield PVC, True if current entry is the last of the PVC list
    # else yield PVC, False
    gen = pvc_gen()
    try:
        prev = next(gen)
        for value in gen:
            yield prev, False
            prev = value
        yield prev, True
    except StopIteration as errmsg:
        return errmsg


def yield_list_of_pvcs(max_entries=0):
    """Yields list of PVCs limited at 'max_entries'"""
    # List of dicts containing data of PVC from info_file (with extra keys,
    # 'name', 'mntdir')
    pvcs = []
    idx = -1
    for idx, value in enumerate(wrap_pvc(yield_pvc_from_hostvol)):
        pvc, last = value
        token = "" if last else str(idx)
        pvcs.append(pvc)
        # Main logic is to 'yield' values when one of the below is observed:
        # 1. If max_entries is set and we collected max_entries of PVCs
        # 2. If max_entries is set and we are at last PVC (unaligned total PVCs
        # against max_entries)
        # 3. No max_entries is set (~all) and we are at last PVC yield all
        # pylint: disable=too-many-boolean-expressions
        if (max_entries and len(pvcs) == max_entries) or (
                max_entries and last) or (not max_entries and last):
            # As per spec 'token' has to be string, we are simply using current
            # PVC count as 'token' and validating the same
            next_token = yield
            logging.debug(logf("Received token", next_token=next_token))
            if next_token and not last and (int(next_token) !=
                                            int(token) - max_entries):
                return "Incorrect token supplied"
            logging.debug(
                logf("Yielding PVC set and next token is ",
                     token=token,
                     pvcs=pvcs))
            yield pvcs, token
            pvcs *= 0
    if idx == -1:
        return "No PVC exist in any storage pool"
