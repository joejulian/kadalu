import sys
import os
import shutil
import argparse
from errno import ENOTCONN
from volumeutils import (ARCHIVE_PREFIX, HOSTVOL_MOUNTDIR, _atomic_write_json,
                         _durable_unlink, archive_metadata_details,
                         release_archived_pv_reservation,
                         yield_pvc_from_mntdir)
from kadalulib import retry_errors


def get_archived_pvs(storage_name, pvc_name):
    """ Return all or specified archived_pvcs based on agrs """

    archived_pvs = {}

    mntdir = os.path.join(HOSTVOL_MOUNTDIR, storage_name, "info")
    if not os.path.isdir(mntdir):
        sys.stderr.write(f"Metadata for storagepool {storage_name} is not found")
        return -1

    try:
        pool_root = os.path.dirname(mntdir)
        for pvc in yield_pvc_from_mntdir(mntdir):
            if pvc is None:
                continue
            archive = archive_metadata_details(
                pool_root,
                pvc["metadata_path"],
                pvc,
            )
            if archive is None or pvc.get("state") == "archiving":
                continue

            archive_name = archive["archive_name"]
            if archive_name in archived_pvs:
                raise ValueError(
                    "Duplicate archive metadata for identity: "
                    f"{archive_name}"
                )
            record = pvc.copy()
            record["name"] = archive_name
            record["original_volume_id"] = archive["original_volume_id"]
            record["metadata_path"] = archive["metadata_path"]
            record["payload_path"] = archive["payload_path"]
            record["legacy_archive"] = archive["legacy"]
            archived_pvs[archive_name] = record

        # Return -1 if no matched specified pvc
        if pvc_name is not None and pvc_name not in archived_pvs:
            sys.stderr.write("Specified PVC %s is not found" % pvc_name)
            return -1

        if pvc_name is not None:
            return {pvc_name: archived_pvs[pvc_name]}

        # This return is for without --pvc.
        return archived_pvs

    except FileNotFoundError:
        sys.stderr.write("Storage pool %s is not found" % storage_name)
        return -1


def delete_archived_pvs(storage_name, archived_pvs):
    """ Delete all archived pvcs in archived_pvs """

    for pvname, values in archived_pvs.items():

        # Check for mount availablity before deleting info file & PVC
        mntdir = os.path.join(HOSTVOL_MOUNTDIR, storage_name)
        retry_errors(os.statvfs, [mntdir], [ENOTCONN])

        archive = archive_metadata_details(
            mntdir,
            values.get("metadata_path", ""),
            values,
        )
        if archive is None or archive["archive_name"] != pvname:
            raise ValueError("Refusing to delete metadata that is not an archive")

        pvc_path = archive["payload_path"]
        info_file_path = archive["metadata_path"]
        if values.get("state") != "reclaiming":
            reclaiming = {
                key: value
                for key, value in values.items()
                if key not in (
                    "legacy_archive",
                    "metadata_path",
                    "name",
                    "payload_path",
                )
            }
            reclaiming.update({
                "original_volume_id": archive["original_volume_id"],
                "path_prefix": (
                    reclaiming.get("path_prefix")
                    if archive["legacy"]
                    else "archive"
                ),
                "state": "reclaiming",
            })
            if archive["legacy"]:
                reclaiming["legacy_archive"] = True
                reclaiming.pop("archive_name", None)
            else:
                reclaiming["archive_name"] = pvname
                reclaiming.pop("legacy_archive", None)
            _atomic_write_json(info_file_path, reclaiming)
            values = reclaiming

        # Delete the archived payload only after a durable reclaim tombstone.
        try:
            if os.path.isdir(pvc_path) and not os.path.islink(pvc_path):
                shutil.rmtree(pvc_path)
            else:
                os.unlink(pvc_path)
        except FileNotFoundError:
            pass

        # Unique archives reserve by archive ID. Legacy archives retained the
        # reservation under the original ID, so release either form while
        # preserving a newly recreated active volume with the same old ID.
        release_archived_pv_reservation(
            storage_name,
            pvname,
            archive["original_volume_id"],
        )

        # Remove the tombstone only after its accounting reservation is gone.
        _durable_unlink(info_file_path)


def main():
    """ main """

    parser = argparse.ArgumentParser()
    parser.add_argument("name", help="name of storage-pool")
    parser.add_argument("--pvc", default=None,
                        help="name of archived pvc belonging to specified storage-pool")

    args = parser.parse_args()

    if args.pvc and not args.pvc.startswith(ARCHIVE_PREFIX):
        sys.stderr.write("Passing of non archived PVC not allowed.")
        sys.exit()

    archived_pvs = get_archived_pvs(args.name, args.pvc)
    if archived_pvs == -1:
        sys.exit()

    if archived_pvs:
        sys.stdout.write("Found archived PVCs at storage pool %s\n" % args.name)
        delete_archived_pvs(args.name, archived_pvs)
        sys.stdout.write("Completed deletion of archived pvc(s) of storage-pool %s\n" %args.name)
    else:
        sys.stderr.write("No archived PVCs found at storage-pool %s" % args.name)


if __name__ == "__main__":
    main()
