"""Utility functions"""

import base64
import errno
import logging
import os
import selectors
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager

import xxhash

CREATE_TABLE_1 = """CREATE TABLE IF NOT EXISTS summary (
    volname    VARCHAR PRIMARY KEY,
    size       INTEGER,
    created_at REAL DEFAULT (datetime('now', 'localtime')),
    updated_at REAL
)"""

CREATE_TABLE_2 = """CREATE TABLE IF NOT EXISTS pv_stats (
    pvname     VARCHAR PRIMARY KEY,
    hash       VARCHAR,
    size       INTEGER,
    created_at REAL DEFAULT (datetime('now', 'localtime')),
    updated_at REAL
)"""

DB_NAME = "stat.db"
PV_TYPE_VIRTBLOCK = "virtblock"
PV_TYPE_SUBVOL = "subvol"
PV_TYPE_RAWBLOCK = "rawblock"

KADALU_VERSION = os.environ.get("KADALU_VERSION", "latest")
COMMAND_TIMEOUT_SECONDS = 120
COMMAND_TERMINATION_GRACE_SECONDS = 5


class TimeoutOSError(OSError):
    """Timeout after retries"""
    pass  # noqa # pylint: disable=unnecessary-pass


def retry_errors(func, args, errors, timeout=130, interval=2):
    """Retries given function in case of specified errors"""
    starttime = int(time.time())

    while True:
        try:
            return func(*args)
        except (OSError, IOError) as err:
            currtime = int(time.time())
            if (currtime - starttime) >= timeout:
                raise TimeoutOSError(err.errno, err.strerror) from None

            if err.errno in errors:
                time.sleep(interval)
                continue

            # Reraise the same error
            raise


# pylint: disable=too-many-return-statements
def _is_gluster_process(args, volname, mountpoint, mount_identity=None):
    """Return whether an argument vector is the requested Gluster client."""
    if not args or os.path.basename(args[0]) != "glusterfs":
        return False

    try:
        volfile_index = args.index("--volfile-id")
    except ValueError:
        return False

    if (
            volfile_index + 1 >= len(args)
            or args[volfile_index + 1] != volname):
        return False

    if mount_identity is not None:
        try:
            display_index = args.index("--fs-display-name")
        except ValueError:
            return False
        expected_display_name = f"kadalu:{volname}:{mount_identity}"
        if (
                display_index + 1 >= len(args)
                or args[display_index + 1] != expected_display_name):
            return False
    if args[-1] == mountpoint:
        return True

    try:
        display_index = args.index("--fs-display-name")
    except ValueError:
        display_index = len(args)

    return display_index + 2 < len(args) and args[display_index + 2] == mountpoint


def is_gluster_mount_proc_running(volname, mountpoint, mount_identity=None):
    """Check whether the requested Gluster client process is running."""
    for proc_entry in os.scandir("/proc"):
        if not proc_entry.name.isdigit():
            continue

        try:
            cmdline_path = os.path.join(proc_entry.path, "cmdline")
            with open(cmdline_path, "rb") as cmdline:
                args = [
                    arg.decode(errors="surrogateescape")
                    for arg in cmdline.read().split(b"\0")
                    if arg
                ]
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue

        if _is_gluster_process(args, volname, mountpoint, mount_identity):
            return True

    return False


def _server_endpoints(hosts, port):
    """Yield each unique address resolved for the requested server pods."""
    endpoints = set()
    for host in hosts:
        try:
            addresses = socket.getaddrinfo(
                host,
                int(port),
                family=socket.AF_UNSPEC,
                type=socket.SOCK_STREAM,
            )
        except socket.gaierror as err:
            logging.info(logf(
                "Failed to resolve server pod",
                server_pod=host,
                error=err,
            ))
            continue

        for family, socktype, protocol, _, address in addresses:
            endpoint = (family, socktype, protocol, address)
            if endpoint not in endpoints:
                endpoints.add(endpoint)
                yield host, endpoint


def _start_server_connections(selector, sockets, hosts, port):
    """Start each server connection and return true if one is immediate."""
    pending_errors = {
        errno.EINPROGRESS,
        errno.EWOULDBLOCK,
        errno.EALREADY,
        errno.EINTR,
    }
    for host, (family, socktype, protocol, address) in _server_endpoints(
            hosts, port):
        sock = socket.socket(family, socktype, protocol)
        sockets.append(sock)
        sock.setblocking(False)
        result = sock.connect_ex(address)
        if result in (0, errno.EISCONN):
            return True
        if result in pending_errors:
            selector.register(sock, selectors.EVENT_WRITE, host)
            continue

        logging.info(logf(
            "Failed to connect to server pod",
            server_pod=host,
            error=os.strerror(result),
        ))

    return False


def _wait_for_server_connection(selector, deadline):
    """Wait until a pending server connection succeeds or time expires."""
    while selector.get_map():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False

        events = selector.select(remaining)
        if not events:
            return False

        for key, _ in events:
            sock = key.fileobj
            selector.unregister(sock)
            error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if error == 0:
                return True

            logging.info(logf(
                "Failed to connect to server pod",
                server_pod=key.data,
                error=os.strerror(error),
            ))

    return False


def is_server_pod_reachable(hosts, port=24007, timeout=20):
    """
    Return whether any server pod is reachable before the total timeout.

    All resolved IPv4 and IPv6 endpoints are attempted concurrently so an
    unavailable server listed first cannot delay a healthy endpoint. Writable
    non-blocking sockets are checked with SO_ERROR because select also marks a
    refused connection writable.
    """
    if not hosts or timeout <= 0:
        return False

    selector = selectors.DefaultSelector()
    sockets = []
    deadline = time.monotonic() + timeout
    try:
        if _start_server_connections(selector, sockets, hosts, port):
            return True
        return _wait_for_server_connection(selector, deadline)
    finally:
        selector.close()
        for sock in sockets:
            sock.close()


def is_host_reachable(hosts, port):
    """Check if glusterd is reachable in the given node"""

    timeout = 5
    for host in hosts:
        try:
            with socket.create_connection(
                    (host, int(port)), timeout=timeout) as sock:
                sock.shutdown(socket.SHUT_RDWR)
            return True
        except socket.error as msg:
            logging.error(logf("Failed to open socket connection",
                               error=msg, host=host))
            continue
    return False


def reachable_host(hosts):
    """Return first reachable host for dir-quota SSH"""
    hosts = hosts.strip().split(',')
    for host in hosts:
        host = host.strip()
        if is_host_reachable([host], 22):
            return host
    return None


def makedirs(dirpath):
    """exist_ok=True parameter will raise exception even if directory
    exists with different attributes. Handle EEXIST gracefully."""
    try:
        os.makedirs(dirpath)
    except FileExistsError:
        pass


class CommandException(Exception):
    """Custom exception for command execution"""
    def __init__(self, ret, out, err):
        self.ret = ret
        self.out = out
        self.err = err
        msg = "[%d] %s %s" % (ret, out, err)
        super().__init__(msg)


def get_volname_hash(volname):
    """XXHash based on Volume name"""
    return xxhash.xxh64_hexdigest(volname)


def is_safe_path_component(value):
    """Return whether a CSI identifier is one safe filesystem component."""
    if not isinstance(value, str) or not value or value in (".", ".."):
        return False
    try:
        if len(value.encode("utf-8")) > 128:
            return False
    except UnicodeEncodeError:
        return False
    return not (
        "/" in value
        or "\0" in value
        or any(ord(character) < 32 or ord(character) == 127
               for character in value)
    )


def is_valid_csi_identifier(value):
    """Return whether a value satisfies the CSI opaque-name contract."""
    if not isinstance(value, str) or not value:
        return False
    try:
        if len(value.encode("utf-8")) > 128:
            return False
    except UnicodeEncodeError:
        return False

    return not any(
        ord(character) <= 0x08
        or ord(character) in (0x0B, 0x0C)
        or 0x0E <= ord(character) <= 0x1F
        or 0x7F <= ord(character) <= 0x9F
        for character in value
    )


VOLUME_COMPONENT_ENCODING_PREFIX = ".kadalu~"
VOLUME_LAYOUT_V2_DIR = ".kadalu-v2"


def volume_name_component(volname):
    """Encode an opaque CSI name as one collision-free path component."""
    if not is_valid_csi_identifier(volname):
        raise ValueError("volume name does not satisfy the CSI identifier contract")
    if (
            is_safe_path_component(volname)
            and not volname.startswith(VOLUME_COMPONENT_ENCODING_PREFIX)):
        return volname

    encoded = base64.urlsafe_b64encode(
        volname.encode("utf-8")
    ).decode("ascii").rstrip("=")
    return f"{VOLUME_COMPONENT_ENCODING_PREFIX}{encoded}"


def volume_name_from_component(component):
    """Decode a component produced by :func:`volume_name_component`."""
    if not isinstance(component, str):
        raise ValueError("volume component must be a string")
    if not component.startswith(VOLUME_COMPONENT_ENCODING_PREFIX):
        return component

    encoded = component[len(VOLUME_COMPONENT_ENCODING_PREFIX):]
    if not encoded:
        raise ValueError("encoded volume component is empty")
    padding = "=" * (-len(encoded) % 4)
    try:
        decoded = base64.b64decode(
            encoded + padding,
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
    except (UnicodeDecodeError, ValueError) as err:
        raise ValueError("encoded volume component is invalid") from err
    if (
            not is_valid_csi_identifier(decoded)
            or volume_name_component(decoded) != component):
        raise ValueError("encoded volume component is not canonical")
    return decoded


def get_volume_path(voltype, volhash, volname):
    """Return the canonical, path-safe location for an opaque CSI name."""
    component = volume_name_component(volname)
    if component.startswith(VOLUME_COMPONENT_ENCODING_PREFIX):
        # Keep encoded names in a layout which old raw-name releases could
        # never produce. Otherwise a deliberately chosen opaque ID could share
        # both a 16-bit hash bucket and component with a legacy raw volume ID.
        return "%s/%s/%s/%s/%s" % (
            voltype,
            VOLUME_LAYOUT_V2_DIR,
            volhash[0:2],
            volhash[2:4],
            component,
        )
    return "%s/%s/%s/%s" % (
        voltype,
        volhash[0:2],
        volhash[2:4],
        component,
    )


def get_legacy_volume_path(voltype, volhash, volname):
    """Return an old raw-name path only for one legacy-safe component."""
    if not is_valid_csi_identifier(volname):
        raise ValueError("volume name does not satisfy the CSI identifier contract")
    if not is_safe_path_component(volname):
        return None

    bucket = os.path.normpath("%s/%s/%s" % (
        voltype,
        volhash[0:2],
        volhash[2:4],
    ))
    candidate = os.path.normpath(os.path.join(bucket, volname))
    try:
        if os.path.commonpath((candidate, bucket)) != bucket or candidate == bucket:
            return None
    except ValueError:
        return None
    return candidate


def execute(*cmd, shell=False, timeout=COMMAND_TIMEOUT_SECONDS):
    """
    Execute command. Returns output and error.
    Raises CommandException on error
    """
    with subprocess.Popen(cmd,
                          stderr=subprocess.PIPE,
                          stdout=subprocess.PIPE,
                          shell=shell,
                          cwd=None,
                          start_new_session=True,
                          universal_newlines=True) as proc:
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                out, err = proc.communicate(
                    timeout=COMMAND_TERMINATION_GRACE_SECONDS,
                )
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                out, err = proc.communicate()
            timeout_error = f"command timed out after {timeout} seconds"
            if err.strip():
                timeout_error = f"{timeout_error}: {err.strip()}"
            raise CommandException(124, out.strip(), timeout_error) from None
        if proc.returncode != 0:
            raise CommandException(proc.returncode, out.strip(), err.strip())
        return (out.strip(), err.strip(), proc.pid)


def logf(msg, **kwargs):
    """Formats message for Logging"""
    if kwargs:
        msg += "\t"

    for msg_key, msg_value in kwargs.items():
        msg += " %s=%s" % (msg_key, msg_value)

    return msg


def logging_setup():
    """Logging Setup"""
    root = logging.getLogger()
    verbose = os.environ.get("VERBOSE", "no")
    root.setLevel(logging.INFO)
    if verbose == "yes":
        root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.INFO)
    if verbose == "yes":
        handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s "
                                  "[%(module)s - %(lineno)s:%(funcName)s] "
                                  "- %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)



def send_analytics_tracker(name, uid=None):
    """Send setup events to Google analytics"""

    # This function is not required anymore as we expect
    # users to report usage through github issues, or by
    # giving a 'star'.
    # Only thing we learnt from this is, External, Replica3
    # and Replica1 are preferred in that order (So far,
    # as of Sept 2020)

    return (name, uid)

class SizeAccounting:
    """
    Context manager to read and update Volume size and PV size info

    Usage:

    with SizeAccounting("storage-pool-1", "/mnt/storage-pool-1") as acc:
        acc.update_pv_record("pv1", 20000000)
    """

    def __init__(self, volname, mount_path):
        self.mount_path = mount_path
        self.volname = volname
        self.conn = None
        self.cursor = None
        self._transaction_active = False

    def __enter__(self):
        """Initialize the Db Connection"""
        self.conn = sqlite3.connect(os.path.join(self.mount_path, DB_NAME))
        self.cursor = self.conn.cursor()
        self._create_tables()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Close Db connection on exit of the Context manager"""
        self.conn.close()

    def _create_tables(self):
        """Create required tables"""
        self.cursor.execute(CREATE_TABLE_1)
        self.cursor.execute(CREATE_TABLE_2)

    def _commit(self):
        """Commit unless an explicit multi-operation transaction owns it."""
        if not self._transaction_active:
            self.conn.commit()

    @contextmanager
    def immediate_transaction(self):
        """Hold SQLite's write lock across related reads and mutations."""
        if self._transaction_active:
            raise RuntimeError("capacity accounting transaction is already active")
        self.cursor.execute("BEGIN IMMEDIATE")
        self._transaction_active = True
        try:
            yield self
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
        finally:
            self._transaction_active = False

    @staticmethod
    def _valid_size(size, *, allow_zero=False):
        """Return an integer size or reject corrupt/accounting input."""
        valid = (
            isinstance(size, int)
            and not isinstance(size, bool)
            and (size >= 0 if allow_zero else size > 0)
        )
        if not valid:
            raise ValueError("capacity accounting contains an invalid size")
        return size

    def update_summary(self, size):
        """Update the total available size in storage pool"""
        size = self._valid_size(size)

        # To retain the old value of created_at, select from existing
        query = """
        INSERT OR REPLACE INTO summary (
            volname, size, created_at, updated_at
        )
        VALUES (
            ?,
            ?,
            COALESCE((SELECT created_at FROM summary WHERE volname = ?),
                     datetime('now', 'localtime')),
            datetime('now', 'localtime')
        )
        """

        self.cursor.execute(query, (self.volname, size, self.volname))
        self._commit()

    def update_pv_record(self, pvname, size):
        """Update Each PV size"""
        size = self._valid_size(size)

        # To retain the old value of created_at, select from existing
        query = """
        INSERT OR REPLACE INTO pv_stats (
            pvname, size, hash, created_at, updated_at
        )
        VALUES (
            ?,
            ?,
            ?,
            COALESCE((SELECT created_at FROM pv_stats WHERE pvname = ?),
                     datetime('now', 'localtime')),
            datetime('now', 'localtime')
        )
        """
        pv_hash = get_volname_hash(pvname)
        self.cursor.execute(query, (pvname, size, pv_hash, pvname))
        self._commit()

    def remove_pv_record(self, pvname):
        """Remove PV related entry when PV is deleted"""

        self.cursor.execute("DELETE FROM pv_stats WHERE pvname = ?", (pvname, ))
        self._commit()

    def rename_pv_record(self, old_name, new_name, size):
        """Move a reservation to an archived name without losing capacity."""
        size = self._valid_size(size)
        self.cursor.execute(
            "DELETE FROM pv_stats WHERE pvname IN (?, ?)",
            (old_name, new_name),
        )
        pv_hash = get_volname_hash(new_name)
        self.cursor.execute(
            """
            INSERT INTO pv_stats (
                pvname, size, hash, created_at, updated_at
            ) VALUES (
                ?, ?, ?, datetime('now', 'localtime'),
                datetime('now', 'localtime')
            )
            """,
            (new_name, size, pv_hash),
        )
        self._commit()

    def get_pv_size(self, pvname):
        """Return the absolute size reserved for a PV, or zero if absent."""
        self.cursor.execute(
            "SELECT size FROM pv_stats WHERE pvname = ?",
            (pvname,),
        )
        record = self.cursor.fetchone()
        return self._valid_size(record[0]) if record is not None else 0

    def get_stats(self):
        """Get Statistics: total/used/free size, number of pvs"""
        self.cursor.execute("SELECT size FROM pv_stats")
        pv_sizes = [self._valid_size(record[0]) for record in self.cursor]
        number_of_pvs = len(pv_sizes)
        used_size_bytes = sum(pv_sizes)

        self.cursor.execute("SELECT volname, size FROM summary")
        summary_record = self.cursor.fetchone()
        total_size_bytes = (
            0
            if summary_record is None
            else self._valid_size(summary_record[1])
        )

        return {
            "number_of_pvs": number_of_pvs,
            "total_size_bytes": total_size_bytes,
            "used_size_bytes": used_size_bytes,
            "free_size_bytes": total_size_bytes - used_size_bytes
        }


# noqa # pylint: disable=too-few-public-methods
class Proc:
    """Handle Process details"""
    def __init__(self, name, command, args):
        self.name = name
        self.command = command
        self.args = args

    def with_args(self):
        """Return command and args together to use in Popen"""
        return [self.command] + self.args


class ProcState:
    """Handle Process states"""
    def __init__(self, proc):
        self.proc = proc
        self.enabled = True
        self.subproc = None

    def start(self):
        """Start a Process"""
        # Context Manager here would wait for subprocess to complete which we
        # don't want and had to disable pylint error
        # noqa # pylint: disable=consider-using-with
        self.subproc = subprocess.Popen(self.proc.with_args(),
                                        stderr=sys.stderr,
                                        universal_newlines=True,
                                        env=os.environ)

    def stop(self):
        """Stop a Process"""
        if self.subproc is not None:
            self.subproc.kill()
            self.subproc.communicate()
            self.subproc = None

    def restart(self):
        """Restart a Process"""
        self.stop()
        self.start()


class Monitor:
    """Start and Monitor multiple processes"""
    def __init__(self, procs=None):
        self.procs = {}
        self.terminating = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        if procs is not None:
            for proc in procs:
                self.procs[proc.name] = ProcState(proc)

    def add_process(self, proc):
        """Add a Process to the list of Monitored processes"""
        self.procs[proc.name] = ProcState(proc)

    def start_all(self):
        """Start all Managed Processes"""
        for name, state in self.procs.items():
            state.start()
            logging.info(logf("Started Process", name=name))

    def stop_all(self):
        """Stop all Managed Processes"""
        for name, state in self.procs.items():
            state.stop()
            logging.info(logf("Stopped Process", name=name))

    def restart_all(self):
        """Restart all managed Processes"""
        for name, state in self.procs.items():
            state.restart()
            logging.info(logf("Restarted Process", name=name))

    def exit_gracefully(self, _signum, _frame):
        """When SIGTERM/SIGINT received"""
        self.terminating = True

    def monitor_proc(self, state, terminating):
        """Monitor single process"""
        if not state.enabled:
            return

        if terminating:
            state.stop()
            logging.info(logf("Terminated Process", name=state.proc.name))
            return

        ret = state.subproc.poll()
        if ret is None:
            return

        if not terminating:
            state.restart()
            logging.info(logf("Restarted Process", name=state.proc.name))

    def monitor(self):
        """
        Start monitoring all the started processes.
        Restart processes on failure
        """
        try:
            while True:
                terminating = self.terminating

                for _, state in self.procs.items():
                    self.monitor_proc(state, terminating)

                if terminating:
                    logging.info("Terminating Monitor process")
                    sys.exit(0)

                time.sleep(1)
        except KeyboardInterrupt:
            self.terminating = True
            sys.exit(1)


def get_single_pv_per_pool(data):
    """
    Extract single PV per pool backward compatible way. Both
    kadalu_format and single_pv_per_pool are supported.
    """
    kformat = data.get('kadalu_format', None)
    if kformat is not None:
        if not isinstance(kformat, str):
            raise ValueError("kadalu_format must be a string")
        return kformat.lower() != "native"

    val = data.get("single_pv_per_pool", False)
    if isinstance(val, str):
        return val.lower() == "true"

    if not isinstance(val, bool):
        raise ValueError("single_pv_per_pool must be a boolean or string")
    return val
