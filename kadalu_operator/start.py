"""Run and supervise the Kadalu operator and metrics processes."""

import os

from kadalulib import Monitor, Proc, logging_setup


OPERATOR_READY_FILE = "/tmp/operator-ready"


def clear_operator_ready():
    """Remove readiness left by an earlier reconciler process."""
    try:
        os.unlink(OPERATOR_READY_FILE)
    except FileNotFoundError:
        pass


class OperatorMonitor(Monitor):
    """Clear readiness before restarting a failed operator reconciler."""

    def monitor_proc(self, state, terminating):
        if (
                not terminating
                and state.enabled
                and state.proc.name == "operator"
                and state.subproc is not None
                and state.subproc.poll() is not None):
            clear_operator_ready()
        return super().monitor_proc(state, terminating)


def main():
    """Start both processes without preserving stale operator readiness."""
    curr_dir = os.path.dirname(__file__)

    clear_operator_ready()
    mon = OperatorMonitor()
    mon.add_process(Proc("operator", "python3", [curr_dir + "/main.py"]))
    mon.add_process(Proc("metrics", "python3", [curr_dir + "/exporter.py"]))

    mon.start_all()
    try:
        mon.monitor()
    finally:
        clear_operator_ready()


if __name__ == "__main__":
    logging_setup()
    main()
