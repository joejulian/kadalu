"""
This is an CLI command to handle kadalu executions
"""
# To prevent Py2 to interpreting print(val) as a tuple.
from __future__ import print_function

import sys
from argparse import ArgumentParser

from version import VERSION
import utils

# Subcommands list that are currently supported, if any new subcommand has to
# be implemeted add argument to this list and implement `validate` and `run`
# functions in a separate file
SUPPORTED_ARGS = [
    "install",
    "storage-add",
    "storage-list",
    "storage-remove",
    "remove-archived-pv",
    "logs",
    "healinfo",
    "option-set",
    "option-reset"
]

def get_args():
    """Argument Parser"""
    parser = ArgumentParser()
    utils.add_global_flags(parser, suppress_defaults=False)
    subparsers = parser.add_subparsers(dest="mode")
    version_set_args("version", subparsers)
    for arg in SUPPORTED_ARGS:
        mod = __import__(arg.replace("-", "_"))
        mod.set_args(arg, subparsers)

    args = parser.parse_args()

    # If no subcommand is provided display help text and exit
    if args.mode is None:
        parser.print_help(sys.stderr)
        sys.exit(1)

    return args


def get_all_kadalu_pods(args):
    """ List of all pods in kadalu namespace """

    try:
        cmd = utils.namespaced_kubectl_cmd(args) + [
            "get", "pods", "-oname"
        ]
        resp = utils.execute(cmd)
        # Remove empty lines(pod-names) from command response
        pods = resp.stdout.split()
        return pods
    except utils.CommandError as err:
        utils.command_error(cmd, err.stderr)
        return None


def get_kadalu_version_in_pod(pod, args):
    """ Get kadalu version from pods inside kadalu-namespace by exec """

    try:

        cmd = utils.namespaced_kubectl_cmd(args) + ["exec", pod]

        if "nodeplugin" in pod or "provisioner" in pod:
            version_container = "kadalu-nodeplugin"
            if "provisioner" in pod:
                version_container = "kadalu-provisioner"
            cmd += ["-c", version_container]

        cmd += ["--", "bash", "-c", "echo $KADALU_VERSION"]

        version_resp = utils.execute(cmd)
        version = version_resp.stdout.strip()

        return version

    except utils.CommandError as err:
        utils.command_error(cmd, err.stderr)
        return None


def show_version(args):
    """Show version information"""
    print("kubectl-kadalu plugin: %s" % VERSION)
    pods = get_all_kadalu_pods(args)
    if pods:
        print("kadalu pod(s) versions")
        for pod in pods:
            version = get_kadalu_version_in_pod(pod, args)
            print("%s: %s" %(pod, version))


def version_set_args(name, parser):
    """Version subcommand"""
    version_parser = parser.add_parser(name)
    utils.add_global_flags(version_parser)


def main():
    """Handle Commands"""
    try:
        args = get_args()
        if args.mode == "version":
            show_version(args)
            sys.exit(0)
        try:
            mod = __import__(args.mode.replace("-", "_"))
            mod.validate(args)
            mod.run(args)
        except ModuleNotFoundError:
            print('Invalid sub-command "%s"' % args.mode, file=sys.stderr)
            sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
