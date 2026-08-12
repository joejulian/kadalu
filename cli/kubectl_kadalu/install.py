"""
'install' subcommand for kubectl-kadalu CLI tool
"""
#To prevent Py2 to interpreting print(val) as a tuple.
from __future__ import print_function

import os
import re
import sys
import tempfile
from urllib.request import urlopen

from version import VERSION
import utils


def set_args(name, subparsers):
    """ add arguments to argparser """
    parser = subparsers.add_parser(name)
    arg = parser.add_argument

    arg(
        "--version",
        help="Kadalu Version to Install [default: " + VERSION + "]",
        choices=[VERSION, "devel"],
        default=VERSION
    )
    arg(
        "--type",
        help="Type of installation - k8s/openshift [default: kubernetes]",
        choices=["openshift", "kubernetes", "microk8s", "rke"],
        default="kubernetes"
    )
    arg(
        "--local-yaml",
        help="local operator yaml file path"
    )
    utils.add_global_flags(parser)


def validate(_args):
    """No validation available"""
    return


def _replace_yaml_scalar(line, value, quoted=False):
    """Replace the scalar value on one simple YAML line."""
    prefix = line.split(":", 1)[0] + ":"
    newline = "\n" if line.endswith("\n") else ""
    rendered = ' "%s"' % value if quoted else " %s" % value
    return prefix + rendered + newline


def render_operator_manifest(manifest, namespace):
    """Retarget a released Kadalu operator manifest to a namespace."""
    if namespace == utils.DEFAULT_NAMESPACE:
        return manifest

    documents = re.split(r"(?m)^---[ \t]*\n?", manifest)
    rendered_documents = []

    for document in documents:
        lines = document.splitlines(keepends=True)
        is_namespace = any(
            re.fullmatch(r"kind:\s*Namespace\s*", line.strip())
            for line in lines
        )
        namespace_name_pending = is_namespace
        kadalu_namespace_value_pending = False

        for index, line in enumerate(lines):
            stripped = line.strip()
            service_account_prefix = "system:serviceaccount:%s:" % \
                utils.DEFAULT_NAMESPACE
            if service_account_prefix in line:
                lines[index] = line.replace(
                    service_account_prefix,
                    "system:serviceaccount:%s:" % namespace,
                )
                continue

            if namespace_name_pending and re.fullmatch(
                    r"name:\s*['\"]?kadalu['\"]?", stripped):
                lines[index] = _replace_yaml_scalar(line, namespace)
                namespace_name_pending = False
                continue

            if re.fullmatch(
                    r"namespace:\s*['\"]?kadalu['\"]?", stripped):
                lines[index] = _replace_yaml_scalar(line, namespace)
                continue

            if re.fullmatch(
                    r"-\s*name:\s*KADALU_NAMESPACE", stripped):
                kadalu_namespace_value_pending = True
                continue

            if kadalu_namespace_value_pending and re.fullmatch(
                    r"value:\s*['\"]?kadalu['\"]?", stripped):
                lines[index] = _replace_yaml_scalar(
                    line, namespace, quoted=True
                )
                kadalu_namespace_value_pending = False

        rendered_documents.append("".join(lines))

    return "---\n".join(rendered_documents)


def read_operator_manifest(operator_file):
    """Read an operator manifest from a local path or HTTPS URL."""
    if operator_file.startswith(("https://", "http://")):
        with urlopen(operator_file) as manifest_response:  # nosec B310
            return manifest_response.read().decode("utf-8")

    with open(operator_file, encoding="utf-8") as manifest_file:
        return manifest_file.read()


def namespace_exists(args):
    """Return whether the requested namespace already exists."""
    cmd = utils.kubectl_cmd(args) + [
        "get", "namespace", args.namespace, "--ignore-not-found", "-oname"
    ]
    try:
        resp = utils.execute(cmd)
        return bool(resp.stdout.strip())
    except utils.CommandError as err:
        utils.command_error(cmd, err.stderr)
        return False


def operator_exists(args):
    """Return whether Kadalu's operator exists in the requested namespace."""
    cmd = utils.namespaced_kubectl_cmd(args) + [
        "get", "deployment", "operator", "--ignore-not-found", "-oname"
    ]
    try:
        resp = utils.execute(cmd)
        return bool(resp.stdout.strip())
    except utils.CommandError as err:
        utils.command_error(cmd, err.stderr)
        return False


# pylint: disable=too-many-branches
def run(args):
    """ perform install subcommand """

    # Check only the selected namespace. A missing namespace is a normal first
    # install condition, not a kubectl failure.
    try:
        if namespace_exists(args) and operator_exists(args):
            print("Kadalu operator already installed")
            return
    except FileNotFoundError:
        utils.kubectl_cmd_help(args.kubectl_cmd)

    operator_file = args.local_yaml
    if not operator_file:
        file_url = ""
        insttype = ""

        if args.version and args.version == "devel":
            file_url = "https://raw.githubusercontent.com/kadalu/kadalu/devel/manifests"
        elif args.version:
            file_url = "https://github.com/kadalu/kadalu/releases/download/%s" % args.version

        if args.type and args.type != "kubernetes":
            insttype = "-%s" % args.type

        operator_file = "%s/kadalu-operator%s.yaml" % (file_url, insttype)

    rendered_operator_file = None
    try:
        apply_file = operator_file
        if args.namespace != utils.DEFAULT_NAMESPACE and not args.dry_run:
            manifest = read_operator_manifest(operator_file)
            manifest = render_operator_manifest(manifest, args.namespace)
            manifest_fd, rendered_operator_file = tempfile.mkstemp(
                prefix="kadalu-operator-", suffix=".yaml"
            )
            with os.fdopen(manifest_fd, "w", encoding="utf-8") as manifest_file:
                manifest_file.write(manifest)
            apply_file = rendered_operator_file

        cmd = utils.namespaced_kubectl_cmd(args) + ["apply", "-f", apply_file]
        print("Executing '%s'" % " ".join(cmd))
        if args.dry_run:
            return

        resp = utils.execute(cmd)
        print("Kadalu operator create request sent successfully")
        print(resp.stdout)
        print()
    except utils.CommandError as err:
        utils.command_error(cmd, err.stderr)
    except FileNotFoundError:
        utils.kubectl_cmd_help(args.kubectl_cmd)
    except OSError as err:
        print("Failed to read the Kadalu operator manifest: %s" % err,
              file=sys.stderr)
        sys.exit(1)
    finally:
        if rendered_operator_file is not None and \
                os.path.exists(rendered_operator_file):
            os.remove(rendered_operator_file)
