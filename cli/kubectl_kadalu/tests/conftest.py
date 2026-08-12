"""Test setup for the self-contained kubectl-kadalu sources."""

import sys
import types
from pathlib import Path


CLI_PACKAGE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CLI_PACKAGE_DIR))
sys.path.insert(0, str(CLI_PACKAGE_DIR.parent))

# version.py is generated only while packaging the single-file plugin.
version = types.ModuleType("version")
version.VERSION = "heist-test"
sys.modules.setdefault("version", version)
