#!/usr/bin/env python
"""Thin wrapper — delegates to run.py with --no-events."""

import subprocess
import sys

from pathlib import Path


run_py = Path(__file__).resolve().parent / 'run.py'
sys.exit(subprocess.call([sys.executable, str(run_py), '--no-events'] + sys.argv[1:]))
