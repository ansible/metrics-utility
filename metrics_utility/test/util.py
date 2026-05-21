"""Test utilities: command runners and environment helpers."""

import os
import subprocess
import sys

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest


def utcdt(s):
    """Parse an ISO date/datetime string as UTC. Assumes UTC if no timezone given."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


@contextmanager
def temporary_env(new_env):
    """Temporarily update os.environ with new_env."""
    original = os.environ.copy()

    # os.environ.update(new_env), but removing keys with None as value
    for k, v in new_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


# Running a command as an external command, to test we can


def _run_ext(env, name, args):
    """Run a management command as a subprocess and fail the test on non-zero exit.

    Args:
        env: Dict of additional environment variables.
        name: Management command name (e.g. ``'build_report'``).
        args: List of additional CLI arguments.

    Returns:
        :class:`subprocess.CompletedProcess` result.
    """
    result = subprocess.run(
        [sys.executable, 'manage.py', name, *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={'AWX_LOGGING_MODE': 'stdout', **env},
    )

    status = result.returncode

    if status != 0:
        pytest.fail(result.stderr)

    assert status == 0

    return result


def run_gather_ext(env, args):
    """Run the ``gather_automation_controller_billing_data`` command as an external subprocess.

    Args:
        env: Dict of additional environment variables.
        args: List of additional CLI arguments.

    Returns:
        :class:`subprocess.CompletedProcess` result.
    """
    return _run_ext(env, 'gather_automation_controller_billing_data', args)


# Running a command python way, so we can work with debugger in the code, and collect coverage


def run_gather_int(env, options):
    """Run the ``gather_automation_controller_billing_data`` command in-process.

    Args:
        env: Dict of environment variables to temporarily set.
        options: Dict of parsed CLI options forwarded to ``Command().handle``.
    """
    from metrics_utility.management.commands.gather_automation_controller_billing_data import Command

    with temporary_env(env):
        Command().handle(**options)
