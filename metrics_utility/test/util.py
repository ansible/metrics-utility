import os
import subprocess
import sys

from contextlib import contextmanager

import pytest


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


def run_build_ext(env, args):
    _run_ext(env, 'build_report', args)


def run_gather_ext(env, args):
    _run_ext(env, 'gather_automation_controller_billing_data', args)


# Running a command python way, so we can work with debugger in the code, and collect coverage


def run_build_int(env, options):
    from metrics_utility.management.commands.build_report import Command

    with temporary_env(env):
        Command().handle(**options)


def run_gather_int(env, options):
    from metrics_utility.management.commands.gather_automation_controller_billing_data import Command

    with temporary_env(env):
        # .handle does exit(0), breaking tests
        Command()._handle(**options)
