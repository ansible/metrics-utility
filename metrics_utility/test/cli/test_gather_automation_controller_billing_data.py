from argparse import ArgumentParser

import pytest

from metrics_utility.exceptions import (
    FailedToUploadPayload,
    MetricsException,
    MissingRequiredEnvVar,
    UnparsableParameter,
)
from metrics_utility.management.commands.gather_automation_controller_billing_data import Command


@pytest.fixture
def command_instance():
    return Command()


@pytest.fixture
def parser():
    return ArgumentParser()


def test_add_arguments_adds_expected_arguments(parser):
    cmd = Command()
    cmd.add_arguments(parser)
    args = [a.dest for a in parser._actions]
    expected_args = ['dry-run', 'ship', 'since', 'until']
    for arg in expected_args:
        assert arg in args


def test_command_help(capsys):
    """
    Ensure that --help prints help text and exits cleanly.
    """
    from argparse import ArgumentParser

    from metrics_utility.management.commands.gather_automation_controller_billing_data import (
        Command,
    )

    parser = ArgumentParser(prog='gather_automation_controller_billing_data', add_help=True)
    cmd = Command()
    cmd.add_arguments(parser)
    with pytest.raises(SystemExit) as e:
        parser.parse_args(['--help'])
    out = capsys.readouterr().out
    assert 'usage:' in out
    assert '--dry-run' in out
    assert '--ship' in out
    assert '--since' in out
    assert '--until' in out
    assert e.value.code == 0


@pytest.mark.parametrize(
    'exc',
    [
        MissingRequiredEnvVar('missing'),
        FailedToUploadPayload('fail'),
        UnparsableParameter('unparsable'),
    ],
)
def test_handle_known_exceptions(command_instance, exc):
    command_instance._read_env = lambda: (_ for _ in ()).throw(exc)

    with pytest.raises(MetricsException):
        command_instance.handle()
