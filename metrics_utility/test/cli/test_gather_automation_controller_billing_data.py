import logging

from argparse import ArgumentParser
from unittest.mock import MagicMock, patch

import pytest

from metrics_utility.exceptions import (
    FailedToUploadPayload,
    MetricsException,
    MissingRequiredEnvVar,
    NoAnalyticsCollected,
    UnparsableParameter,
)
from metrics_utility.management.commands.gather_automation_controller_billing_data import Command
from metrics_utility.test.util import temporary_env


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


def test_create_parser():
    cmd = Command()
    parser = cmd.create_parser('manage.py', 'gather_automation_controller_billing_data')
    assert parser is not None
    # epilog should contain environment variable documentation
    assert 'ENVIRONMENT' in parser.epilog


def test_handle_verbose():
    cmd = Command()
    cmd._read_env = lambda: ('directory', {}, {'ship_path': '/tmp'})

    mock_collector = MagicMock()
    mock_collector.gather.return_value = ['/tmp/test.tar.gz']

    with patch('metrics_utility.management.commands.gather_automation_controller_billing_data.Collector', return_value=mock_collector):
        cmd.handle(verbose=True, ship=False, since=None, until=None)

    from metrics_utility.logger import logger

    assert logger.level <= logging.DEBUG


def test_handle_no_analytics_collected():
    cmd = Command()
    cmd._read_env = lambda: ('directory', {}, {'ship_path': '/tmp'})

    mock_collector = MagicMock()
    mock_collector.gather.return_value = None

    with patch('metrics_utility.management.commands.gather_automation_controller_billing_data.Collector', return_value=mock_collector):
        with pytest.raises(NoAnalyticsCollected):
            cmd.handle(verbose=False, ship=False, since=None, until=None)


def test_warn_surplus_env_vars():
    env = {
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_SHIP_PATH': '/tmp',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS': None,
        'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': None,
        'METRICS_UTILITY_BUCKET_NAME': 'my-bucket',
    }
    with temporary_env(env):
        with patch('metrics_utility.management.commands.gather_automation_controller_billing_data.logger') as mock_logger:
            Command()._read_env()
        mock_logger.warning.assert_called()
        call_args = mock_logger.warning.call_args[0][0]
        assert 'METRICS_UTILITY_BUCKET_NAME' in call_args
