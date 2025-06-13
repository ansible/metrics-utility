from argparse import ArgumentParser

import pytest

from metrics_utility.exceptions import (
    BadParameter,
    BadRequiredEnvVar,
    BadShipTarget,
    DateFormatError,
    MissingRequiredEnvVar,
    MissingRequiredParameter,
    UnparsableParameter,
)
from metrics_utility.management.commands.build_report import Command


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

    # Check that all expected arguments are present
    expected_args = ['month', 'since', 'until', 'ephemeral', 'force', 'verbose']
    for arg in expected_args:
        assert arg in args


def test_handle_ship_target_directory(monkeypatch, command_instance):
    monkeypatch.setattr(
        'metrics_utility.management.commands.build_report.handle_directory_ship_target',
        lambda: {'ship_path': 'directory'},
    )
    assert command_instance._handle_ship_target('directory') == {'ship_path': 'directory'}


def test_handle_ship_target_s3(monkeypatch, command_instance):
    monkeypatch.setattr(
        'metrics_utility.management.commands.build_report.handle_s3_ship_target',
        lambda: {'ship_path': 's3'},
    )
    assert command_instance._handle_ship_target('s3') == {'ship_path': 's3'}


def test_handle_ship_target_invalid(command_instance):
    with pytest.raises(BadShipTarget):
        command_instance._handle_ship_target('invalid_target')


def test_handle_extra_params_missing_ship_path(monkeypatch, command_instance):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    monkeypatch.delenv('METRICS_UTILITY_SHIP_PATH', raising=False)
    with pytest.raises(MissingRequiredEnvVar):
        command_instance._handle_extra_params('directory')


def test_handle_extra_params_missing_report_type(monkeypatch, command_instance):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', 'directory')
    monkeypatch.delenv('METRICS_UTILITY_REPORT_TYPE', raising=False)
    with pytest.raises(MissingRequiredEnvVar):
        command_instance._handle_extra_params('directory')


def test_handle_extra_params_bad_report_type(monkeypatch, command_instance):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'BADTYPE')
    with pytest.raises(BadRequiredEnvVar):
        command_instance._handle_extra_params('directory')


def test_handle_extra_params_all_valid(monkeypatch, command_instance):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    params = command_instance._handle_extra_params('directory')
    assert params['ship_path'] == 'directory'
    assert params['report_type'] == 'CCSP'


def test_init_logging(command_instance):
    command_instance.init_logging()
    assert hasattr(command_instance, 'logger')
    assert command_instance.logger.name == 'awx.main.analytics'


def test_handle_success(monkeypatch, command_instance):
    # Patch handle_env_validation and _handle to simulate success
    monkeypatch.setattr(
        'metrics_utility.management.commands.build_report.handle_env_validation',
        lambda x: None,
    )
    monkeypatch.setattr(command_instance, '_handle', lambda *a, **k: None)
    # Patch logger to capture logs
    command_instance.logger = type(
        'Logger',
        (),
        {'error': lambda self, msg: None, 'exception': lambda self, msg: None},
    )()
    # Should not raise
    command_instance.handle()


@pytest.mark.parametrize(
    'exc',
    [
        BadShipTarget('bad'),
        MissingRequiredEnvVar('missing'),
        BadRequiredEnvVar('bad env'),
        MissingRequiredParameter('missing param'),
        UnparsableParameter('unparsable'),
        BadParameter('bad param'),
        DateFormatError('bad date'),
    ],
)
def test_handle_known_exceptions(monkeypatch, command_instance, exc):
    monkeypatch.setattr(
        'metrics_utility.management.commands.build_report.handle_env_validation',
        lambda x: None,
    )

    def raise_exc(*a, **k):
        raise exc

    monkeypatch.setattr(command_instance, '_handle', raise_exc)
    # Patch logger to capture error
    errors = []

    class Logger:
        def error(self, msg):
            errors.append(msg)

        def exception(self, msg):
            # used in tests
            pass

    command_instance.logger = Logger()
    with pytest.raises(SystemExit) as e:
        command_instance.handle()
    assert e.value.code == 1
    assert errors


def test_handle_unexpected_exception(monkeypatch, command_instance):
    monkeypatch.setattr(
        'metrics_utility.management.commands.build_report.handle_env_validation',
        lambda x: None,
    )

    def raise_exc(*a, **k):
        raise RuntimeError('unexpected')

    monkeypatch.setattr(command_instance, '_handle', raise_exc)
    # Patch logger to capture exception
    exceptions = []

    class Logger:
        def error(self, msg):
            # Used in Tests
            pass

        def exception(self, msg):
            exceptions.append(msg)

    command_instance.logger = Logger()
    with pytest.raises(SystemExit) as e:
        command_instance.handle()
    assert e.value.code == 1
    assert exceptions


def test_command_help(capsys):
    """
    Ensure that --help prints help text and exits cleanly.
    """
    parser = ArgumentParser(prog='build_report', add_help=True)
    cmd = Command()
    cmd.add_arguments(parser)
    with pytest.raises(SystemExit) as e:
        parser.parse_args(['--help'])
    out = capsys.readouterr().out
    assert 'usage:' in out
    assert '--month' in out
    assert '--since' in out
    assert '--until' in out
    assert '--ephemeral' in out
    assert '--force' in out
    assert '--verbose' in out
    assert e.value.code == 0
