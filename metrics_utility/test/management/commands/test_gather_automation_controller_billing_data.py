import datetime

from argparse import ArgumentParser

import pytest

from metrics_utility.exceptions import (
    BadRequiredEnvVar,
    BadShipTarget,
    FailedToUploadPayload,
    MissingRequiredEnvVar,
    UnparsableParameter,
)
from metrics_utility.management.commands.gather_automation_controller_billing_data import (
    Command,
)


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


def test_handle_success(monkeypatch, command_instance):
    handle_env_validation = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_env_validation'
    monkeypatch.setattr(handle_env_validation, lambda x: None)
    monkeypatch.setattr(command_instance, '_handle', lambda *a, **k: None)
    command_instance.logger = type(
        'Logger',
        (),
        {'error': lambda self, msg: None, 'exception': lambda self, msg: None},
    )()
    with pytest.raises(SystemExit) as e:
        command_instance.handle()
    assert e.value.code == 0


@pytest.mark.parametrize(
    'exc',
    [
        BadShipTarget('bad'),
        MissingRequiredEnvVar('missing'),
        BadRequiredEnvVar('bad env'),
        FailedToUploadPayload('fail'),
        UnparsableParameter('unparsable'),
    ],
)
def test_handle_known_exceptions(monkeypatch, command_instance, exc):
    handle_env_validation = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_env_validation'
    monkeypatch.setattr(handle_env_validation, lambda x: None)

    def raise_exc(*a, **k):
        raise exc

    monkeypatch.setattr(command_instance, '_handle', raise_exc)
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
    handle_env_validation = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_env_validation'
    monkeypatch.setattr(handle_env_validation, lambda x: None)

    def raise_exc(*a, **k):
        raise RuntimeError('unexpected')

    monkeypatch.setattr(command_instance, '_handle', raise_exc)
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


def test_handle_datelike_days(command_instance):
    days = 2
    val = f'{days}d'
    dt = command_instance._handle_datelike(val)
    assert isinstance(dt, datetime.datetime)
    assert dt.tzinfo is not None


def test_handle_datelike_minutes(command_instance):
    mins = 5
    val = f'{mins}m'
    dt = command_instance._handle_datelike(val)
    assert isinstance(dt, datetime.datetime)
    assert dt.tzinfo is not None


def test_handle_datelike_iso(command_instance):
    val = '2024-01-01T00:00:00Z'
    dt = command_instance._handle_datelike(val)
    assert isinstance(dt, datetime.datetime)
    assert dt.tzinfo is not None


def test_handle_interval(command_instance):
    since = '2024-01-01T00:00:00Z'
    until = '2024-01-02T00:00:00Z'
    s, u = command_instance._handle_interval(since, until)
    assert isinstance(s, datetime.datetime)
    assert isinstance(u, datetime.datetime)


def test_handle_ship_target_crc(monkeypatch, command_instance):
    handle_not_s3 = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_not_s3'
    handle_crc_ship_target = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_crc_ship_target'
    monkeypatch.setattr(handle_not_s3, lambda: None)
    monkeypatch.setattr(handle_crc_ship_target, lambda: {'ship_path': 'crc'})
    assert command_instance._handle_ship_target('crc') == {'ship_path': 'crc'}


def test_handle_ship_target_directory(monkeypatch, command_instance):
    handle_not_crc = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_not_crc'
    handle_not_s3 = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_not_s3'
    handle_directory_ship_target = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_directory_ship_target'
    monkeypatch.setattr(handle_not_crc, lambda: None)
    monkeypatch.setattr(handle_not_s3, lambda: None)
    monkeypatch.setattr(
        handle_directory_ship_target,
        lambda: {'ship_path': 'directory'},
    )
    assert command_instance._handle_ship_target('directory') == {'ship_path': 'directory'}


def test_handle_ship_target_s3(monkeypatch, command_instance):
    handle_not_crc = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_not_crc'
    handle_s3_ship_target = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_s3_ship_target'
    monkeypatch.setattr(handle_not_crc, lambda: None)
    monkeypatch.setattr(handle_s3_ship_target, lambda: {'ship_path': 's3'})
    assert command_instance._handle_ship_target('s3') == {'ship_path': 's3'}


def test_handle_ship_target_invalid(command_instance):
    with pytest.raises(BadShipTarget):
        command_instance._handle_ship_target('invalid')
