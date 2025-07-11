from argparse import ArgumentParser

import pytest

from metrics_utility.exceptions import (
    BadRequiredEnvVar,
    BadShipTarget,
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

    with pytest.raises(MetricsException):
        command_instance.handle()


def test_handle_unexpected_exception(monkeypatch, command_instance):
    handle_env_validation = 'metrics_utility.management.commands.gather_automation_controller_billing_data.handle_env_validation'
    monkeypatch.setattr(handle_env_validation, lambda x: None)

    with pytest.raises(MetricsException):
        command_instance.handle()


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
