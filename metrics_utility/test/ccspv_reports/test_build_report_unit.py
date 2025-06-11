from unittest import mock

import pytest

from metrics_utility.exceptions import BadRequiredEnvVar, BadShipTarget, MissingRequiredEnvVar
from metrics_utility.management.commands.build_report import Command


@pytest.fixture
def command_instance():
    return Command()


def test_add_arguments_calls_env_validation(monkeypatch, command_instance):
    called = {}

    def fake_handle_env_validation(build):
        called['called'] = True

    monkeypatch.setattr(
        'metrics_utility.management.commands.build_report.handle_env_validation',
        fake_handle_env_validation,
    )
    parser = mock.Mock()
    command_instance.add_arguments(parser)
    assert called['called']


def test_handle_ship_target_directory(monkeypatch, command_instance):
    monkeypatch.setattr('metrics_utility.management.commands.build_report.handle_directory_ship_target', lambda: {'ship_path': 'directory'})
    assert command_instance._handle_ship_target('directory') == {'ship_path': 'directory'}


def test_handle_ship_target_s3(monkeypatch, command_instance):
    monkeypatch.setattr('metrics_utility.management.commands.build_report.handle_s3_ship_target', lambda: {'ship_path': 's3'})
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
