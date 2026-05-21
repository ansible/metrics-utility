import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.validation import (
    handle_directory_ship_target,
    handle_env_validation,
    validate_collectors,
    validate_max_gather_period_days,
    validate_ship_target,
)


# Error message constants
MAX_GATHER_DAYS_ERROR_MSG = 'Value must be number between 0 to 3650'


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    keys = [
        'METRICS_UTILITY_OPTIONAL_COLLECTORS',
        'METRICS_UTILITY_SHIP_PATH',
        'METRICS_UTILITY_SHIP_TARGET',
        'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS',
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield


def test_validate_collectors_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
    errors = []
    validate_collectors(errors)
    assert not errors


def test_validate_collectors_total_workers_vcpu_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'total_workers_vcpu')
    errors = []
    validate_collectors(errors)
    assert not errors


def test_validate_collectors_multiple_including_total_workers_vcpu_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host,total_workers_vcpu,main_jobevent')
    errors = []
    validate_collectors(errors)
    assert not errors


def test_validate_collectors_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid_collector')
    errors = []
    validate_collectors(errors)
    assert errors
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in errors[0]


def test_validate_max_gather_period_days_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '30')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result == 30
    assert not errors


def test_validate_max_gather_period_days_valid_min_value(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '1')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result == 1
    assert not errors


def test_validate_max_gather_period_days_valid_max_value(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '365')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result == 365
    assert not errors


def test_validate_max_gather_period_days_not_set():
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result is None
    assert not errors


def test_validate_max_gather_period_days_invalid_negative(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '-5')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result is None
    assert errors
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: -5' in errors[0]
    assert MAX_GATHER_DAYS_ERROR_MSG in errors[0]


def test_validate_max_gather_period_days_invalid_too_large(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '4000')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result is None
    assert errors
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: 4000' in errors[0]
    assert MAX_GATHER_DAYS_ERROR_MSG in errors[0]


def test_validate_max_gather_period_days_invalid_non_integer(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', 'abc')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result is None
    assert errors
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "abc"' in errors[0]
    assert MAX_GATHER_DAYS_ERROR_MSG in errors[0]


def test_validate_max_gather_period_days_invalid_float(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '30.5')
    errors = []
    result = validate_max_gather_period_days(errors)
    assert result is None
    assert errors
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "30.5"' in errors[0]
    assert MAX_GATHER_DAYS_ERROR_MSG in errors[0]


def test_validate_ship_target_gather_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    errors = []
    result = validate_ship_target(errors)
    assert result == 'directory'
    assert not errors


def test_validate_ship_target_gather_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid')
    errors = []
    result = validate_ship_target(errors)
    assert result == 'invalid'
    assert errors
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in errors[0]


def test_validate_ship_path_empty_raises(monkeypatch):
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_directory_ship_target()
    assert str(excinfo.value).startswith('Missing required env variable METRICS_UTILITY_SHIP_PATH')


def test_handle_env_validation_gather_raises(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation()
    msg = str(excinfo.value)
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_handle_env_validation_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', 'whatever')
    handle_env_validation()
