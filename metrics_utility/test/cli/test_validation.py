import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.commands.gather_automation_controller_billing_data import Command


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


def read_env(monkeypatch):
    """Call _read_env with minimum valid env for the directory target."""
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/tmp')
    return Command()._read_env()


def read_env_error(monkeypatch):
    """Call _read_env expecting MissingRequiredEnvVar, return the exception."""
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    with pytest.raises(MissingRequiredEnvVar) as exc:
        Command()._read_env()
    return exc.value


def test_validate_collectors_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
    read_env(monkeypatch)


def test_validate_collectors_total_workers_vcpu_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'total_workers_vcpu')
    read_env(monkeypatch)


def test_validate_collectors_multiple_including_total_workers_vcpu_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host,total_workers_vcpu,main_jobevent')
    read_env(monkeypatch)


def test_validate_collectors_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid_collector')
    e = read_env_error(monkeypatch)
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in e.name


def test_validate_max_gather_period_days_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '30')
    read_env(monkeypatch)


def test_validate_max_gather_period_days_valid_min_value(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '1')
    read_env(monkeypatch)


def test_validate_max_gather_period_days_valid_max_value(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '365')
    read_env(monkeypatch)


def test_validate_max_gather_period_days_not_set(monkeypatch):
    read_env(monkeypatch)


def test_validate_max_gather_period_days_invalid_negative(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '-5')
    e = read_env_error(monkeypatch)
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: -5' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_max_gather_period_days_invalid_too_large(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '4000')
    e = read_env_error(monkeypatch)
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: 4000' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_max_gather_period_days_invalid_non_integer(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', 'abc')
    e = read_env_error(monkeypatch)
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "abc"' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_max_gather_period_days_invalid_float(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '30.5')
    e = read_env_error(monkeypatch)
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "30.5"' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_ship_target_gather_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/tmp')
    ship_target, _, _ = Command()._read_env()
    assert ship_target == 'directory'


def test_validate_ship_target_gather_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid')
    with pytest.raises(MissingRequiredEnvVar) as exc:
        Command()._read_env()
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in exc.value.name


def test_validate_ship_path_empty_raises(monkeypatch):
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        Command._read_ship_params('directory')
    assert 'METRICS_UTILITY_SHIP_PATH' in excinfo.value.name


def test_read_env_multiple_errors(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        Command()._read_env()
    msg = excinfo.value.name
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_read_env_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', 'whatever')
    Command()._read_env()
