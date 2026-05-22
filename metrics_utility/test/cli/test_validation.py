import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.commands.gather_automation_controller_billing_data import Command
from metrics_utility.test.util import temporary_env


MAX_GATHER_DAYS_ERROR_MSG = 'Value must be number between 0 to 3650'

CLEAN_ENV = {
    'METRICS_UTILITY_OPTIONAL_COLLECTORS': None,
    'METRICS_UTILITY_SHIP_PATH': None,
    'METRICS_UTILITY_SHIP_TARGET': None,
    'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': None,
}


def read_env(extra=None):
    """Call _read_env with minimum valid env for the directory target."""
    env = {**CLEAN_ENV, 'METRICS_UTILITY_SHIP_TARGET': 'directory', 'METRICS_UTILITY_SHIP_PATH': '/tmp', **(extra or {})}
    with temporary_env(env):
        return Command()._read_env()


def read_env_error(extra=None):
    """Call _read_env expecting MissingRequiredEnvVar, return the exception."""
    env = {**CLEAN_ENV, 'METRICS_UTILITY_SHIP_TARGET': 'directory', **(extra or {})}
    with temporary_env(env):
        with pytest.raises(MissingRequiredEnvVar) as exc:
            Command()._read_env()
        return exc.value


def test_validate_collectors_valid():
    read_env({'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host'})


def test_validate_collectors_total_workers_vcpu_valid():
    read_env({'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'total_workers_vcpu'})


def test_validate_collectors_multiple_including_total_workers_vcpu_valid():
    read_env({'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,total_workers_vcpu,main_jobevent'})


def test_validate_collectors_invalid():
    e = read_env_error({'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'invalid_collector'})
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in e.name


def test_validate_max_gather_period_days_valid():
    read_env({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '30'})


def test_validate_max_gather_period_days_valid_min_value():
    read_env({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '1'})


def test_validate_max_gather_period_days_valid_max_value():
    read_env({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '365'})


def test_validate_max_gather_period_days_not_set():
    read_env()


def test_validate_max_gather_period_days_invalid_negative():
    e = read_env_error({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '-5'})
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: -5' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_max_gather_period_days_invalid_too_large():
    e = read_env_error({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '4000'})
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: 4000' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_max_gather_period_days_invalid_non_integer():
    e = read_env_error({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': 'abc'})
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "abc"' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_max_gather_period_days_invalid_float():
    e = read_env_error({'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '30.5'})
    assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "30.5"' in e.name
    assert MAX_GATHER_DAYS_ERROR_MSG in e.name


def test_validate_ship_target_gather_valid():
    ship_target, _, _ = read_env()
    assert ship_target == 'directory'


def test_validate_ship_target_gather_invalid():
    e = read_env_error({'METRICS_UTILITY_SHIP_TARGET': 'invalid'})
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in e.name


def test_validate_ship_path_empty_raises():
    with temporary_env(CLEAN_ENV):
        with pytest.raises(MissingRequiredEnvVar) as excinfo:
            Command._read_ship_params('directory')
        assert 'METRICS_UTILITY_SHIP_PATH' in excinfo.value.name


def test_read_env_multiple_errors():
    e = read_env_error(
        {
            'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'invalid,page',
            'METRICS_UTILITY_SHIP_TARGET': 'invalid_path',
            'METRICS_UTILITY_SHIP_PATH': '/non/existing/dir',
        }
    )
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in e.name
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in e.name


def test_read_env_valid():
    read_env(
        {
            'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host',
            'METRICS_UTILITY_SHIP_PATH': 'whatever',
        }
    )
