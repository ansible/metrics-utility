import tempfile

import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.validation import (
    handle_directory_ship_target,
    handle_env_validation,
    validate_ccsp_report_sheets,
    validate_collectors,
    validate_max_gather_period_days,
    validate_report_type,
    validate_ship_path,
    validate_ship_target,
)


# Error message constants
MAX_GATHER_DAYS_ERROR_MSG = 'Value must be number between 0 to 3650'


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    # Clear relevant env vars before each test
    keys = [
        'METRICS_UTILITY_REPORT_TYPE',
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS',
        'METRICS_UTILITY_SHIP_PATH',
        'METRICS_UTILITY_SHIP_TARGET',
        'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS',
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield


def test_validate_report_type_build_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    errors = []
    result = validate_report_type(errors, 'build')
    assert result == 'CCSP'
    assert not errors


def test_validate_report_type_gather(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'ignored')
    errors = []
    result = validate_report_type(errors, 'gather')
    assert result is None
    assert not errors


def test_validate_report_type_build_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'INVALID')
    errors = []
    result = validate_report_type(errors, 'build')
    assert result == 'INVALID'
    assert errors
    assert 'Invalid METRICS_UTILITY_REPORT_TYPE' in errors[0]


def test_validate_ccsp_report_sheets_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'ccsp_summary,managed_nodes')
    errors = []
    validate_ccsp_report_sheets(errors, 'CCSP')
    assert not errors


def test_validate_ccsp_report_sheets_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'ccsp_summary,invalid_sheet')
    errors = []
    validate_ccsp_report_sheets(errors, 'CCSP')
    assert errors
    assert 'Invalid METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS' in errors[0]


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


def test_validate_ship_target_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    VALID_SHIP_TARGET_BUILD = {'directory', 's3', 'controller_db'}
    errors = []
    result = validate_ship_target(errors, VALID_SHIP_TARGET_BUILD, 'CCSPv2')
    assert result == 'directory'
    assert not errors


def test_validate_ship_target_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid')
    VALID_SHIP_TARGET_BUILD = {'directory', 's3', 'controller_db'}
    errors = []
    result = validate_ship_target(errors, VALID_SHIP_TARGET_BUILD, 'CCSPv2')
    assert result == 'invalid'
    assert errors
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in errors[0]


def test_validate_ship_target_gather_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    VALID_SHIP_TARGET_GATHER = {'directory', 's3', 'crc'}
    errors = []
    result = validate_ship_target(errors, VALID_SHIP_TARGET_GATHER, None)
    assert result == 'directory'
    assert not errors


def test_validate_ship_target_gather_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid')
    VALID_SHIP_TARGET_GATHER = {'directory', 's3', 'crc'}
    errors = []
    result = validate_ship_target(errors, VALID_SHIP_TARGET_GATHER, None)
    assert result == 'invalid'
    assert errors
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in errors[0]


def test_validate_ship_path_build_valid(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', tmpdir)
        errors = []
        validate_ship_path(errors, 'directory', 'build')
        assert not errors


def test_validate_ship_path_build_empty_build_valid(monkeypatch):
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_directory_ship_target()
    assert str(excinfo.value).startswith('Missing required env variable METRICS_UTILITY_SHIP_PATH')


def test_validate_ship_path_build_empty_gather_valid(monkeypatch):
    errors = []
    validate_ship_path(errors, 'directory', 'gather')
    assert not errors


def test_validate_ship_path_gather_valid(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', tmpdir)
        errors = []
        validate_ship_path(errors, 'directory', 'gather')
        assert not errors


def test_validate_ship_path_build_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    errors = []
    validate_ship_path(errors, 'directory', 'build')
    assert errors
    assert 'Invalid METRICS_UTILITY_SHIP_PATH' in errors[0]


def test_validate_ship_path_gather_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    errors = []
    validate_ship_path(errors, 'directory', 'gather')
    assert not errors


def test_handle_env_validation_all_build_valid(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
        monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'ccsp_summary')
        monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
        monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
        monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', tmpdir)
        monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', '30')
        # Should not raise
        handle_env_validation('build')


def test_handle_env_validation_with_invalid_max_gather_period_days(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
        monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'ccsp_summary')
        monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
        monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
        monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', tmpdir)
        monkeypatch.setenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS', 'invalid')
        with pytest.raises(MissingRequiredEnvVar) as excinfo:
            handle_env_validation('build')
        msg = str(excinfo.value)
        assert 'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS' in msg


def test_handle_env_validation_gather_raises1(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'INVALID')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'egg,fried')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation('gather')
    msg = str(excinfo.value)
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_handle_env_validation_raises_valid_build_report_type(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'egg,fried')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation('build')
    msg = str(excinfo.value)
    assert 'Invalid METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS' in msg
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_handle_env_validation_gather_raises2(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'INVALID')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'egg,fried')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation('gather')
    msg = str(excinfo.value)
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_handle_env_validation_raises_valid_buid_report_type(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'egg,fried')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation('build')
    msg = str(excinfo.value)
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_report_type_ignored_in_gather(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'RENEWAL_GUIDANCE')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', 'whatever')
    # Only "controller_db" is allowed for "RENEWAL_GUIDANCE" .. but only in build_report
    handle_env_validation('gather')
