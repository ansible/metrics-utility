import tempfile

import pytest

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.validation import (
    handle_env_validation,
    validate_ccsp_report_sheets,
    validate_collectors,
    validate_report_type,
    validate_ship_path,
    validate_ship_target,
)


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    # Clear relevant env vars before each test
    keys = [
        'METRICS_UTILITY_REPORT_TYPE',
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS',
        'METRICS_UTILITY_SHIP_PATH',
        'METRICS_UTILITY_SHIP_TARGET',
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield


def test_validate_report_type_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    errors = []
    result = validate_report_type(errors)
    assert result == 'CCSP'
    assert not errors


def test_validate_report_type_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'INVALID')
    errors = []
    result = validate_report_type(errors)
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


def test_validate_collectors_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid_collector')
    errors = []
    validate_collectors(errors)
    assert errors
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in errors[0]


def test_validate_ship_target_valid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
    errors = []
    result = validate_ship_target(errors)
    assert result == 'directory'
    assert not errors


def test_validate_ship_target_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid')
    errors = []
    result = validate_ship_target(errors)
    assert result == 'invalid'
    assert errors
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in errors[0]


def test_validate_ship_path_valid(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', tmpdir)
        errors = []
        validate_ship_path(errors, 'directory')
        assert not errors


def test_validate_ship_path_invalid(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    errors = []
    validate_ship_path(errors, 'directory')
    assert errors
    assert 'Invalid METRICS_UTILITY_SHIP_PATH' in errors[0]


def test_handle_env_validation_all_valid(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
        monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'ccsp_summary')
        monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_host')
        monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'directory')
        monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', tmpdir)
        # Should not raise
        handle_env_validation()


def test_handle_env_validation_raises(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'INVALID')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'egg,fried')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation()
    msg = str(excinfo.value)
    print(f'full message: {msg}')
    assert 'Invalid METRICS_UTILITY_REPORT_TYPE' in msg
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg


def test_handle_env_validation_raises_valid_report_type(monkeypatch):
    monkeypatch.setenv('METRICS_UTILITY_REPORT_TYPE', 'CCSP')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS', 'egg,fried')
    monkeypatch.setenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'invalid,page')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_TARGET', 'invalid_path')
    monkeypatch.setenv('METRICS_UTILITY_SHIP_PATH', '/non/existing/dir')
    with pytest.raises(MissingRequiredEnvVar) as excinfo:
        handle_env_validation()
    msg = str(excinfo.value)
    print(f'full message: {msg}')
    assert 'Invalid METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS' in msg
    assert 'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS' in msg
    assert 'Invalid METRICS_UTILITY_SHIP_TARGET' in msg
