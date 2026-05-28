"""Test utilities: command runners and environment helpers."""

import csv
import os
import shutil
import subprocess
import sys

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock

import pytest


def utcdt(s):
    """Parse an ISO date/datetime string as UTC. Assumes UTC if no timezone given."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def mock_cursor_db():
    """Build a mock db connection with a cursor context manager."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_db, mock_cursor


def mock_copy_db(data_chunks):
    """Build a mock db connection that yields data_chunks from cursor.copy().read().

    data_chunks: list of bytes objects, each returned by successive read() calls.
    A final None is appended automatically to signal EOF.
    """
    mock_db, mock_cursor = mock_cursor_db()
    mock_copy = MagicMock()
    mock_cursor.copy.return_value.__enter__ = MagicMock(return_value=mock_copy)
    mock_cursor.copy.return_value.__exit__ = MagicMock(return_value=False)
    mock_copy.read.side_effect = [*data_chunks, None]
    return mock_db, mock_cursor


def mock_http_response(json_data=None, *, status_code=200, text=None):
    """Build a mock HTTP response with status_code and optional json/text."""
    response = Mock()
    response.status_code = status_code
    if json_data is not None:
        response.json.return_value = json_data
    if text is not None:
        response.text = text
    return response


@contextmanager
def temporary_env(new_env):
    """Temporarily update os.environ with new_env."""
    original = os.environ.copy()

    # os.environ.update(new_env), but removing keys with None as value
    for k, v in new_env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


# Running a command as an external command, to test we can

_SUBPROCESS_BASE_ENV = {
    'LANG': 'en_US.UTF-8',
    'PYTHONDONTWRITEBYTECODE': '1',
    'TZ': 'UTC',
}


def _db_env():
    """Reconstruct METRICS_UTILITY_DB_* from Django settings so subprocesses connect to the same DB."""
    from django.conf import settings

    db = settings.DATABASES['default']
    return {
        'METRICS_UTILITY_DB_HOST': db['HOST'],
        'METRICS_UTILITY_DB_PORT': db['PORT'],
        'METRICS_UTILITY_DB_NAME': db['NAME'],
        'METRICS_UTILITY_DB_USER': db['USER'],
        'METRICS_UTILITY_DB_PASSWORD': db['PASSWORD'],
    }


def _run_ext(env, name, args):
    """Run a management command as a subprocess and fail the test on non-zero exit.

    Args:
        env: Dict of additional environment variables.
        name: Management command name (e.g. ``'gather_automation_controller_billing_data'``).
        args: List of additional CLI arguments.

    Returns:
        :class:`subprocess.CompletedProcess` result.
    """
    result = subprocess.run(
        [sys.executable, 'manage.py', name, *args],
        text=True,
        capture_output=True,
        env={**_SUBPROCESS_BASE_ENV, **_db_env(), **env},
    )

    status = result.returncode

    if status != 0:
        pytest.fail(result.stderr)

    assert status == 0

    return result


def run_gather_ext(env, args):
    """Run the ``gather_automation_controller_billing_data`` command as an external subprocess.

    Args:
        env: Dict of additional environment variables.
        args: List of additional CLI arguments.

    Returns:
        :class:`subprocess.CompletedProcess` result.
    """
    return _run_ext(env, 'gather_automation_controller_billing_data', args)


# Running a command python way, so we can work with debugger in the code, and collect coverage


def run_gather_int(env, options):
    """Run the ``gather_automation_controller_billing_data`` command in-process.

    Args:
        env: Dict of environment variables to temporarily set.
        options: Dict of parsed CLI options forwarded to ``Command().handle``.
    """
    from metrics_utility.management.commands.gather_automation_controller_billing_data import Command

    with temporary_env(env):
        try:
            cmd = Command()
            cmd.handle(**options)
        finally:
            if cmd and getattr(cmd, 'collector', None) and cmd.collector.tmp_dir:
                shutil.rmtree(cmd.collector.tmp_dir, ignore_errors=True)


def _parse_expected_csv(expected_lines):
    """Parse expected CSV lines into header and data rows."""
    expected_reader = csv.reader(expected_lines)
    expected_rows = list(expected_reader)
    return expected_rows[0], expected_rows[1:]


def _read_dataframe(df):
    # Convert boolean columns from True/False to t/f
    # Convert float columns that are actually integers to Int64
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == 'bool':
            df_copy[col] = df_copy[col].map({True: 't', False: 'f'})
        elif df_copy[col].dtype in ['float64', 'float32']:
            # If all non-null values are whole numbers, convert to nullable int
            non_null_values = df_copy[col].dropna()
            if len(non_null_values) > 0 and (non_null_values == non_null_values.astype(int)).all():
                df_copy[col] = df_copy[col].astype('Int64')

    text = df_copy.to_csv(index=False).splitlines()
    reader = csv.reader(text)
    rows = list(reader)
    return rows[0], rows[1:], text


def _get_sort_key(row, header_row):
    """Create sort key from available columns: job_id, host_id, event, or first column."""
    key_parts = []
    sort_columns = ['job_id', 'host_id', 'event']

    for col_name in sort_columns:
        if col_name in header_row:
            idx = header_row.index(col_name)
            key_parts.append(row[idx] if idx < len(row) else '')

    # Fallback: use first column if no standard columns found
    if not key_parts and row:
        key_parts.append(row[0])

    return tuple(key_parts or ('',))


def _validate_header(actual_header, expected_header):
    """Validate that CSV headers match."""
    assert actual_header == expected_header, f'\nHeader mismatch:\nExpected: {expected_header}\nActual:   {actual_header}'


def _validate_row_count(actual_data, expected_data):
    """Validate that row counts match."""
    assert len(actual_data) == len(expected_data), f'\nRow count mismatch: expected {len(expected_data)}, got {len(actual_data)}'


def _validate_rows(actual_data_sorted, expected_data_sorted, header, skip_columns_names):
    """Validate that all rows match, skipping specified columns."""
    skip_columns = set(skip_columns_names)
    for i, (expected_row, actual_row) in enumerate(zip(expected_data_sorted, actual_data_sorted), start=1):
        for idx, (exp_cell, act_cell) in enumerate(zip(expected_row, actual_row)):
            col_name = header[idx]
            if col_name in skip_columns:
                continue
            assert exp_cell == act_cell, (
                f'\nData mismatch on row {i + 1}, column {col_name!r} (index {idx}):\nExpected: {exp_cell!r}\nActual:   {act_cell!r}'
            )


def validate_dataframe(df, expected_lines, skip_columns_names):
    """Validate DataFrame

    df: pandas DataFrame to validate
    expected_lines: list of strings where first is header, rest rows
    skip_columns_names: iterable of column names to skip comparison
    """
    expected_header, expected_data = _parse_expected_csv(expected_lines)
    header, actual_data, _text = _read_dataframe(df)

    _validate_header(header, expected_header)
    _validate_row_count(actual_data, expected_data)

    # Sort both actual and expected data for consistent comparison
    actual_data_sorted = sorted(actual_data, key=lambda r: _get_sort_key(r, header))
    expected_data_sorted = sorted(expected_data, key=lambda r: _get_sort_key(r, header))

    _validate_rows(actual_data_sorted, expected_data_sorted, header, skip_columns_names)
