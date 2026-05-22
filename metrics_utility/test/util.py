"""Test utilities: command runners and environment helpers."""

import csv
import os
import shutil
import subprocess
import sys

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest


def utcdt(s):
    """Parse an ISO date/datetime string as UTC. Assumes UTC if no timezone given."""
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


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
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={'AWX_LOGGING_MODE': 'stdout', **env},
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


def _print_comparison(actual_text, expected_lines):
    """Print actual and expected CSV content for debugging."""
    print('original --------------------------------')
    for line in actual_text:
        print(line)
    print('--------------------------------\n\n')

    print('expected --------------------------------')
    for line in expected_lines:
        print(line)
    print('--------------------------------\n\n')


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
    header, actual_data, text = _read_dataframe(df)

    _print_comparison(text, expected_lines)
    _validate_header(header, expected_header)
    _validate_row_count(actual_data, expected_data)

    # Sort both actual and expected data for consistent comparison
    actual_data_sorted = sorted(actual_data, key=lambda r: _get_sort_key(r, header))
    expected_data_sorted = sorted(expected_data, key=lambda r: _get_sort_key(r, header))

    _validate_rows(actual_data_sorted, expected_data_sorted, header, skip_columns_names)
