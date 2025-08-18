import os

import pytest

from openpyxl import load_workbook
from pandas import read_excel

from metrics_utility.exceptions import BadParameter, DateFormatError, UnparsableParameter
from metrics_utility.test.util import run_build_int, run_gather_int


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': 'ccsp_summary,managed_nodes,indirectly_managed_nodes,usage_by_organizations',
}
file_path = './metrics_utility/test/test_data/reports/2025/02/CCSPv2-2025-02-25--2025-02-26.xlsx'


def handle_build_exception(env_vars, params, klass):
    with pytest.raises(klass) as e:
        run_build_int(env_vars, params)
    return e.value


def test_uses_since_and_until_values():
    args = {'since': '2025-02-25', 'until': '2025-02-26', 'force': True}
    run_build_int({**env_vars, 'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2'}, args)
    try:
        workbook = load_workbook(filename=file_path)
        worksheet = read_excel(file_path, sheet_name='Usage Reporting')
        cell_value = worksheet.iat[3, 1]

        assert cell_value == '2025-02-25, 2025-02-26', f"Expected '2025-02-25, 2025-02-26' but got '{cell_value}'"
    finally:
        workbook.close()
        os.remove(file_path)


def test_accepts_month_and_ignores_since_until():
    e = handle_build_exception(
        env_vars,
        {'since': '2024-01-01', 'until': '2024-02-01', 'month': '2025-04', 'force': True},
        BadParameter,
    )
    assert e.name == 'The --since and --until parameters are not allowed if the --month parameter is provided.'


def test_renewal_guidance_fails_with_until_params():
    command_args = [
        {'args': {'until': '2025-01-01'}},
        {'args': {'until': '2025-01-01', 'since': '2024-02-01'}},
    ]
    env = {**env_vars, 'METRICS_UTILITY_REPORT_TYPE': 'RENEWAL_GUIDANCE', 'METRICS_UTILITY_SHIP_TARGET': 'controller_db'}
    for arg in command_args:
        e = handle_build_exception(env, arg['args'], BadParameter)
        assert e.name == 'The --until parameter is not allowed for renewal guidance report.'


def test_invalid_month_format():
    bad_inputs = ['abc', '12', 'mo3', '3mon', '3months', '3m', '3mo']

    for bad_input in bad_inputs:
        e = handle_build_exception(env_vars, {'month': bad_input}, DateFormatError)
        assert e.name == 'Invalid --month format. Supported date format: YYYY-MM'


def test_invalid_build_report_argument_format():
    from metrics_utility.management.commands.build_report import Command

    bad_inputs = ['2', '2y', 'mo3', '3weeks', '3w']
    args = ['until', 'since']

    inp_errors = [
        'Bare integers are not allowed',
        "Invalid isoformat string: '2y'",
        None,
        None,
        None,
    ]
    arg_errors = [
        'End date for collection',
        'Start date for collection',
    ]

    for bad_input, err_input in zip(bad_inputs, inp_errors):
        for arg, err_arg in zip(args, arg_errors):
            cmd = Command()

            # either {'since': bad_input} or {'since': (valid), 'until': bad_input}
            e = handle_build_exception(env_vars, {'since': '2024-01-01', arg: bad_input}, UnparsableParameter)

            assert (err_input or cmd.help_texts[arg]) in e.name
            assert err_arg in e.name


def test_ephemeral_allowed():
    """Allows value types:
    '1d', '2day', '3days', '4mo', '5month', '6months'
    """

    illegal_values = [
        '1da',
        '4mon',
        '5mons',
    ]

    env = {**env_vars, 'METRICS_UTILITY_REPORT_TYPE': 'RENEWAL_GUIDANCE', 'METRICS_UTILITY_SHIP_TARGET': 'controller_db'}
    for value in illegal_values:
        e = handle_build_exception(env, {'since': '2024-01-01', 'ephemeral': value}, UnparsableParameter)
        expected = (
            'Duration in months or days to determine if host is ephemeral.'
            ' Months are considered as 30 days in duration. Example: --ephemeral=3months, or'
            ' --ephemeral=3days'
        )
        actual = ' '.join(e.name.split())
        assert actual == expected


def handle_gather_exception(env_vars, params, klass):
    with pytest.raises(klass) as e:
        run_gather_int(env_vars, params)
    return e.value


def test_invalid_gather_argument_format():
    from metrics_utility.management.commands.gather_automation_controller_billing_data import Command

    bad_inputs = ['2', '2y', 'mo3', '3weeks', '3w']
    args = ['until', 'since']

    inp_errors = [
        'Bare integers are not allowed',
        "Invalid isoformat string: '2y'",
        None,
        None,
        None,
    ]
    arg_errors = [
        'End date for collection',
        'Start date for collection',
    ]

    for bad_input, err_input in zip(bad_inputs, inp_errors):
        for arg, err_arg in zip(args, arg_errors):
            cmd = Command()

            e = handle_gather_exception(env_vars, {arg: bad_input}, UnparsableParameter)

            assert (err_input or cmd.help_texts[arg]) in e.name
            assert err_arg in e.name
