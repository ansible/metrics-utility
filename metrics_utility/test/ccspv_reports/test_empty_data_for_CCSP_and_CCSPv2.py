import os
import subprocess

import pytest
import openpyxl

# Define reports, date ranges, and sheet options
reports = [
    'CCSPv2',
    'CCSP',
]

ranges = [
    ['2025-04-02', '2025-04-02'],  # files with data
    ['2025-04-03', '2025-04-03'],  # no data at all (empty folder)
    ['2025-04-01', '2025-04-01'],  # empty csv files
    ['2025-04-01', '2025-04-03'],  # all of the above
]

options = [
    'ccsp_summary',
    'managed_nodes',
    'indirectly_managed_nodes',
    'inventory_scope',
    'usage_by_organizations',
    'usage_by_collections',
    'usage_by_roles',
    'usage_by_modules',
    'managed_nodes_by_organizations',
    'jobs',
    'data_collection_status',
]

def build_file_path(report, date_range):
    year, month, _ = date_range[1].split('-')
    return (
        f'./metrics_utility/test/test_data/reports/'
        f'{year}/{month}/{report}-{date_range[0]}--{date_range[1]}.xlsx'
    )

# Build all combinations of parameters
param_values = [
    (report, date_range, option, build_file_path(report, date_range))
    for report in reports
    for date_range in ranges
    for option in options
]

id_list = [
    f"{report}-{date_range[0]}--{date_range[1]}-{option}"
    for report, date_range, option, _ in param_values
]

@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'report,date_range,option,cleanup',
    param_values,
    indirect=['cleanup'],
    ids=id_list,
)
def test_empty_data(report, date_range, option, cleanup):
    since, until = date_range

    # Prepare the environment overrides
    overrides = {
        'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_REPORT_TYPE': report,
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': option,
    }
    env = os.environ.copy()
    env.update(overrides)

    # Build the command
    cmd = [
        'uv', 'run', './manage.py', 'build_report',
        f'--since={since}',
        f'--until={until}',
        '--force',
    ]

    # Inline the environment vars into the command text for diagnostics
    env_str = ' '.join(f"{k}={v!r}" for k, v in overrides.items())
    command_text = f"Command was:\n{env_str} {' '.join(cmd)}"

    # Run the command
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
    )

    # Verify exit code
    assert result.returncode == 0, (
        f"Build report failed.\n"
        f"{command_text}\n"
        f"Stdout:\n{result.stdout}\n"
        f"Stderr:\n{result.stderr}"
    )

    if "No billing data for input date range" not in result.stderr:
        file_name = build_file_path(report, date_range)

        # Verify the XLSX output is loadable
        workbook = openpyxl.load_workbook(filename=file_name)
        try:
            assert workbook is not None, (
                "Workbook load failed.\n"
                f"{command_text}"
            )
            # TODO: further sheet/field-level assertions as needed
        finally:
            workbook.close()
