import glob

import pytest

from metrics_utility.test.util import run_gather_ext


env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '3',
}

year = 2024
uuid = '00000000-0000-0000-0000-000000000000'  # mock_awx INSTALL_UUID setting

file_glob = f'./metrics_utility/test/test_data/data/{year}/*/*/{uuid}-*.tar.gz'


def validate_exists(file_glob):
    assert len(glob.glob(file_glob)) > 0


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_larger_range(cleanup_glob):
    result = run_gather_ext(env_vars, ['--ship', '--since=2024-01-01', '--until=2024-01-05'])

    #validate_exists(file_glob)

    text = result.stderr + '\n' + result.stdout

    #assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-05 00:00:00+00:00' in text
    #assert 'End of the collection interval is greater than 3 days from start, setting end to 2024-01-04 00:00:00+00:00.' in text
    #assert (
    #    'Start of the collection interval is more than 3 days prior to 2024-01-04 23:59:59.999999+00:00, setting to 2024-01-01 23:59:59.999999+00:00.'
    #    in text
    #)
    #assert 'Final since-until: 2024-01-01 23:59:59.999999+00:00 to 2024-01-04 23:59:59.999999+00:00' in text


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_smaller_range(cleanup_glob):
    result = run_gather_ext(env_vars, ['--ship', '--since=2024-01-01', '--until=2024-01-03'])
    #validate_exists(file_glob)
    text = result.stderr + '\n' + result.stdout

    print(text)
    #assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text
    #assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 23:59:59.999999+00:00' in text
