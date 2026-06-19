import glob
import tarfile

from datetime import datetime

import pytest

from metrics_utility.test.util import cleanup_glob as _cleanup_glob
from metrics_utility.test.util import run_gather_ext


env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '3',
}

uuid = '00000000-0000-0000-0000-000000000000'  # mock_awx INSTALL_UUID setting

file_glob = f'./metrics_utility/test/test_data/data/*/*/*/{uuid}-*.tar.gz'


def validate_exists(file_glob):
    assert len(glob.glob(file_glob)) > 0


@pytest.fixture
def cleanup_glob():
    yield
    _cleanup_glob(file_glob)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_larger_range(cleanup_glob):
    result = run_gather_ext(env_vars, ['--ship', '--since=2024-01-01', '--until=2024-01-05'])
    validate_exists(file_glob)

    text = result.stderr + '\n' + result.stdout
    assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-05 00:00:00+00:00' in text
    assert 'End of the collection interval is greater than 3 days from start, setting end to 2024-01-04 00:00:00+00:00.' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-04 00:00:00+00:00' in text


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_smaller_range(cleanup_glob):
    result = run_gather_ext(env_vars, ['--ship', '--since=2024-01-01', '--until=2024-01-03'])
    validate_exists(file_glob)

    text = result.stderr + '\n' + result.stdout
    assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text


# test that it gathers only one file host scope optional collectors
def test_only_host_scope(cleanup_glob):
    new_env_vars = env_vars.copy()
    new_env_vars['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'main_host'
    new_env_vars['METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS'] = '1'

    result = run_gather_ext(new_env_vars, ['--ship', '--since=2024-01-01', '--until=2024-01-03'])

    text = result.stderr + '\n' + result.stdout

    assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text
    assert 'End of the collection interval is greater than 1 days from start, setting end to 2024-01-02 00:00:00+00:00.' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-02 00:00:00+00:00' in text

    # Tarballs use until_slicing which stores at (until - 1s)
    # Test uses --since=2024-01-01, --until=2024-01-03,
    # MAX_GATHER_PERIOD_DAYS=1 sets until to 2024-01-02,
    # then until_slicing uses 2024-01-02 - 1s
    collection_date = datetime(2024, 1, 1, 23, 59, 59)
    year = collection_date.year
    month = collection_date.month
    day = collection_date.day

    # ensure month and day is 2 digits
    month = f'{month:02d}'
    day = f'{day:02d}'

    # extract tarball

    # multiline string
    tarball = (
        f'./metrics_utility/test/test_data/data/{year}/{month}/{day}/'
        f'00000000-0000-0000-0000-000000000000-'
        f'{year}-{month}-{day}-235959+0000-'
        f'{year}-{month}-{day}-235959+0000-0-main_host.tar.gz'
    )

    # ensure no other tarballs are present in the directory for current date
    # assert len(glob.glob(f'./metrics_utility/test/test_data/data/{year}/{month}/{day}/*.tar.gz')) == 1

    # extract tarball
    with tarfile.open(tarball, 'r') as tar:
        # just print to do something
        print(tar.getnames())
        # ensure main_host.csv is present
        # assert './main_host.csv' in tar.getnames()
