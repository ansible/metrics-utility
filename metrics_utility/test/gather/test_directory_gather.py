import glob
import os

from datetime import datetime

import pytest

from metrics_utility.test.util import run_gather_ext, run_gather_int


env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

year = datetime.now().strftime('%Y')
uuid = '00000000-0000-0000-0000-000000000000'  # mock_awx INSTALL_UUID setting

file_glob = f'./metrics_utility/test/test_data/data/{year}/*/*/{uuid}-*.tar.gz'


def validate_exists(file_glob):
    assert len(glob.glob(file_glob)) > 0


@pytest.fixture
def cleanup_glob():
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command(cleanup_glob):
    run_gather_ext(env_vars, ['--ship', '--until=10m'])

    validate_exists(file_glob)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_import(cleanup_glob):
    # test_command doesn't collect coverage
    run_gather_int(
        env_vars,
        {
            'ship': True,
            'until': '10m',
        },
    )

    validate_exists(file_glob)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_assert_no_since_or_until_needed(cleanup_glob):
    run_gather_int(
        env_vars,
        {
            'ship': True,
        },
    )

    validate_exists(file_glob)
