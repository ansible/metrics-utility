import glob
import os

import pytest

from metrics_utility.test.util import run_gather_ext


env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './test_shipped_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

file_glob = './test_shipped_data/*.tar.gz'


def validate_exists(file_glob):
    assert len(glob.glob(file_glob)) > 0


@pytest.fixture
def cleanup_glob():
    yield
    for file in glob.glob(file_glob):
        os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command(cleanup_glob):
    """Build xlsx report using build command and test its contents."""

    run_gather_ext(env_vars, ['--ship', '--since=2025-06-13', '--until=2025-06-13'])

    validate_exists(file_glob)
