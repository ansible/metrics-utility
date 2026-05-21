import glob
import os

from datetime import datetime

import pytest

from metrics_utility.test.util import run_gather_ext, run_gather_int


env_vars = {
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


def test_collector_gating_default(cleanup_glob):
    """job_host_summary runs by default, main_jobevent runs as default optional collector."""
    rg = run_gather_ext(env_vars, ['--ship', '--since=2025-06-13', '--until=2025-06-14'])

    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Now gathering main_jobevent' in rg.stderr
    # main_host is not in optional_collectors by default
    assert 'Progress info: Skipping main_host' in rg.stderr


def test_collector_gating_disable_job_host_summary(cleanup_glob):
    """job_host_summary can be disabled via env var."""
    extra_env = {**env_vars, 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true'}
    rg = run_gather_ext(extra_env, ['--ship', '--since=2025-06-13', '--until=2025-06-14'])

    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Skipping job_host_summary' in rg.stderr


def test_collector_gating_disable_main_jobevent(cleanup_glob):
    """main_jobevent is skipped when optional_collectors is set to something else."""
    extra_env = {**env_vars, 'METRICS_UTILITY_OPTIONAL_COLLECTORS': ''}
    rg = run_gather_ext(extra_env, ['--ship', '--since=2025-06-13', '--until=2025-06-14'])

    assert 'Progress info: Now gathering main_jobevent' in rg.stderr
    assert 'Progress info: Skipping main_jobevent' in rg.stderr
