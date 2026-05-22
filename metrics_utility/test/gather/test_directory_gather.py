import glob

from datetime import datetime

from metrics_utility.test.util import run_gather_ext, run_gather_int


year = datetime.now().strftime('%Y')
uuid = '00000000-0000-0000-0000-000000000000'  # mock_awx INSTALL_UUID setting


def make_env(ship_path):
    return {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }


def make_glob(ship_path):
    return f'{ship_path}/data/{year}/*/*/{uuid}-*.tar.gz'


def validate_exists(file_glob):
    assert len(glob.glob(file_glob)) > 0


def test_command(ship_path):
    run_gather_ext(make_env(ship_path), ['--ship', '--until=10m'])

    validate_exists(make_glob(ship_path))


def test_import(ship_path):
    run_gather_int(
        make_env(ship_path),
        {
            'ship': True,
            'until': '10m',
        },
    )

    validate_exists(make_glob(ship_path))


def test_assert_no_since_or_until_needed(ship_path):
    run_gather_int(
        make_env(ship_path),
        {
            'ship': True,
        },
    )

    validate_exists(make_glob(ship_path))


def test_collector_gating_default(ship_path):
    """job_host_summary runs by default, main_jobevent runs as default optional collector."""
    rg = run_gather_ext(make_env(ship_path), ['--ship', '--since=2025-06-13', '--until=2025-06-14'])

    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Now gathering main_jobevent' in rg.stderr
    # main_host is not in optional_collectors by default
    assert 'Progress info: Disabled main_host' in rg.stderr


def test_collector_gating_disable_job_host_summary(ship_path):
    """job_host_summary can be disabled via env var."""
    extra_env = {**make_env(ship_path), 'METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR': 'true'}
    rg = run_gather_ext(extra_env, ['--ship', '--since=2025-06-13', '--until=2025-06-14'])

    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Disabled job_host_summary' in rg.stderr


def test_collector_gating_disable_main_jobevent(ship_path):
    """main_jobevent is disabled when optional_collectors is set to something else."""
    extra_env = {**make_env(ship_path), 'METRICS_UTILITY_OPTIONAL_COLLECTORS': ''}
    rg = run_gather_ext(extra_env, ['--ship', '--since=2025-06-13', '--until=2025-06-14'])

    assert 'Progress info: Now gathering main_jobevent' in rg.stderr
    assert 'Progress info: Disabled main_jobevent' in rg.stderr
