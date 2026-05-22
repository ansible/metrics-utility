import glob
import os

from metrics_utility.test.util import run_gather_ext


uuid = '00000000-0000-0000-0000-000000000000'  # mock_awx INSTALL_UUID setting


def make_env(ship_path):
    return {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS': '3',
    }


def make_glob(ship_path):
    return f'{ship_path}/data/*/*/*/{uuid}-*.tar.gz'


def validate_exists(file_glob):
    assert len(glob.glob(file_glob)) > 0


def test_larger_range(ship_path):
    result = run_gather_ext(make_env(ship_path), ['--ship', '--since=2024-01-01', '--until=2024-01-05'])
    validate_exists(make_glob(ship_path))

    text = result.stderr + '\n' + result.stdout
    assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-05 00:00:00+00:00' in text
    assert 'End of the collection interval is greater than 3 days from start, setting end to 2024-01-04 00:00:00+00:00.' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-04 00:00:00+00:00' in text


def test_smaller_range(ship_path):
    result = run_gather_ext(make_env(ship_path), ['--ship', '--since=2024-01-01', '--until=2024-01-03'])
    validate_exists(make_glob(ship_path))

    text = result.stderr + '\n' + result.stdout
    assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text


def test_only_host_scope(ship_path):
    env = make_env(ship_path)
    env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'main_host'
    env['METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS'] = '1'

    result = run_gather_ext(env, ['--ship', '--since=2024-01-01', '--until=2024-01-03'])

    text = result.stderr + '\n' + result.stdout

    assert 'Original since-until: 2024-01-01 00:00:00+00:00 to 2024-01-03 00:00:00+00:00' in text
    assert 'End of the collection interval is greater than 1 days from start, setting end to 2024-01-02 00:00:00+00:00.' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-02 00:00:00+00:00' in text

    # Tarballs use until_slicing which stores at (until - 1s)
    # Test uses --since=2024-01-01, --until=2024-01-03,
    # MAX_GATHER_PERIOD_DAYS=1 sets until to 2024-01-02,
    # then until_slicing uses 2024-01-02 - 1s
    tarball = f'{ship_path}/data/2024/01/01/00000000-0000-0000-0000-000000000000-2024-01-01-235959+0000-2024-01-01-235959+0000-0-main_host.tar.gz'

    assert os.path.exists(tarball)


def test_since_only(ship_path):
    result = run_gather_ext(make_env(ship_path), ['--ship', '--since=2024-01-01'])
    validate_exists(make_glob(ship_path))

    text = result.stderr + '\n' + result.stdout
    assert 'End of the collection interval set to 2024-01-04 00:00:00+00:00.' in text
    assert 'Final since-until: 2024-01-01 00:00:00+00:00 to 2024-01-04 00:00:00+00:00' in text


def test_no_since_no_until(ship_path):
    result = run_gather_ext(make_env(ship_path), ['--ship'])
    validate_exists(make_glob(ship_path))

    text = result.stderr + '\n' + result.stdout
    assert 'End of the collection interval set to ' in text
    assert 'Final since-until: ' in text
    assert 'Final since-until: None' not in text
