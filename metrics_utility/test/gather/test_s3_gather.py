import os

import pytest

from metrics_utility.test.util import run_build_ext, run_gather_ext, run_gather_int


env_vars = {
    'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'myuseraccesskey',
    'METRICS_UTILITY_BUCKET_ENDPOINT': os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', 'http://localhost:9000'),  # or http://minio:9000
    'METRICS_UTILITY_BUCKET_NAME': 'metricsutilitys3',
    'METRICS_UTILITY_BUCKET_REGION': 'us-east-1',
    'METRICS_UTILITY_BUCKET_SECRET_KEY': 'myusersecretkey',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': f'metrics-utility/shipped_data_{os.getpid()}',
    'METRICS_UTILITY_SHIP_TARGET': 's3',
}


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command():
    """Build xlsx report using build command and test its contents."""
    run_gather_ext(env_vars, ['--ship', '--until=10m'])
    # mc ls -r local/metricsutilitys3/metrics-utility/shipped_data_*/data/


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_import():
    # test_command doesn't collect coverage
    run_gather_int(
        env_vars,
        {
            'ship': True,
            'until': '10m',
        },
    )


def test_full():
    rg = run_gather_ext(env_vars, ['--ship', '--since=2025-06-13', '--until=2025-06-14'])
    rb = run_build_ext(env_vars, ['--since=2025-06-13', '--until=2025-06-13'])

    assert 'Final since-until: 2025-06-13 00:00:00+00:00 to 2025-06-14 00:00:00+00:00' in rg.stderr
    assert 'Progress info: Now gathering job_host_summary' in rg.stderr
    assert 'Progress info: Skipping main_host because it is not enabled.' in rg.stderr
    assert 'Progress info: Skipping main_indirectmanagednodeaudit because it is not enabled.' in rg.stderr
    assert 'Progress info: Now gathering main_jobevent' in rg.stderr
    assert 'Analytics collected' in rg.stderr

    output_filename = env_vars['METRICS_UTILITY_SHIP_PATH'] + '/reports/2025/06/CCSPv2-2025-06-13--2025-06-13.xlsx'
    assert f'Report sent into S3 bucket into path: {output_filename}' in rb.stderr
    assert f'Report generated into s3: {output_filename}' in rb.stderr
