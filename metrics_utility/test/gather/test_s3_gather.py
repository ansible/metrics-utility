import os

import pytest

from metrics_utility.test.util import run_gather_ext, run_gather_int


env_vars = {
    'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'myuseraccesskey',
    'METRICS_UTILITY_BUCKET_ENDPOINT': os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', 'http://localhost:9000'),  # or http://minio:9000
    'METRICS_UTILITY_BUCKET_NAME': 'metricsutilitys3',
    'METRICS_UTILITY_BUCKET_REGION': 'us-east-1',
    'METRICS_UTILITY_BUCKET_SECRET_KEY': 'myusersecretkey',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': 'metrics-utility/shipped_data',
    'METRICS_UTILITY_SHIP_TARGET': 's3',
}


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command():
    """Build xlsx report using build command and test its contents."""
    run_gather_ext(env_vars, ['--ship', '--until=10m'])
    # mc ls -r local/metricsutilitys3/metrics-utility/shipped_data/data/


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
