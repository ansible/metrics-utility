import os
import subprocess
import sys

import pytest


env_vars = {
    'METRICS_UTILITY_BUCKET_ACCESS_KEY': 'myuseraccesskey',
    'METRICS_UTILITY_BUCKET_ENDPOINT': os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', 'http://localhost:9000'),  # or http://minio:9000
    'METRICS_UTILITY_BUCKET_NAME': 'metricsutilitys3',
    'METRICS_UTILITY_BUCKET_REGION': 'us-east-1',
    'METRICS_UTILITY_BUCKET_SECRET_KEY': 'myusersecretkey',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': 'metrics-utility/shipped_data',
    'METRICS_UTILITY_SHIP_TARGET': 's3',
    'AWX_LOGGING_MODE': 'stdout',
}


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command():
    """Build xlsx report using build command and test its contents."""

    python_executable = sys.executable
    result = subprocess.run(
        [python_executable, 'manage.py', 'gather_automation_controller_billing_data', '--ship', '--until=10m', '--force'],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env_vars,
    )

    assert result.returncode == 0

    # mc ls -r local/metricsutilitys3/metrics-utility/shipped_data/data/
