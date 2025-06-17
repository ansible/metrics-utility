import glob
import os
import tarfile

import pytest

from metrics_utility.test.util import run_gather_ext


env_vars = {
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
}

file_name = '00000000-0000-0000-0000-000000000000-2025-06-13-000000+0000-2025-06-14-000000+0000-6.tar.gz'
file_path = './metrics_utility/test/test_data/data/2025/06/13/' + file_name

year = 2025
uuid = '00000000-0000-0000-0000-000000000000'  # mock_awx INSTALL_UUID setting

file_glob = f'./metrics_utility/test/test_data/data/{year}/*/*/{uuid}-*.tar.gz'

@pytest.fixture
def cleanup_glob():
    yield
    for file in glob.glob(file_glob):
        print(file)
        os.remove(file)


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_command(cleanup_glob):
    """Build xlsx report using build command and test its contents."""

    run_gather_ext(env_vars, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    with tarfile.open(file_path, "r:gz") as tar:
        # List all files
        for member in tar.getmembers():
            if member.name.endswith("job_host_summary.csv"):
                # Extract file object
                f = tar.extractfile(member)
                if f:
                    content = f.read().decode("utf-8")
                    print(content)
                break
        else:
            print("job_host_summary.csv not found.")


