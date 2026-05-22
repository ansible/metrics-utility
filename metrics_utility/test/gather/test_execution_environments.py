from metrics_utility.test.gather.support.helpers import validate_csv_in_tarballs
from metrics_utility.test.util import run_gather_ext


uuid = '00000000-0000-0000-0000-000000000000'


def make_env(ship_path):
    return {
        'METRICS_UTILITY_SHIP_PATH': ship_path,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }


def make_paths(ship_path):
    return f'{ship_path}/data/2025/06/13/{uuid}-*.tar.gz'


execution_environments_lines = [
    'id,created,modified,description,image,managed,created_by_id,credential_id,modified_by_id,organization_id,name,pull',
    '1,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    'Python 3.11 environment with common ML libraries,'
    'registry.example.com/envs/python-ml:3.11,t,,,,,'
    'Python ML Environment,always',
    '2,2025-06-13 10:00:00+00,2025-06-13 10:00:00+00,'
    'Node.js 20 environment for backend services,'
    'registry.example.com/envs/node-backend:20,f,,,,,'
    'Node Backend Environment,missing',
]

execution_environments_skip_columns = [
    'id',
    'created_by_id',
    'credential_id',
    'modified_by_id',
    'organization_id',
]


def test_execution_environments_command(ship_path):
    """Build and validate execution_environments.csv contents in the generated tarball."""
    test_env = make_env(ship_path)
    test_env['METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'] = 'true'
    test_env['METRICS_UTILITY_OPTIONAL_COLLECTORS'] = 'execution_environments'

    run_gather_ext(test_env, ['--ship', '--since=2025-06-12', '--until=2025-06-14'])

    validate_csv_in_tarballs(make_paths(ship_path), 'execution_environments.csv', execution_environments_lines, execution_environments_skip_columns)
