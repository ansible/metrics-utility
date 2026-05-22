import importlib.util
import os
import sys
import warnings

from metrics_utility.management_utility import ManagementUtility


def prepare():
    """Tries to find awx modules. Either we're already in the venv, or it can be configured through AWX_PATH, or we fall back to the mock."""
    spec = importlib.util.find_spec('awx')
    if spec is None:
        awx_path = os.getenv('AWX_PATH', '/awx_devel')
        sys.path.append(awx_path)

        spec = importlib.util.find_spec('awx')
        if spec is None:
            warnings.warn(f'Automation Controller modules not found in {awx_path} (AWX_PATH). Using mock and continuing.')

            mock_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'mock_awx'))
            sys.path.append(mock_path)

    db_env_vars = [key for key in os.environ if key.startswith('METRICS_UTILITY_DB_')]
    if spec is not None and db_env_vars:
        sys.stderr.write(
            f'Warning: {", ".join(db_env_vars)} ignored because Automation Controller modules were found. '
            'These only take effect in standalone mode.\n'
        )

    import django

    from awx import prepare_env

    prepare_env()
    django.setup()


def manage():
    """Run a ManagementUtility."""
    prepare()

    utility = ManagementUtility(sys.argv)
    utility.execute()
