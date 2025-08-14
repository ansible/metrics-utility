import importlib.util
import os
import sys

from metrics_utility.management_utility import ManagementUtility


def prepare():
    """Tries to find awx modules. Either we're already in the venv, or it can be configured through AWX_PATH, or we fall back to the mock."""
    spec = importlib.util.find_spec('awx')
    if spec is None:
        awx_path = os.getenv('AWX_PATH', '/awx_devel')
        sys.path.append(awx_path)

        spec = importlib.util.find_spec('awx')
        if spec is None:
            sys.stderr.write(f'Automation Controller modules not found in {awx_path} (AWX_PATH). Using mock and continuing.\n')

            mock_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'mock_awx'))
            sys.path.append(mock_path)

    import django

    from awx import prepare_env

    prepare_env()
    django.setup()


def manage():
    """Run a ManagementUtility."""
    prepare()

    utility = ManagementUtility(sys.argv)
    utility.execute()
