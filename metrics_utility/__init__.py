import importlib.util
import logging
import os
import sys

from metrics_utility.management_utility import ManagementUtility


def prepare():
    # Tries to find awx modules. They are either in venv, or can be configured through AWX_PATH ENV
    spec = importlib.util.find_spec('awx')
    if spec is None:
        awx_path = os.getenv('AWX_PATH', '/awx_devel')
        sys.path.append(awx_path)
        spec = importlib.util.find_spec('awx')
        if spec is None:
            sys.stderr.write(f'Automation Controller modules not found in {awx_path} (AWX_PATH). Using mock and continuing.\n')
            sys.path.append(os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'mock_awx')))

            # debug logging by default in standalone mode
            logger = logging.getLogger('awx.main.analytics')
            logger.setLevel(logging.DEBUG)  # Ensure the logger captures all messages

    import django

    from awx import prepare_env

    prepare_env()
    django.setup()


def manage():
    """Run a ManagementUtility."""
    prepare()

    utility = ManagementUtility(sys.argv)
    utility.execute()
