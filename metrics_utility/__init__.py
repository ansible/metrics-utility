import os
import sys
import importlib.util

from metrics_utility.management_utility import ManagementUtility


def manage():
    """Run a ManagementUtility."""

    # Tries to find awx modules. They are either in venv, or can be configured through AWX_PATH ENV
    spec = importlib.util.find_spec('awx')
    if spec is None:
        awx_path = os.getenv('AWX_PATH', '/awx_devel')
        sys.path.append(awx_path)
        spec = importlib.util.find_spec('awx')
        if spec is None:
            sys.stderr.write(f"Automation Controller modules not found in {awx_path}\n")
            exit(-1)

    from awx import prepare_env
    import django

    prepare_env()
    django.setup()

    utility = ManagementUtility(sys.argv)
    utility.execute()
