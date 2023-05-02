import os
import sys

from metrics_utility.management_utility import ManagementUtility

os.environ.setdefault('AWX_DIR', '/awx_devel')

sys.path.append("/awx_devel")
from awx import prepare_env
import django


def manage():
    """Run a ManagementUtility."""
    prepare_env()
    django.setup()

    utility = ManagementUtility(sys.argv)
    utility.execute()
