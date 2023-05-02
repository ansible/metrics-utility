#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
# import os
# import sys

# from metrics_utility.management_utility import ManagementUtility

# os.environ.setdefault('AWX_DIR', '/awx_devel')
#
# sys.path.append("/awx_devel")
# from awx import prepare_env, __version__
#
#
# prepare_env()


# def execute_from_command_line(argv=None):
#     """Run a ManagementUtility."""
#     utility = ManagementUtility(argv)
#     utility.execute()


if __name__ == '__main__':
    from metrics_utility import manage
    manage()
