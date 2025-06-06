#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""

import sys

from metrics_utility.exceptions import MissingRequiredEnvVar


if __name__ == '__main__':
    from metrics_utility import manage
    try:
        manage()
    except MissingRequiredEnvVar as exc:
        print(f"\nEnvironment Variable Validation Error: \n{exc}\n")
        sys.exit(1)
