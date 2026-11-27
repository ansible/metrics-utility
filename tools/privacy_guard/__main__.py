"""Module entry point for ``python -m tools.privacy_guard``."""

import sys

from tools.privacy_guard.cli import main


if __name__ == '__main__':
    sys.exit(main())
