"""Configure and expose the metrics-utility logger and debug helper."""

import logging
import sys
import warnings


if sys.argv[0].endswith('manage.py'):
    warnings.simplefilter(action='ignore', category=FutureWarning)

# note: total_workers_vcpu requires at least INFO
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def debug():
    """Set the logger level to DEBUG, enabling verbose output."""
    logger.setLevel(logging.DEBUG)
