import logging
import sys
import warnings


if sys.argv[0].endswith('manage.py'):
    warnings.simplefilter(action='ignore', category=FutureWarning)

logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def logger_debug():
    logger.setLevel(logging.DEBUG)
