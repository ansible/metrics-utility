import logging
import sys
import warnings


first = sys.argv[0]

if first.endswith('manage.py'):
    warnings.simplefilter(action='ignore', category=FutureWarning)

# FIXME: warning
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# This logger will log all message info and up
logger_info_level = logging.getLogger(__name__)
logger_info_level.setLevel(logging.INFO)


def debug():
    logger.setLevel(logging.DEBUG)
