import logging
import sys


VERBOSE = '--verbose' in sys.argv


class NoWarningsFilter(logging.Filter):
    def filter(self, record):
        if VERBOSE:  # allow everything in verbose mode
            return True
        return record.levelno != logging.WARNING


# FIXME: warning
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# This logger will log all message info and up
logger_info_level = logging.getLogger(__name__)
logger_info_level.setLevel(logging.INFO)
logger.addFilter(NoWarningsFilter())


def debug():
    logger.setLevel(logging.DEBUG)
