import logging


# FIXME: warning
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def debug():
    logger.setLevel(logging.DEBUG)
