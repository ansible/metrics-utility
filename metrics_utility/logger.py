import logging


# FIXME: warning
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# This logger will log all message info and up
logger_info_level = logging.getLogger(__name__)
logger_info_level.setLevel(logging.INFO)


def debug():
    logger.setLevel(logging.DEBUG)
