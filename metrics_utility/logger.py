"""Configure and expose the metrics-utility logger and debug helper."""

import logging


# note: total_workers_vcpu requires at least INFO
logging.basicConfig(format='%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
