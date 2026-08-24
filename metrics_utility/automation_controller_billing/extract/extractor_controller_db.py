"""Extractor that reads host_metric data directly from the Controller database."""

import datetime

from django.db import connection

from metrics_utility.library.collectors.controller.main_hostmetric import main_hostmetric


class ExtractorControllerDB:
    """Extracts host_metric data from the AWX/Controller PostgreSQL database.

    Thin adapter over the :func:`~metrics_utility.library.collectors.controller.main_hostmetric.main_hostmetric`
    library collector, which owns the SQL, the custom PostgreSQL helper functions,
    and the keyset pagination.
    """

    def __init__(self, extra_params):
        """Initialise the DB extractor.

        Args:
            extra_params: Dict containing at least ``'opt_since'`` (datetime).
        """
        super().__init__()

        self.extra_params = extra_params

    def iter_batches(self):
        """Yield the host_metric batch from the Controller database.

        Delegates fetching (and keyset pagination) to the ``main_hostmetric``
        collector, which returns the full result set as a single DataFrame.

        Yields:
            Dict ``{'host_metric': pandas.DataFrame}`` when there is data.
        """
        since = self.extra_params['opt_since']
        if since.tzinfo is None:
            since = since.replace(tzinfo=datetime.UTC)

        host_metric = main_hostmetric(db=connection, since=since).gather()

        if host_metric is not None and not host_metric.empty:
            yield {'host_metric': host_metric}
