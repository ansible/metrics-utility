import datetime
import logging

import pandas as pd

from awx.main.models.inventory import HostMetric


class ExtractorControllerDB():
    LOG_PREFIX = "[ExtractorDirectory]"

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        super().__init__()

        self.extension = "parquet"
        self.path = extra_params["ship_path"]
        self.extra_params = extra_params

        self.logger = logger

    def get_report_path(self, date):
        path_prefix = f"{self.path}/reports"

        year = date.strftime("%Y")
        month = date.strftime("%m")

        path = f"{path_prefix}/{year}/{month}"

        return path

    def iter_batches(self):
        cols = self._columns()

        since = self.extra_params['opt_since']
        if since.tzinfo is None:
            since = since.replace(tzinfo=datetime.timezone.utc)

        sql_filter = {'last_automation__gte': since}

        while True:
            host_metric = HostMetric.objects.filter(**sql_filter)\
                                    .values(*cols).order_by('hostname')[:self.limit()]

            # Marker based pagination
            if len(host_metric) <= 0:
                break

            marker = list(host_metric)[-1]["hostname"]
            sql_filter['hostname__gt'] = marker

            host_metric = pd.DataFrame(host_metric)

            yield {'host_metric': host_metric}

    @staticmethod
    def _columns():
        return 'hostname', 'first_automation', 'last_automation', 'automated_counter', \
            'deleted_counter', 'last_deleted', 'deleted'

    @staticmethod
    def limit():
        return 1 # 10000