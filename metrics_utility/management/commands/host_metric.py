import datetime
import sys

from django.core.exceptions import FieldDoesNotExist
from metrics_utility.base_command import BaseCommand
from awx.main.models.inventory import HostMetric

BATCH_LIMIT = 1000


class HostMetricExporter:
    def __init__(self, since=None, sql_filter=None, limit=BATCH_LIMIT):
        self.limit = limit
        self.since = since
        self.sql_filter = sql_filter or {}

    def to_tgz(self):
        pass

    def to_csv(self, target="stdout"):
        cols = self._get_existing_columns()
        self._write_line(cols, target)

        offset, new_offset = -1, 0

        while offset != new_offset:
            offset = new_offset

            host_metrics = HostMetric.objects.filter(**self.sql_filter)\
                                     .values(*cols).order_by('last_automation')[offset:offset + self.limit]

            for host_metric in list(host_metrics):
                new_offset += 1
                self._write_line(host_metric.values())

    @staticmethod
    def _columns():
        return 'hostname', 'first_automation', 'last_automation', 'automated_counter', \
            'deleted_counter', 'last_deleted', 'deleted'

    @staticmethod
    def _write_line(row, target="stdout"):
        if target == "stdout":
            sys.stdout.write(",".join([str(s) for s in row]))
            sys.stdout.write("\n")

    def _get_existing_columns(self):
        existing = []
        for col in self._columns():
            try:
                HostMetric._meta.get_field(col)
                existing.append(col)
            except FieldDoesNotExist:
                pass
        return existing


class Command(BaseCommand):
    help = 'This is for offline licensing usage'

    def add_arguments(self, parser):
        parser.add_argument('--since', type=datetime.datetime.fromisoformat, help='Start Date in ISO format YYYY-MM-DD')

    def handle(self, *args, **options):
        since = options.get('since')
        sql_filter = {}

        if since is not None:
            if since.tzinfo is None:
                since = since.replace(tzinfo=datetime.timezone.utc)
            sql_filter = {'last_automation__gte': since}

        HostMetricExporter(since=since, sql_filter=sql_filter).to_csv()

        return None
