import csv
import datetime
import sys
import tempfile

from django.conf import settings
from django.db.models import Value, ExpressionWrapper, CharField
from django.db.models.fields import DateField
from django.db.models.functions import Trunc
from summary_utility.base_command import BaseCommand
from awx.main.models.jobs import JobHostSummary


BATCHED_FETCH_COUNT = 10000


class HostSummaryExporter:
    def __init__(self, since=None, sql_filter=None, limit=BATCHED_FETCH_COUNT):
        self.limit = limit
        self.since = since
        self.sql_filter = sql_filter or {}

    def to_tgz(self):
        pass

    def output_csv(self, options, filter_kwargs):
        with tempfile.TemporaryDirectory() as temp_dir:
            for csv_detail in self.csv_for_tar(temp_dir, options.get('csv', 'host_metric'), filter_kwargs, BATCHED_FETCH_COUNT, False):
                csv_file = csv_detail[0]
                with open(csv_file) as f:
                    sys.stdout.write(f.read())

    def to_csv(self, target="stdout"):
        install_uuid = settings.INSTALL_UUID
        cols = self._columns()
        self._write_line(cols, target)

        offset, new_offset = -1, 0

        while offset != new_offset:
            offset = new_offset

            host_summary = JobHostSummary.objects\
                .annotate(cluster_uuid=ExpressionWrapper(Value(install_uuid), output_field=CharField(max_length=45)))\
                .annotate(created_date=Trunc('created', 'day', output_field=DateField()))\
                .select_related('job__organization')\
                .values(*cols)\
                .distinct(*cols)\
                .order_by(*cols)[offset:offset + self.limit]

            # host_summary =    JobHostSummary.objects.filter(**self.sql_filter)\
            #                              .values(*cols).order_by('last_automation')[offset:offset + self.limit]

            for host_metric in list(host_summary):
                new_offset += 1
                self._write_line(host_metric.values())

    @staticmethod
    def _columns():
        return ("cluster_uuid", "created_date", "job__organization__name", "host_name")

    @staticmethod
    def _write_line(row, target="stdout"):
        if target == "stdout":
            sys.stdout.write(",".join([str(s) for s in row]))
            sys.stdout.write("\n")

    def _get_existing_columns(self):
        return ("created_date", "job__organization__name", "host_name")


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

        HostSummaryExporter(since=since, sql_filter=sql_filter).to_csv()

        return None
