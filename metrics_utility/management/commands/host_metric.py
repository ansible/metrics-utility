import datetime
import sys

from django.db import connection
from metrics_utility.base_command import BaseCommand

CSV_PREFERRED_ROW_COUNT = 500000
BATCH_LIMIT = 10000


class HostMetricExporter:
    def __init__(self, since=None, sql_filter=None, limit=BATCH_LIMIT,
                 rows_per_file=CSV_PREFERRED_ROW_COUNT):
        self.limit = limit
        self.rows_per_file = rows_per_file
        self.since = since
        self.sql_filter = sql_filter or {}

    def to_tgz(self):
        pass

    def to_csv(self, target="stdout"):
        with connection.cursor() as cursor:
            offset, new_offset = -1, 0
            write_headers = True

            while offset != new_offset:
                offset = new_offset
                if write_headers:
                    self._write_line(self._columns(), target)
                    write_headers = False

                sql, sql_filter = self._query(self.limit, offset)
                cursor.execute(sql, sql_filter)

                for row in cursor.fetchall():
                    new_offset += 1
                    self._write_line(row, target)

    @staticmethod
    def _write_line(row, target="stdout"):
        if target == "stdout":
            sys.stdout.write(",".join([str(s) for s in row]))
            sys.stdout.write("\n")

    def _query(self, limit=BATCH_LIMIT, offset=0):
        sql_args = []

        sql = f"SELECT {', '.join(self._columns())} FROM main_hostmetric"
        if self.sql_filter.get('last_automation', None):
            sql += " WHERE last_automation >= %s"
            sql_args.append(self.sql_filter['last_automation'])
        sql += " ORDER BY last_automation"
        sql += " LIMIT %s OFFSET %s;"

        # sql_args.append(self.order_by)
        sql_args.append(limit)
        sql_args.append(offset)

        return sql, sql_args

    @staticmethod
    def _columns():
        return 'hostname', 'first_automation', 'last_automation'


class Command(BaseCommand):
    help = 'This is for offline licensing usage'

    def add_arguments(self, parser):
        parser.add_argument('--since', type=datetime.datetime.fromisoformat, help='Start Date in ISO format YYYY-MM-DD')
        # parser.add_argument('--rows_per_file', type=int, help=f'Split rows in chunks of {CSV_PREFERRED_ROW_COUNT}')

    def handle(self, *args, **options):
        since = options.get('since')
        sql_filter = {}

        if since is not None:
            if since.tzinfo is None:
                since = since.replace(tzinfo=datetime.timezone.utc)
            sql_filter = {'since': since}

        HostMetricExporter(since=since, sql_filter=sql_filter).to_csv()

        return None
