import logging
import os

import datetime
from metrics_utility.exceptions import BadShipTarget, MissingRequiredEnvVar, FailedToUploadPayload,\
    BadRequiredEnvVar, NoAnalyticsCollected
from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.management.validation import handle_directory_ship_target, handle_s3_ship_target, \
    handle_crc_ship_target

from dateutil import parser
from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    """
    Gather Automation Controller billing data
    """

    help = 'Gather Automation Controller billing data'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run',
                            dest='dry-run',
                            action='store_true',
                            help='Gather billing metrics without shipping.')
        parser.add_argument('--ship',
                            dest='ship',
                            action='store_true',
                            help='Enable shipping of billing metrics to the console.redhat.com')

        parser.add_argument('--since',
                            dest='since',
                            action='store',
                            help='Start date for collection including (e.g. --since=2023-12-20), or dynamic '\
                                 'format <X>d marking X days ago (e.g. collecting yesterday woul be '\
                                 '--since=2d --until=1d)')
        parser.add_argument('--until',
                            dest='until',
                            action='store',
                            help='End date for collection excluding (e.g. --since=2023-12-21), or dynamic '\
                                 'format <X>d marking X days ago (e.g. collecting yesterday woul be '\
                                 '--since=2d --until=1d)')

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def handle(self, *args, **options):
        try:
            self._handle(self, *args, **options)
            exit(0)
        except (BadShipTarget, MissingRequiredEnvVar, BadRequiredEnvVar, FailedToUploadPayload) as e:
            self.logger.error(e.name)
            exit(1)
        except Exception as e:
            self.logger.exception(e)
            exit(1)

    def _handle(self, *args, **options):
        self.init_logging()
        opt_ship = options.get('ship')
        opt_dry_run = options.get('dry-run')
        opt_since = options.get('since') or None
        opt_until = options.get('until') or None

        since, until = self._handle_interval(opt_since, opt_until)

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        billing_provider_params = self._handle_ship_target(ship_target)

        if opt_ship and opt_dry_run:
            self.logger.error('Arguments --ship and --dry-run cannot be processed at the same time, set only one of these.')
            return

        collector = Collector(collection_type=Collector.MANUAL_COLLECTION if opt_ship else Collector.DRY_RUN,
                              ship_target=ship_target, billing_provider_params=billing_provider_params)

        tgzfiles = collector.gather(since=since, until=until, billing_provider_params=billing_provider_params)
        if tgzfiles:
            for tgz in tgzfiles:
                self.logger.info(tgz)
        else:
            self.logger.error('No analytics collected')
            raise NoAnalyticsCollected('No analytics collected')

    def _handle_ship_target(self, ship_target):
        if ship_target == "crc":
            return handle_crc_ship_target(ship_target)
        elif ship_target == "directory":
            return handle_directory_ship_target(ship_target)
        elif ship_target == "s3":
            return handle_s3_ship_target(ship_target)
        else:
            raise BadShipTarget("Unexpected value for METRICS_UTILITY_SHIP_TARGET env var"\
                                ", allowed values are [crc, s3, directory]")

    def _handle_interval(self, opt_since, opt_until):
        # Process since argument
        since = None
        if opt_since and opt_since.endswith('d'):
            days_ago = int(opt_since[0:-1])
            since = (datetime.datetime.now() - datetime.timedelta(days=days_ago-1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif opt_since and opt_since.endswith('m'):
            minutes_ago = int(opt_since[0:-1])
            since = (datetime.datetime.now() - datetime.timedelta(minutes=minutes_ago))
        else:
            since = parser.parse(opt_since) if opt_since else None
        # Add default utc timezone
        if since and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        # Process until argument
        until = None
        if opt_until and opt_until.endswith('d'):
            days_ago = int(opt_until[0:-1])
            until = (datetime.datetime.now() - datetime.timedelta(days=days_ago-1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif opt_until and opt_until.endswith('m'):
            minutes_ago = int(opt_until[0:-1])
            until = (datetime.datetime.now() - datetime.timedelta(minutes=minutes_ago))
        else:
            until = parser.parse(opt_until) if opt_until else None
        # Add default utc timezone
        if until and until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)

        return since, until
