import logging
import os

from django.core.management.base import BaseCommand

from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.automation_controller_billing.helpers import parse_date_param
from metrics_utility.exceptions import (
    BadShipTarget,
    NoAnalyticsCollected,
)
from metrics_utility.management.validation import (
    handle_crc_ship_target,
    handle_directory_ship_target,
    handle_env_validation,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    handle_validate_date_param,
)


class HelpText:
    since = (
        'Start date for collection (including) as an absolute date (in YYYY-MM-DD format, e.g. --since=2024-12-31) or a relative offset '
        '(e.g. --since=5d for five days ago, --since=5mo for five months ago, --since=5m for five minutes ago).'
    )
    until = (
        'End date for collection (including) as an absolute date (in YYYY-MM-DD format, e.g. --until=2024-12-31) or a relative offset '
        '(e.g. --until=5d for five days ago, --until=5mo for five months ago, --until=5m for five minutes ago). '
    )
    dry_run = 'Gather billing metrics without shipping.'
    ship = 'Enable shipping of billing metrics to console.redhat.com.'


class Command(BaseCommand):
    """
    Gather Automation Controller billing data
    """

    help = 'Gather Automation Controller billing data'

    def add_arguments(self, parser):
        parser.add_argument('--since', dest='since', action='store', help=HelpText.since)
        parser.add_argument('--until', dest='until', action='store', help=HelpText.until)

        # dry-run and ship are mutually exclusive
        exclusive = parser.add_mutually_exclusive_group(required=False)
        exclusive.add_argument('--dry-run', dest='dry-run', action='store_true', help=HelpText.dry_run)
        exclusive.add_argument('--ship', dest='ship', action='store_true', help=HelpText.ship)

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def handle(self, *args, **options):
        self.init_logging()

        handle_env_validation('gather')

        opt_since = options.get('since') or None
        opt_until = options.get('until') or None
        handle_validate_date_param(opt_since, HelpText.since, 'gather')
        handle_validate_date_param(opt_until, HelpText.until, 'gather')

        opt_ship = options.get('ship')
        opt_dry_run = options.get('dry-run')

        since = parse_date_param(opt_since, help=HelpText.since)
        until = parse_date_param(opt_until, help=HelpText.until)

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        billing_provider_params = self._handle_ship_target(ship_target)

        if opt_ship and opt_dry_run:
            self.logger.error('Arguments --ship and --dry-run cannot be processed at the same time, set only one of these.')
            return

        collector = Collector(
            collection_type=Collector.MANUAL_COLLECTION if opt_ship else Collector.DRY_RUN,
            ship_target=ship_target,
            billing_provider_params=billing_provider_params,
        )

        tgzfiles = collector.gather(since=since, until=until, billing_provider_params=billing_provider_params)
        if tgzfiles:
            for tgz in tgzfiles:
                self.logger.info(tgz)
        else:
            self.logger.error('No analytics collected')
            raise NoAnalyticsCollected('No analytics collected')

    def _handle_ship_target(self, ship_target):
        if ship_target == 'crc':
            handle_not_s3()
            return handle_crc_ship_target()
        elif ship_target == 'directory':
            handle_not_crc()
            handle_not_s3()
            return handle_directory_ship_target()
        elif ship_target == 's3':
            handle_not_crc()
            return handle_s3_ship_target()
        else:
            allowed = ', '.join(['crc', 'directory', 's3'])
            raise BadShipTarget(f'Unexpected value for METRICS_UTILITY_SHIP_TARGET env var ({ship_target}), allowed values: {allowed}')
