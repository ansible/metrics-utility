import logging
import os

from django.core.management.base import BaseCommand

from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.exceptions import (
    BadShipTarget,
    NoAnalyticsCollected,
)
from metrics_utility.management.validation import (
    date_format_text,
    handle_crc_ship_target,
    handle_directory_ship_target,
    handle_env_validation,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    parse_date_param,
)


class Command(BaseCommand):
    """
    Gather Automation Controller billing data
    """

    help = 'Gather Automation Controller billing data'
    help_texts = {
        'since': (f'Start date for collection, including. {date_format_text.format(name="since")}'),
        'until': (f'End date for collection, including. {date_format_text.format(name="until")}'),
        'dry-run': ('Gather billing metrics without shipping.'),
        'ship': ('Enable shipping of billing metrics to the console.redhat.com'),
    }

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', dest='dry-run', action='store_true', help=self.help_texts.get('dry-run'))
        parser.add_argument('--ship', dest='ship', action='store_true', help=self.help_texts.get('ship'))
        parser.add_argument('--since', dest='since', action='store', help=self.help_texts.get('since'))
        parser.add_argument('--until', dest='until', action='store', help=self.help_texts.get('until'))

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

        opt_since = options.get('since')
        opt_until = options.get('until')
        opt_ship = options.get('ship')
        opt_dry_run = options.get('dry-run')

        since = parse_date_param(opt_since, self.help_texts, 'since')
        until = parse_date_param(opt_until, self.help_texts, 'until')

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
        if not tgzfiles:
            self.logger.error('No analytics collected')
            raise NoAnalyticsCollected('No analytics collected')
        if tgzfiles:
            self.logger.info('Analytics collected')

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
