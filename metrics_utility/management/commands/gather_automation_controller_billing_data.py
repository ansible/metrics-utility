import os

from argparse import RawDescriptionHelpFormatter

from django.core.management.base import BaseCommand

from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.exceptions import (
    BadShipTarget,
    NoAnalyticsCollected,
)
from metrics_utility.logger import debug, logger
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
        'until': (f'End date for collection, excluding. {date_format_text.format(name="until")}'),
        'dry-run': ('Gather billing metrics without shipping.'),
        'ship': ('Enable shipping of billing metrics to the console.redhat.com'),
        'verbose': ('Print debug information to console.'),
    }

    def create_parser(self, prog_name, subcommand, **kwargs):
        return super().create_parser(
            prog_name,
            subcommand,
            # ensure newlines are preserved in descriptions and epilog
            formatter_class=RawDescriptionHelpFormatter,
            epilog='\n'.join(
                [
                    'ENVIRONMENT',
                    '',
                    '  Core Configuration:',
                    "    METRICS_UTILITY_SHIP_TARGET (required): one of 'crc', 'directory', 's3' - input/output mechanism",
                    '    METRICS_UTILITY_SHIP_PATH (required): directory path for data collection and storage',
                    '',
                    '  Collection Configuration:',
                    '    METRICS_UTILITY_CLUSTER_NAME (optional): cluster name for total_workers_vcpu collector (required when enabled)',  # noqa: E501
                    '    METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX (optional): custom lock name for total_workers_vcpu collector',
                    '    METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR (optional): disable job_host_summary collector',  # noqa: E501
                    '    METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES (optional): skip updating last gather info from controller settings',  # noqa: E501
                    '    METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS (optional): maximum length of collection interval in days (default: 28)',  # noqa: E501
                    '    METRICS_UTILITY_OPTIONAL_COLLECTORS (optional): optional collectors, comma-separated list',
                    '    METRICS_UTILITY_USAGE_BASED_METERING_ENABLED (optional): total_workers_vcpu collector toggle (default: false)',  # noqa: E501
                    '',
                    '  Billing Provider Configuration:',
                    '    METRICS_UTILITY_BILLING_ACCOUNT_ID (optional): AWS account ID for billing',
                    '    METRICS_UTILITY_BILLING_PROVIDER (optional): billing provider type',
                    '    METRICS_UTILITY_RED_HAT_ORG_ID (optional): Red Hat organization ID',
                    '',
                    '  S3 Configuration:',
                    '    METRICS_UTILITY_BUCKET_NAME (optional): S3 bucket name',
                    '    METRICS_UTILITY_BUCKET_ENDPOINT (optional): S3 endpoint URL',
                    '    METRICS_UTILITY_BUCKET_ACCESS_KEY (optional): S3 access key',
                    '    METRICS_UTILITY_BUCKET_SECRET_KEY (optional): S3 secret key',
                    '    METRICS_UTILITY_BUCKET_REGION (optional): S3 region',
                    '',
                    '  CRC Configuration:',
                    '    METRICS_UTILITY_CRC_INGRESS_URL (optional): CRC upload URL',
                    '    METRICS_UTILITY_CRC_SSO_URL (optional): CRC login URL',
                    '    METRICS_UTILITY_PROXY_URL (optional): upload proxy URL',
                    '    METRICS_UTILITY_SERVICE_ACCOUNT_ID (optional): service account ID',
                    '    METRICS_UTILITY_SERVICE_ACCOUNT_SECRET (optional): service account secret',
                ]
            ),
            **kwargs,
        )

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', dest='dry-run', action='store_true', help=self.help_texts.get('dry-run'))
        parser.add_argument('--ship', dest='ship', action='store_true', help=self.help_texts.get('ship'))
        parser.add_argument('--since', dest='since', action='store', help=self.help_texts.get('since'))
        parser.add_argument('--until', dest='until', action='store', help=self.help_texts.get('until'))
        parser.add_argument('--verbose', dest='verbose', action='store_true', help=self.help_texts.get('verbose'))

    def handle(self, *args, **options):
        if options.get('verbose'):
            debug()
        handle_env_validation('gather')

        opt_since = options.get('since')
        opt_until = options.get('until')
        opt_ship = options.get('ship')
        opt_dry_run = options.get('dry-run')

        since = parse_date_param(opt_since, self.help_texts, 'since')
        until = parse_date_param(opt_until, self.help_texts, 'until')

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET')
        extra_params = self._handle_ship_target(ship_target)

        if opt_ship and opt_dry_run:
            logger.error('Arguments --ship and --dry-run cannot be processed at the same time, set only one of these.')
            return

        collector = Collector(
            collection_type=Collector.MANUAL_COLLECTION if opt_ship else Collector.DRY_RUN,
            ship_target=ship_target,
            billing_provider_params=extra_params,
        )

        tgzfiles = collector.gather(since=since, until=until, billing_provider_params=extra_params)
        if not tgzfiles:
            logger.error('No analytics collected')
            raise NoAnalyticsCollected('No analytics collected')
        if tgzfiles:
            logger.info('Analytics collected')

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
