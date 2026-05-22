import logging
import os

from argparse import RawDescriptionHelpFormatter

from django.core.management.base import BaseCommand

from metrics_utility.exceptions import (
    MissingRequiredEnvVar,
    NoAnalyticsCollected,
)
from metrics_utility.gather.collector import Collector
from metrics_utility.logger import logger
from metrics_utility.management.validation import (
    date_format_text,
    parse_date_param,
)


VALID_COLLECTORS = {
    ## shared
    # config, manifest, data_collection_status are always on
    ## ccsp
    # job_host_summary is on by default, disable via METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR
    # main_jobevent is on by default when METRICS_UTILITY_OPTIONAL_COLLECTORS is not set
    'main_host',
    'main_host_daily',
    'main_indirectmanagednodeaudit',
    'main_jobevent',
    ## vcpu
    'total_workers_vcpu',
    ## anonymized
    'controller_version_service',
    'credentials_service',
    'execution_environments',
    'feature_flags_service',
    'job_host_summary_service',
    'main_jobevent_service',
    'table_metadata',
    'unified_jobs',
    ## dashboard & service
    'dashboard_jobs',
    'task_executions_service',
}

VALID_SHIP_TARGETS = {'directory', 's3', 'crc'}

MAX_GATHER_PERIOD_DAYS = 3650  # 10 years maximum

S3_ENV_VARS = [
    'METRICS_UTILITY_BUCKET_ACCESS_KEY',
    'METRICS_UTILITY_BUCKET_ENDPOINT',
    'METRICS_UTILITY_BUCKET_NAME',
    'METRICS_UTILITY_BUCKET_REGION',
    'METRICS_UTILITY_BUCKET_SECRET_KEY',
]

CRC_ENV_VARS = [
    'METRICS_UTILITY_BILLING_ACCOUNT_ID',
    'METRICS_UTILITY_BILLING_PROVIDER',
    'METRICS_UTILITY_RED_HAT_ORG_ID',
]


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
            logger.setLevel(logging.DEBUG)

        since = parse_date_param(options.get('since'), self.help_texts, 'since')
        until = parse_date_param(options.get('until'), self.help_texts, 'until')

        if options.get('ship') and options.get('dry-run'):
            logger.error('Arguments --ship and --dry-run cannot be processed at the same time, set only one of these.')
            return

        ship_target, billing_provider_params, ship_params = self._read_env()

        collector = Collector(
            collection_type=Collector.MANUAL_COLLECTION if options.get('ship') else Collector.DRY_RUN,
            ship_target=ship_target,
        )

        tgzfiles = collector.gather(since=since, until=until, billing_provider_params=billing_provider_params, ship_params=ship_params)
        if not tgzfiles:
            logger.error('No analytics collected')
            raise NoAnalyticsCollected('No analytics collected')
        logger.info('Analytics collected')

    def _read_env(self):
        """Validate environment and return (ship_target, billing_provider_params, ship_params)."""
        errors = []

        # Validate optional collectors
        collectors = os.getenv('METRICS_UTILITY_OPTIONAL_COLLECTORS', 'main_jobevent').strip(', \t')
        if collectors:
            invalid = set(collectors.split(',')) - VALID_COLLECTORS
            if invalid:
                errors.append(f'Invalid METRICS_UTILITY_OPTIONAL_COLLECTORS: {", ".join(invalid)}. Valid values: {", ".join(VALID_COLLECTORS)}')

        # Validate max gather period days
        max_days_str = os.getenv('METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS')
        if max_days_str is not None:
            max_days_err = f'Value must be number between 0 to {MAX_GATHER_PERIOD_DAYS}'
            try:
                max_days = int(max_days_str)
                if max_days < 0 or max_days > MAX_GATHER_PERIOD_DAYS:
                    errors.append(f'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: {max_days}. {max_days_err}')
            except (ValueError, TypeError):
                errors.append(f'Invalid METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS: "{max_days_str}". {max_days_err}')

        # Validate ship target
        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET')
        if not ship_target:
            errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET is empty. Valid values: {", ".join(VALID_SHIP_TARGETS)}')
        elif ship_target not in VALID_SHIP_TARGETS:
            errors.append(f'Invalid METRICS_UTILITY_SHIP_TARGET: {ship_target}. Valid values: {", ".join(VALID_SHIP_TARGETS)}')

        if errors:
            raise MissingRequiredEnvVar('\n'.join(errors))

        # Read ship-target-specific configuration and warn about surplus env vars
        if ship_target == 'crc':
            billing_provider_params, ship_params = self._read_crc_env()
            self._warn_surplus(S3_ENV_VARS, 's3')
        elif ship_target == 'directory':
            billing_provider_params, ship_params = self._read_directory_env()
            self._warn_surplus(CRC_ENV_VARS, 'crc')
            self._warn_surplus(S3_ENV_VARS, 's3')
        elif ship_target == 's3':
            billing_provider_params, ship_params = self._read_s3_env()
            self._warn_surplus(CRC_ENV_VARS, 'crc')

        return ship_target, billing_provider_params, ship_params

    @staticmethod
    def _read_directory_env():
        ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
        if not ship_path:
            raise MissingRequiredEnvVar('Missing required env variable METRICS_UTILITY_SHIP_PATH - place for collected data')
        return {}, {'ship_path': ship_path}

    @staticmethod
    def _read_s3_env():
        ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
        bucket_name = os.getenv('METRICS_UTILITY_BUCKET_NAME')
        bucket_endpoint = os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT')
        bucket_region = os.getenv('METRICS_UTILITY_BUCKET_REGION')
        bucket_access_key = os.getenv('METRICS_UTILITY_BUCKET_ACCESS_KEY')
        bucket_secret_key = os.getenv('METRICS_UTILITY_BUCKET_SECRET_KEY')

        missing = []
        if not bucket_name:
            missing += ['METRICS_UTILITY_BUCKET_NAME - name of S3 bucket']
        if not bucket_endpoint:
            missing += ['METRICS_UTILITY_BUCKET_ENDPOINT - S3 endpoint, eg. https://s3.us-east.example.com']
        if not bucket_access_key:
            missing += ['METRICS_UTILITY_BUCKET_ACCESS_KEY - S3 access key']
        if not bucket_secret_key:
            missing += ['METRICS_UTILITY_BUCKET_SECRET_KEY - S3 secret key']
        if not ship_path:
            missing += ['METRICS_UTILITY_SHIP_PATH - place for collected data']

        if missing:
            raise MissingRequiredEnvVar(f'Missing some required env variables for S3 configuration, namely: {", ".join(missing)}.')

        return {}, {
            'ship_path': ship_path,
            'bucket_name': bucket_name,
            'bucket_endpoint': bucket_endpoint,
            'bucket_region': bucket_region,
            'bucket_access_key': bucket_access_key,
            'bucket_secret_key': bucket_secret_key,
        }

    @staticmethod
    def _read_crc_env():
        billing_provider = os.getenv('METRICS_UTILITY_BILLING_PROVIDER')

        billing_provider_params = {'billing_provider': billing_provider}
        if billing_provider == 'aws':
            billing_account_id = os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID')
            if not billing_account_id:
                raise MissingRequiredEnvVar('METRICS_UTILITY_BILLING_ACCOUNT_ID, containing AWS 12 digit customer id needs to be provided.')
            billing_provider_params['billing_account_id'] = billing_account_id
        else:
            raise MissingRequiredEnvVar('Uknown METRICS_UTILITY_BILLING_PROVIDER env var, supported values are [aws].')

        red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID')
        if red_hat_org_id:
            billing_provider_params['red_hat_org_id'] = red_hat_org_id

        ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH')
        if ship_path:
            logger.warning('Ignoring METRICS_UTILITY_SHIP_PATH used without METRICS_UTILITY_SHIP_TARGET="directory", "s3"')

        return billing_provider_params, {}

    @staticmethod
    def _warn_surplus(var_names, expected_target):
        surplus = [v for v in var_names if os.getenv(v)]
        if surplus:
            logger.warning(f'Ignoring env variables used without METRICS_UTILITY_SHIP_TARGET="{expected_target}": {", ".join(surplus)}')
