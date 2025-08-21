import datetime
import os

from argparse import RawDescriptionHelpFormatter

from django.core.management.base import BaseCommand

from metrics_utility.automation_controller_billing.dataframe_engine.factory import Factory as DataframeFactory
from metrics_utility.automation_controller_billing.dedup.factory import Factory as DedupFactory
from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractorFactory
from metrics_utility.automation_controller_billing.report.factory import Factory as ReportFactory
from metrics_utility.automation_controller_billing.report_saver.factory import Factory as ReportSaverFactory
from metrics_utility.exceptions import BadRequiredEnvVar, BadShipTarget, MissingRequiredEnvVar
from metrics_utility.logger import debug, logger
from metrics_utility.management.validation import (
    date_format_text,
    handle_directory_ship_target,
    handle_env_validation,
    handle_month,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    parse_number_of_days,
    validate_build_params,
)


def get_report_path(ship_path, date):
    year = date.strftime('%Y')
    month = date.strftime('%m')

    return f'{ship_path}/reports/{year}/{month}'


def get_organization_filter():
    # handle None or empty string
    if not os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER'):
        return None
    return os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER').rstrip(';')


class Command(BaseCommand):
    """
    Build Report
    """

    help = 'Build Report'
    help_texts = {
        'since': (f'Start date for collection, including. {date_format_text.format(name="since")}'),
        'until': (f'End date for collection, including. {date_format_text.format(name="until")}'),
        'month': (
            'Month the report will be generated for, with format YYYY-MM. '
            "If this parameter is not provided, the previous month's report will be generated if it does not already exist."
        ),
        'ephemeral': (
            'Duration in months or days to determine if host is ephemeral. '
            'Months are considered as 30 days in duration. '
            'Example: --ephemeral=3months, or --ephemeral=3days'
        ),
        'force': ('With this option, the existing reports will be overwritten if running this command again.'),
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
                    "    METRICS_UTILITY_REPORT_TYPE (required): one of 'CCSPv2', 'CCSP', 'RENEWAL_GUIDANCE' - determines which kind of report we're generating",  # noqa: E501
                    "    METRICS_UTILITY_SHIP_TARGET (required): one of 'directory', 's3', 'controller_db' - input/output mechanism",
                    '    METRICS_UTILITY_SHIP_PATH (required): local or s3 directory path, input tarballs in path/data/, output xlsx in path/reports/',  # noqa: E501
                    '',
                    '  Optional Configuration:',
                    "    METRICS_UTILITY_DEDUPLICATOR (optional): one of 'ccsp', 'renewal', 'ccsp-experimental' - choice of deduplication algorithm",  # noqa: E501
                    '    METRICS_UTILITY_ORGANIZATION_FILTER (optional): CCSPv2 only, semicolon-separated list of org names to filter by',  # noqa: E501
                    '    METRICS_UTILITY_PRICE_PER_NODE (optional): price per node multiplier for cost calculations',
                    '    METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS (optional): enables optional report sheets, comma-separated list',  # noqa: E501
                    '    REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS (optional): max dedup iterations for renewal guidance (default: 3)',  # noqa: E501
                    '',
                    '  Report Customization (Optional):',
                    '    METRICS_UTILITY_REPORT_SKU (optional): SKU identifier for the report',
                    '    METRICS_UTILITY_REPORT_SKU_DESCRIPTION (optional): SKU description for the report',
                    '    METRICS_UTILITY_REPORT_H1_HEADING (optional): main heading for the report',
                    '    METRICS_UTILITY_REPORT_COMPANY_NAME (optional): company name for the report',
                    '    METRICS_UTILITY_REPORT_EMAIL (optional): contact email for the report',
                    '    METRICS_UTILITY_REPORT_RHN_LOGIN (optional): Red Hat Network login for the report',
                    '    METRICS_UTILITY_REPORT_PO_NUMBER (optional): purchase order number for the report',
                    '    METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER (optional): business leader name for the report',  # noqa: E501
                    '    METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER (optional): procurement leader name for the report',  # noqa: E501
                    '    METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME (optional): end user company name for the report',  # noqa: E501
                    '    METRICS_UTILITY_REPORT_END_USER_CITY (optional): end user company city for the report',
                    '    METRICS_UTILITY_REPORT_END_USER_STATE (optional): end user company state for the report',
                    '    METRICS_UTILITY_REPORT_END_USER_COUNTRY (optional): end user company country for the report',
                    '',
                    '  S3 Configuration:',
                    '    METRICS_UTILITY_BUCKET_NAME (optional): S3 bucket name',
                    '    METRICS_UTILITY_BUCKET_ENDPOINT (optional): S3 endpoint URL',
                    '    METRICS_UTILITY_BUCKET_ACCESS_KEY (optional): S3 access key',
                    '    METRICS_UTILITY_BUCKET_SECRET_KEY (optional): S3 secret key',
                    '    METRICS_UTILITY_BUCKET_REGION (optional): S3 region',
                    '',
                ]
            ),
            **kwargs,
        )

    def add_arguments(self, parser):
        parser.add_argument('--month', dest='month', action='store', help=self.help_texts.get('month'))
        parser.add_argument('--since', dest='since', action='store', help=self.help_texts.get('since'))
        parser.add_argument('--until', dest='until', action='store', help=self.help_texts.get('until'))
        parser.add_argument('--ephemeral', dest='ephemeral', action='store', help=self.help_texts.get('ephemeral'))
        parser.add_argument('--force', dest='force', action='store_true', help=self.help_texts.get('force'))
        parser.add_argument('--verbose', dest='verbose', action='store_true', help=self.help_texts.get('verbose'))

    def handle(self, *args, **options):
        if options.get('verbose'):
            debug()

        handle_env_validation('build')

        opt_since, opt_until = validate_build_params(options, self.help_texts)

        opt_month, month, next_month = handle_month(options.get('month') or None)
        opt_ephemeral = parse_number_of_days(options.get('ephemeral'))
        opt_force = options.get('force')

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET')

        # FIXME: separate params per factory
        extra_params = self._handle_extra_params(ship_target)
        extra_params['opt_since'] = opt_since
        extra_params['opt_until'] = opt_until
        extra_params['ephemeral_days'] = opt_ephemeral
        extra_params['month_since'] = month
        extra_params['month_until'] = next_month
        extra_params['deduplicator'] = os.getenv('METRICS_UTILITY_DEDUPLICATOR') or None

        # Determine destination path for generated report and skip processing if it exists
        report_type = extra_params['report_type']
        ship_path = extra_params['ship_path']
        if opt_since is not None:
            since_date = opt_since.date()
            until_date = opt_until.date() if opt_until else datetime.date.today()

            extra_params['since_date'] = since_date
            extra_params['until_date'] = until_date

            extra_params['report_period'] = f'{since_date}, {until_date}'
            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                get_report_path(ship_path, until_date),
                f'{report_type}-{since_date}--{until_date}.xlsx',
            )
        else:
            extra_params['report_period'] = opt_month
            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                get_report_path(ship_path, month),
                f'{report_type}-{opt_month}.xlsx',
            )

        report_saver_engine = ReportSaverFactory(ship_target, extra_params=extra_params).create()

        if report_saver_engine.report_exist() and not opt_force:
            # If the monthly report already exists, skip the generation
            logger.info(
                'Skipping report generation, report: '
                f'{report_saver_engine.report_spreadsheet_destination_path} already exists. '
                'Use --force option to override the report.'
            )
            return

        extractor = ExtractorFactory(ship_target, extra_params).create()

        # FIXME move from month to extra_params
        dataframes = DataframeFactory(extractor=extractor, month=month, extra_params=extra_params).create()

        dedup = DedupFactory(dataframes=dataframes, extra_params=extra_params).create()
        dataframes = dedup.run()

        if all(dataframe is None or dataframe.empty for _name, dataframe in dataframes.items()):
            if opt_since is not None:
                logger.info(f'No billing data for input date range {since_date}--{until_date}')
            else:
                logger.info(f'No billing data for month {opt_month}')
            return

        report_engine = ReportFactory(dataframes=dataframes, extra_params=extra_params).create()
        report_spreadsheet = report_engine.build_spreadsheet()

        # Save the report to the configured destination
        report_saver_engine.save(report_spreadsheet)
        logger.info(f'Report generated into {ship_target}: {report_saver_engine.report_spreadsheet_destination_path}')

    def _handle_ship_target(self, ship_target):
        if ship_target in ['controller_db', 'directory']:
            # controller_db is just directory but with different extractor
            handle_not_crc()
            handle_not_s3()
            return handle_directory_ship_target()
        elif ship_target == 's3':
            handle_not_crc()
            return handle_s3_ship_target()
        else:
            allowed = ', '.join(['controller_db', 'directory', 's3'])
            raise BadShipTarget(f'Unexpected value for METRICS_UTILITY_SHIP_TARGET env var ({ship_target}), allowed values: {allowed}')

    def _handle_extra_params(self, ship_target=None):
        base = self._handle_ship_target(ship_target)

        report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE')
        price_per_node = float(os.getenv('METRICS_UTILITY_PRICE_PER_NODE', 0))

        if not report_type:
            raise MissingRequiredEnvVar('Missing required env variable METRICS_UTILITY_REPORT_TYPE.')

        if report_type not in ['CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE']:
            raise BadRequiredEnvVar(
                "Bad value for required env variable METRICS_UTILITY_REPORT_TYPE, allowed values are: ['CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE']"
            )

        base.update(
            {
                'report_type': report_type,
                'price_per_node': price_per_node,
                # XLSX specific params
                'report_sku': os.getenv('METRICS_UTILITY_REPORT_SKU', ''),
                'report_sku_description': os.getenv('METRICS_UTILITY_REPORT_SKU_DESCRIPTION', ''),
                'report_h1_heading': os.getenv('METRICS_UTILITY_REPORT_H1_HEADING', ''),
                'report_company_name': os.getenv('METRICS_UTILITY_REPORT_COMPANY_NAME', ''),
                'report_email': os.getenv('METRICS_UTILITY_REPORT_EMAIL', ''),
                'report_rhn_login': os.getenv('METRICS_UTILITY_REPORT_RHN_LOGIN', ''),
                'report_po_number': os.getenv('METRICS_UTILITY_REPORT_PO_NUMBER', ''),
                'report_company_business_leader': os.getenv('METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER', ''),
                'report_company_procurement_leader': os.getenv('METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER', ''),
                'report_end_user_company_name': os.getenv('METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME', ''),
                'report_end_user_company_city': os.getenv('METRICS_UTILITY_REPORT_END_USER_CITY', ''),
                'report_end_user_company_state': os.getenv('METRICS_UTILITY_REPORT_END_USER_STATE', ''),
                'report_end_user_company_country': os.getenv('METRICS_UTILITY_REPORT_END_USER_COUNTRY', ''),
                # Renewal guidance specific params
                'report_renewal_guidance_dedup_iterations': os.getenv('REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS', '3'),
                'report_organization_filter': get_organization_filter(),
                # optional bits
                'optional_sheets': os.getenv(
                    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
                    'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
                )
                .rstrip(',')
                .split(','),
            }
        )
        return base
