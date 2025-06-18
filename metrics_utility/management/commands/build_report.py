import datetime
import logging
import os

from datetime import timezone

from django.core.management.base import BaseCommand

from metrics_utility.automation_controller_billing.dataframe_engine.factory import Factory as DataframeEngineFactory
from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractorFactory
from metrics_utility.automation_controller_billing.helpers import (
    handle_month,
    parse_date_param,
)
from metrics_utility.automation_controller_billing.report.factory import Factory as ReportFactory
from metrics_utility.automation_controller_billing.report_saver.factory import Factory as ReportSaverFactory
from metrics_utility.exceptions import (
    BadParameter,
    BadRequiredEnvVar,
    BadShipTarget,
    DateFormatError,
    MissingRequiredEnvVar,
    MissingRequiredParameter,
    UnparsableParameter,
)
from metrics_utility.management.validation import (
    handle_directory_ship_target,
    handle_env_validation,
    handle_not_crc,
    handle_not_s3,
    handle_s3_ship_target,
    validate_build_extra_params,
)
from metrics_utility.metric_utils import get_optional_collectors


class Command(BaseCommand):
    """
    Build Report
    """

    help = 'Build Report'

    def __init__(self):
        super().__init__()

        self.help_texts = {
            'since': (
                'Start date for collection (e.g. --since=2023-12-20), '
                'a number of minutes ago (--since=2m), '
                'a number of days ago (--since=5d), or '
                'a number of months ago (--since=2mo | 2 month | 2 months).'
            ),
            'until': (
                'End date for collection (e.g. --until=2023-12-21), '
                'a number of minutes ago (--until=2m), '
                'a number of days ago (--until=5d), or '
                'a number of months ago (--since=2mo | 2 month | 2 months).'
            ),
            'month': (
                'Month the report will be generated for, with format YYYY-MM. '
                "If this parameter is not provided, the previous month's report will be generated if it does not already exist."
            ),
            'ephemeral': (
                'Duration in months or days to determine if host is ephemeral. '
                'Months are considered as 30 days in duration. '
                'Example: --ephemeral=3months, or --ephemeral=3days'
            ),
        }

    def add_arguments(self, parser):
        parser.add_argument('--month', dest='month', action='store', help=self.help_texts.get('month'))
        parser.add_argument('--since', dest='since', action='store', help=self.help_texts.get('since'))
        parser.add_argument(
            '--until',
            dest='until',
            action='store',
            help=self.help_texts.get('until'),
        )
        parser.add_argument(
            '--ephemeral',
            dest='ephemeral',
            action='store',
            help=self.help_texts.get('ephemeral'),
        )
        parser.add_argument(
            '--force',
            dest='force',
            action='store_true',
            help='With this option, the existing reports will be overwritten if running this command again.',
        )
        parser.add_argument('--verbose', dest='verbose', action='store_true', help='Starts to print debug information to terminal.')

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def _handle(self, *args, **options):
        self.init_logging()
        handle_env_validation('build')

        validate_build_extra_params(self.help_texts, options)

        opt_month, month, next_month = handle_month(options.get('month') or None)
        opt_since = parse_date_param(options.get('since'))
        opt_until = parse_date_param(options.get('until'))
        opt_force = options.get('force')

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        extra_params = self._handle_extra_params(ship_target)
        extra_params['opt_since'] = opt_since
        extra_params['opt_until'] = opt_until
        extra_params['opt_ephemeral'] = options.get('ephemeral') or None
        extra_params['month_since'] = month
        extra_params['month_until'] = next_month

        extractor = ExtractorFactory(ship_target, extra_params).create()

        # Determine destination path for generated report and skip processing if it exists
        if opt_since is not None:
            now = datetime.datetime.now().replace(second=0, microsecond=0, tzinfo=timezone.utc)
            extra_params['since_date'] = opt_since.date()
            extra_params['until_date'] = opt_until.date() if opt_until else now.date()

            extra_params['report_period_range'] = f'{extra_params["since_date"]}, {extra_params["until_date"]}'

            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                extractor.get_report_path(extra_params['until_date']),
                f'{extra_params["report_type"]}-{opt_since.date()}--{extra_params["until_date"]}.xlsx',
            )
        else:
            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                extractor.get_report_path(month),
                f'{extra_params["report_type"]}-{opt_month}.xlsx',
            )

        report_saver_engine = ReportSaverFactory(ship_target, extra_params=extra_params).create()

        if report_saver_engine.report_exist() and not opt_force:
            # If the monthly report already exists, skip the generation
            self.logger.info(
                'Skipping report generation, report: '
                f'{report_saver_engine.report_spreadsheet_destination_path} already exists. '
                'Use --force option to override the report.'
            )
            return

        report_dataframe = DataframeEngineFactory(extractor=extractor, month=month, extra_params=extra_params).create()

        if all(item is None or item.empty for item in report_dataframe):
            if opt_since is not None:
                self.logger.info(f'No billing data for input date range {extra_params["since_date"]}--{extra_params["until_date"]}')
            else:
                self.logger.info(f'No billing data for month {opt_month}')
            return

        report_engine = ReportFactory(
            report_period=opt_month,
            report_dataframe=report_dataframe,
            ship_target=ship_target,
            extra_params=extra_params,
        ).create()
        report_spreadsheet = report_engine.build_spreadsheet()

        # Save the report to the configured destination
        report_saver_engine.save(report_spreadsheet)
        self.logger.info(f'Report generated into {ship_target}: {report_saver_engine.report_spreadsheet_destination_path}')

    def handle(self, *args, **options):
        try:
            self._handle(*args, **options)
        except (
            BadShipTarget,
            MissingRequiredEnvVar,
            BadRequiredEnvVar,
            MissingRequiredParameter,
            UnparsableParameter,
            BadParameter,
            DateFormatError,
        ) as e:
            self.logger.error(str(e))
            exit(1)
        except Exception as e:
            self.logger.exception(e)
            exit(1)

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

        report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
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
                'report_organization_filter': os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER', None),
                # optional bits
                'optional_collectors': get_optional_collectors(),
                'optional_sheets': os.getenv(
                    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS',
                    'ccsp_summary,managed_nodes,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules',
                ).split(','),
            }
        )
        return base
