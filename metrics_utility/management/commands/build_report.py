import logging
import os

import datetime
from dateutil.parser import parse as date_parse
from dateutil.relativedelta import relativedelta
from metrics_utility.exceptions import BadShipTarget, MissingRequiredEnvVar, BadRequiredEnvVar
from metrics_utility.automation_controller_billing.collector import Collector
from metrics_utility.automation_controller_billing.dataframe_engine.factory import Factory as DataframeEngineFactory
from metrics_utility.automation_controller_billing.extract.factory import Factory as ExtractorFactory
from metrics_utility.automation_controller_billing.report.factory import Factory as ReportFactory
from metrics_utility.automation_controller_billing.report_saver.factory import Factory as ReportSaverFactory
from metrics_utility.automation_controller_billing.helpers import parse_date_param
from metrics_utility.management.validation import handle_directory_ship_target, handle_s3_ship_target

from dateutil import parser
from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    """
    Gather Automation Controller billing data
    """

    help = 'Gather Automation Controller billing data'

    def add_arguments(self, parser):
        parser.add_argument('--month',
                            dest='month',
                            action='store',
                            help='Month the report will be generated for, with format YYYY-MM. '\
                                 'If this params isn\'t provided, previou month report will be'\
                                 'generated if it doesn\'t exists already.')
        parser.add_argument('--since',
                            dest='since',
                            action='store',
                            help='Date or number of days/months ago we want to generate the reports for.')
        parser.add_argument('--until',
                            dest='until',
                            action='store',
                            help='Date or number of days/months ago we want to generate the reports until. Not available for renewal guidance report')
        parser.add_argument('--ephemeral',
                            dest='ephemeral',
                            action='store',
                            help='Duration in months or days to determine if host is ephemeral. Months are taken'\
                                 'as 30days duration.')
        parser.add_argument('--force',
                            dest='force',
                            action='store_true',
                            help='With this option, the existing reports will be overwritten if '\
                                 'running this command again.')

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def handle(self, *args, **options):
        self.init_logging()

        # parse params
        opt_month = options.get('month') or None
        opt_month, month = self._handle_month(opt_month)

        # Since and ephemenral params are specific to subset of reports
        opt_since = None
        opt_ephemeral = None

        opt_since = options.get('since') or None
        opt_since = parse_date_param(opt_since)

        opt_until = options.get('until') or None
        opt_until = parse_date_param(opt_until)

        opt_ephemeral = options.get('ephemeral') or None

        opt_force = options.get('force')

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        extra_params = self._handle_extra_params(ship_target)
        extra_params['opt_since'] = opt_since
        extra_params['opt_until'] = opt_until
        extra_params['opt_ephemeral'] = opt_ephemeral

        extractor = ExtractorFactory(ship_target, extra_params).create()

        # Determine destination path for generated report and skip processing if it exists
        if opt_since is not None:
            now = datetime.datetime.now().replace(second=0, microsecond=0, tzinfo=timezone.utc)
            extra_params['since_date'] = opt_since.date()
            extra_params['until_date'] = opt_until.date() if opt_until else now.date()

            extra_params['report_period_range'] = f"{extra_params['since_date']}, {extra_params['until_date']}"

            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                extractor.get_report_path(extra_params['until_date']),
                f"{extra_params['report_type']}-{opt_since.date()}--{extra_params['until_date']}.xlsx")
        else:
            extra_params['report_spreadsheet_destination_path'] = os.path.join(
                extractor.get_report_path(month),
                f"{extra_params['report_type']}-{opt_month}.xlsx")

        report_saver_engine = ReportSaverFactory(ship_target, extra_params=extra_params).create()

        if report_saver_engine.report_exist() and not opt_force:
            # If the monthly report already exists, skip the generation
            self.logger.info("Skipping report generation, report: "\
                             f"{report_saver_engine.report_spreadsheet_destination_path} already exists. "\
                             "Use --force option to override the report.")
            return

        report_dataframe = DataframeEngineFactory(
            extractor=extractor,
            month=month,
            extra_params=extra_params).create()

        if report_dataframe[0] is None or report_dataframe[0].empty:
            self.logger.info(f"No billing data for month: {opt_month}")
            return

        report_engine = ReportFactory(report_period=opt_month,
                                      report_dataframe=report_dataframe,
                                      ship_target=ship_target,
                                      extra_params=extra_params).create()
        report_spreadsheet = report_engine.build_spreadsheet()

        # Save the report to the configured destination
        report_saver_engine.save(report_spreadsheet)
        self.logger.info(f"Report generated into {ship_target}: {report_saver_engine.report_spreadsheet_destination_path}")

    def _handle_ship_target(self, ship_target):
        if ship_target == "controller_db":
            return {}
        elif ship_target == "directory":
            return handle_directory_ship_target(ship_target)
        elif ship_target == "s3":
            return handle_s3_ship_target(ship_target)
        else:
            raise BadShipTarget("Unexpected value for METRICS_UTILITY_SHIP_TARGET env var"\
                                ", allowed value for local report generation are "\
                                "['s3', 'directory', 'controller_db']")

    def _handle_extra_params(self, ship_target=None):
        base = self._handle_ship_target(ship_target)

        ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
        report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
        price_per_node = float(os.getenv('METRICS_UTILITY_PRICE_PER_NODE', 0))

        if not ship_path:
            raise MissingRequiredEnvVar(
                "Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination"\
                " for the generated data and for the built reports")

        if not report_type:
            raise MissingRequiredEnvVar(
                "Missing required env variable METRICS_UTILITY_REPORT_TYPE.")
        elif report_type not in ['CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE']:
            raise BadRequiredEnvVar(
                "Bad value for required env variable METRICS_UTILITY_REPORT_TYPE, allowed"\
                " values are: ['CCSP', 'CCSPv2', 'RENEWAL_GUIDANCE']")

        base.update({
            "ship_path": ship_path,
            "report_type": report_type,
            "price_per_node": price_per_node,
            # XLSX specific params
            "report_sku": os.getenv('METRICS_UTILITY_REPORT_SKU', ""),
            "report_sku_description": os.getenv('METRICS_UTILITY_REPORT_SKU_DESCRIPTION', ""),
            "report_h1_heading": os.getenv('METRICS_UTILITY_REPORT_H1_HEADING', ""),
            "report_company_name": os.getenv('METRICS_UTILITY_REPORT_COMPANY_NAME', ""),
            "report_email": os.getenv('METRICS_UTILITY_REPORT_EMAIL', ""),
            "report_rhn_login": os.getenv('METRICS_UTILITY_REPORT_RHN_LOGIN', ""),
            "report_po_number": os.getenv('METRICS_UTILITY_REPORT_PO_NUMBER', ""),
            "report_company_business_leader": os.getenv('METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER', ""),
            "report_company_procurement_leader": os.getenv('METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER', ""),
            "report_end_user_company_name": os.getenv('METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME', ""),
            "report_end_user_company_city": os.getenv('METRICS_UTILITY_REPORT_END_USER_CITY', ""),
            "report_end_user_company_state": os.getenv('METRICS_UTILITY_REPORT_END_USER_STATE', ""),
            "report_end_user_company_country": os.getenv('METRICS_UTILITY_REPORT_END_USER_COUNTRY', ""),
            # Renewal guidance specific params
            "report_renewal_guidance_dedup_iterations": os.getenv('REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS', "3"),
            "report_organization_filter": os.getenv('METRICS_UTILITY_ORGANIZATION_FILTER', None),
            # S3 specific options
            "s3_bucket_name": os.getenv('METRICS_UTILITY_BUCKET_NAME', None),
            "s3_bucket_endpoint": os.getenv('METRICS_UTILITY_BUCKET_ENDPOINT', None),
            "s3_bucket_region": os.getenv('METRICS_UTILITY_BUCKET_REGION', None),
            "s3_bucket_id": os.getenv('METRICS_UTILITY_BUCKET_ID', None),
            "s3_bucket_key": os.getenv('METRICS_UTILITY_BUCKET_KEY', None),
        })
        return base

    def _handle_month(self, month):
        # Process month argument
        if month is not None:
            date = date_parse(f"{month}-01")
        else:
            # Return last month if no month was passed
            beginning_of_the_month = datetime.date.today().replace(day=1)
            beginning_of_the_previous_month = beginning_of_the_month - relativedelta(months=1)
            date = beginning_of_the_previous_month
            y = date.strftime("%Y")
            m = date.strftime("%m")
            month = f"{y}-{m}"

        return month, date
