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

    def init_logging(self):
        self.logger = logging.getLogger('awx.main.analytics')
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def handle(self, *args, **options):
        self.init_logging()

        opt_month = options.get('month') or None
        opt_month, month = self._handle_month(opt_month)

        ship_target = os.getenv('METRICS_UTILITY_SHIP_TARGET', None)
        extra_params = self._handle_ship_target(ship_target)

        extractor = ExtractorFactory(ship_target, extra_params).create()

        # Determine destination path for generated report and skip processing if it exists
        report_spreadsheet_destination_path = os.path.join(
            extractor.get_report_path(month),
            f"{extra_params['report_type']}-{opt_month}.xlsx")

        if os.path.exists(report_spreadsheet_destination_path):
            # If the monthly report already exists, skip the generation
            self.logger.info("Skipping report generation, report: "\
                             f"{report_spreadsheet_destination_path} already exists")
            return
        else:
            # otherwise create the dir structure for the final report
            os.makedirs(os.path.dirname(report_spreadsheet_destination_path), exist_ok=True)

        dataframe_engine = DataframeEngineFactory(
            extractor=extractor,
            month=month,
            extra_params=extra_params).create()

        report_dataframe = dataframe_engine.build_dataframe()
        if report_dataframe is None or report_dataframe.empty:
            self.logger.info(f"No billing data for month: {opt_month}")
            return

        report_engine = ReportFactory(report_period=opt_month,
                                      report_dataframe=report_dataframe,
                                      ship_target=ship_target,
                                      extra_params=extra_params).create()
        report_spreadsheet = report_engine.build_spreadsheet()
        report_spreadsheet.save(report_spreadsheet_destination_path)

        self.logger.info(f"Report generated into: {report_spreadsheet_destination_path}")

    def _handle_ship_target(self, ship_target):
        if ship_target == "directory":
            return self._handle_extra_params()
        else:
            raise BadShipTarget("Unexpected value for METRICS_UTILITY_SHIP_TARGET env var"\
                                ", allowed value for local report generation are "\
                                "[directory]")

    def _handle_extra_params(self):
        ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)
        report_type = os.getenv('METRICS_UTILITY_REPORT_TYPE', None)
        price_per_node = int(os.getenv('METRICS_UTILITY_PRICE_PER_NODE', 0))

        if not ship_path:
            raise MissingRequiredEnvVar(
                "Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination"\
                " for the generated data and for the built reports")

        if not report_type:
            raise MissingRequiredEnvVar(
                "Missing required env variable METRICS_UTILITY_REPORT_TYPE.")
        elif report_type not in ["CCSP"]:
            raise BadRequiredEnvVar(
                "Bad value for required env variable METRICS_UTILITY_REPORT_TYPE, allowed"\
                " valies are: [CCSP]")

        return {"ship_path": ship_path,
                "report_type": report_type,
                "price_per_node": price_per_node,
                # XLSX specific params
                "report_sku": os.getenv('METRICS_UTILITY_REPORT_SKU', ""),
                "report_sku_description": os.getenv('METRICS_UTILITY_REPORT_SKU_DESCRIPTION', ""),
                "report_h1_heading": os.getenv('METRICS_UTILITY_REPORT_H1_HEADING', ""),
                "report_company_name": os.getenv('METRICS_UTILITY_REPORT_COMPANY_NAME', ""),
                "report_email": os.getenv('METRICS_UTILITY_REPORT_EMAIL', ""),
                "report_rhn_login": os.getenv('METRICS_UTILITY_REPORT_RHN_LOGIN', ""),
                "report_company_business_leader": os.getenv('METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER', ""),
                "report_company_procurement_leader": os.getenv('METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER', ""),
                }

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


