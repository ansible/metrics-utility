import logging
import os

import datetime
from metrics_utility.exceptions import BadShipTarget, MissingRequiredEnvVar, FailedToUploadPayload,\
    BadRequiredEnvVar
from metrics_utility.automation_controller_billing.collector import Collector
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
        except (BadShipTarget, MissingRequiredEnvVar, BadRequiredEnvVar, FailedToUploadPayload) as e:
            self.logger.error(e.name)
            exit(0)
        except Exception as e:
            self.logger.exception(e)
            exit(0)

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

    def _handle_ship_target(self, ship_target):
        if ship_target == "crc":
            return self._handle_crc_ship_target()
        elif ship_target == "directory":
            return self._handle_directory_ship_target()
        else:
            raise BadShipTarget("Unexpected value for METRICS_UTILITY_SHIP_TARGET env var"\
                                ", allowed values are [crc, directory]")


    def _handle_crc_ship_target(self):
        billing_provider = os.getenv('METRICS_UTILITY_BILLING_PROVIDER', None)
        red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID', None)

        billing_provider_params = {"billing_provider": billing_provider}
        if billing_provider == "aws":
            billing_account_id = os.getenv('METRICS_UTILITY_BILLING_ACCOUNT_ID', None)
            if not billing_account_id:
                raise MissingRequiredEnvVar(
                    "Env var: METRICS_UTILITY_BILLING_ACCOUNT_ID, containing "\
                    " AWS 12 digit customer id needs to be provided.")
            billing_provider_params["billing_account_id"] = billing_account_id
        else:
            raise MissingRequiredEnvVar(
                "Uknown METRICS_UTILITY_BILLING_PROVIDER env var, supported values are"\
                " [aws].")

        if red_hat_org_id:
            billing_provider_params["red_hat_org_id"] = red_hat_org_id

        return billing_provider_params

    def _handle_directory_ship_target(self):
        ship_path = os.getenv('METRICS_UTILITY_SHIP_PATH', None)

        if not ship_path:
            raise MissingRequiredEnvVar(
                "Missing required env variable METRICS_UTILITY_SHIP_PATH, having destination "\
                "for the generated data")

        return {"ship_path": ship_path}


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


