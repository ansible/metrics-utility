from insights_analytics_collector import Collector
from tests.classes.package import Package


class AnalyticsCollector(Collector):
    @staticmethod
    def db_connection():
        return None

    @staticmethod
    def _package_class():
        return Package

    def _is_shipping_configured(self):
        return False

    def _is_valid_license(self):
        return True

    def _last_gathering(self):
        return None

    def _load_last_gathered_entries(self):
        return {}

    def _save_last_gathered_entries(self, last_gathered_entries):
        return None

    def _save_last_gather(self):
        return None
