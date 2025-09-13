from base.classes.package import Package

from metrics_utility.base import Collector


class AnalyticsCollector(Collector):
    @staticmethod
    def db_connection():
        return None

    @staticmethod
    def _package_class():
        return Package

    def _load_last_gathered_entries(self):
        return {}

    def _save_last_gathered_entries(self, last_gathered_entries):
        return None
