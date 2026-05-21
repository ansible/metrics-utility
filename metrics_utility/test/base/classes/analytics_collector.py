"""Test analytics collector stub used by functional tests."""

from base.classes.package import Package

from metrics_utility.gather.collector import Collector


class AnalyticsCollector(Collector):
    """Minimal Collector subclass for functional tests.

    Uses in-memory no-op implementations of ``_load_last_gathered_entries``
    and ``_save_last_gathered_entries`` so tests can run without a database.
    """

    @staticmethod
    def _package_class():
        return Package

    def _load_last_gathered_entries(self):
        return {}

    def _save_last_gathered_entries(self, last_gathered_entries):
        return None

    def _gather_finalize(self):
        if not self.ship:
            return
        self._update_last_gathered_entries()
