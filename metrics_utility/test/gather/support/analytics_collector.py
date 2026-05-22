"""Test analytics collector stub used by functional tests."""

from metrics_utility.gather.collector import Collector
from metrics_utility.test.gather.support.package import Package


class AnalyticsCollector(Collector):
    """Minimal Collector subclass for functional tests.

    Uses in-memory no-op implementations of ``_load_last_gathered_entries``
    and ``_update_last_gathered_entries`` so tests can run without a database.
    """

    def _create_package(self):
        return Package(self)

    def _load_last_gathered_entries(self):
        return {}

    def _update_last_gathered_entries(self):
        last_gathered_updates = {'keys': {}, 'locked': set()}

        for _, packages in self.packages.items():
            for package in packages:
                package.update_last_gathered_entries(last_gathered_updates)

        for unsuccessful_key in last_gathered_updates['locked']:
            last_gathered_updates.pop(f'{unsuccessful_key}_full', None)

        self.last_gathered_entries.update(last_gathered_updates['keys'])

    def _gather_finalize(self):
        if not self.ship:
            return
        self._update_last_gathered_entries()
