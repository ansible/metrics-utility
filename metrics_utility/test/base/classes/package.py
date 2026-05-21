"""Test Package stub used by functional tests."""

from metrics_utility.gather.package.package import Package as InsightsAnalyticsPackage


class Package(InsightsAnalyticsPackage):
    """Minimal Package subclass for functional tests.

    Uses a small MAX_DATA_SIZE so tests trigger file splitting with little data.
    Shipping is a no-op.
    """

    MAX_DATA_SIZE = 1000

    def _tarname_base(self):
        timestamp = self.collector.gather_until
        return f'test-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'

    def ship(self):
        self.shipping_successful = True
        return True
