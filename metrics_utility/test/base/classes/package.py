from metrics_utility.base import Package as InsightsAnalyticsPackage


class Package(InsightsAnalyticsPackage):
    MAX_DATA_SIZE = 1000  # bytes

    def _tarname_base(self):
        timestamp = self.collector.gather_until
        return f'test-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'
