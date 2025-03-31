from metrics_utility.base import Package as InsightsAnalyticsPackage


class Package(InsightsAnalyticsPackage):
    PAYLOAD_CONTENT_TYPE = 'application/vnd.redhat.test.test_payload+tgz'
    MAX_DATA_SIZE = 1000

    def _tarname_base(self):
        timestamp = self.collector.gather_until
        return f'test-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'

    def get_ingress_url(self):
        return None

    def _get_rh_user(self):
        return ''

    def _get_rh_password(self):
        return ''

    def _get_x_rh_identity(self):
        return ''

    def _get_http_request_headers(self):
        return {}
