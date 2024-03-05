import insights_analytics_collector as base

from awx.main.utils import get_awx_http_client_headers

from django.conf import settings


class PackageCRC(base.Package):
    CERT_PATH = "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
    PAYLOAD_CONTENT_TYPE = "application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz"

    def _tarname_base(self):
        timestamp = self.collector.gather_until
        return f'{settings.SYSTEM_UUID}-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'

    def get_ingress_url(self):
        return getattr(settings, 'AUTOMATION_ANALYTICS_URL', None)

    def _get_rh_user(self):
        return getattr(settings, 'REDHAT_USERNAME', None)

    def _get_rh_password(self):
        return getattr(settings, 'REDHAT_PASSWORD', None)

    def _get_http_request_headers(self):
        return get_awx_http_client_headers()

    def shipping_auth_mode(self):
        # TODO make this as a configuration so we can use this for local testing,
        # for now, uncomment when testin locally in docker
        # return self.SHIPPING_AUTH_IDENTITY

        # TODO: allow to use certificate based auth based on Controller config
        return self.SHIPPING_AUTH_USERPASS
