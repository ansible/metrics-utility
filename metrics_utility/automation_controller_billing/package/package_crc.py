import base64
import json
import os

import requests

from awx.main.utils import get_awx_http_client_headers
from django.conf import settings

import metrics_utility.base as base

from metrics_utility.exceptions import FailedToUploadPayload, MetricsException
from metrics_utility.logger import logger


class PackageCRC(base.Package):
    CERT_PATH = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
    PAYLOAD_CONTENT_TYPE = 'application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz'

    def _tarname_base(self):
        timestamp = self.collector.gather_until
        return f'{settings.SYSTEM_UUID}-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'

    def _get_sso_url(self):
        return os.getenv('METRICS_UTILITY_CRC_SSO_URL', 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token')

    def _get_ingress_url(self):
        return os.getenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')

    def _get_proxy_url(self):
        return os.getenv('METRICS_UTILITY_PROXY_URL')

    def _get_rh_user(self):
        return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID')

    def _get_rh_password(self):
        return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET')

    def _get_http_request_headers(self):
        return get_awx_http_client_headers()

    # only service_account was reachable in 0.6.0 without changing code; identity still isn't
    # FIXME: only use for service if needed, remove if not
    def _shipping_auth(self):
        return os.getenv('METRICS_UTILITY_SHIP_AUTH', 'service_account')  # identity | mutual_tls | service_account | user_pass

    def is_shipping_configured(self):
        if not self.tar_path:
            logger.error('Insights for Ansible Automation Platform TAR not found')
            return False

        if not os.path.exists(self.tar_path):
            logger.error(f'Insights for Ansible Automation Platform TAR {self.tar_path} not found')
            return False

        if 'Error:' in str(self.tar_path):
            return False

        if not self._get_ingress_url():
            logger.error('METRICS_UTILITY_CRC_INGRESS_URL is not set')
            return False

        if self._shipping_auth() == 'service_account':
            if not self._get_sso_url():
                logger.error('METRICS_UTILITY_CRC_SSO_URL is not set')
                return False

        if self._shipping_auth() in ('service_account', 'user_pass'):
            if not self._get_rh_user():
                logger.error('METRICS_UTILITY_SERVICE_ACCOUNT_ID is not set')
                return False

            if not self._get_rh_password():
                logger.error('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET is not set')
                return False

        # _get_proxy_url is optional

        return True

    def ship(self):
        """
        Ship gathered metrics to the Insights API
        """
        if not self.is_shipping_configured():
            self.shipping_successful = False
            return False

        logger.debug(f'shipping analytics file: {self.tar_path}')

        with open(self.tar_path, 'rb') as f:
            files = {
                'file': (
                    os.path.basename(self.tar_path),
                    f,
                    self.PAYLOAD_CONTENT_TYPE,
                )
            }

            response = self._request(files)

            # Accept 2XX status_codes
            if response.status_code >= 300:
                raise FailedToUploadPayload(f'Upload failed with status {response.status_code}, {response.text}')

            self.shipping_successful = True
            return True

    def _session(self):
        session = requests.Session()
        session.headers = self._get_http_request_headers()
        session.headers.pop('Content-Type')

        session.verify = self.CERT_PATH
        session.timeout = (31, 31)

        return session

    def _bearer(self):
        response = requests.post(
            self._get_sso_url(),
            data={'client_id': self._get_rh_user(), 'client_secret': self._get_rh_password(), 'grant_type': 'client_credentials'},
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=(31, 31),
            verify=self.CERT_PATH,
        )

        return json.loads(response.content)['access_token']

    def _proxies(self):
        if not self._get_proxy_url():
            return {}

        return {'https': self._get_proxy_url()}

    def _identity(self, url, files):
        session = self._session()

        # FIXME: make parametrizable, if used
        identity = {
            'identity': {
                'type': 'User',
                'account_number': '0000001',
                'user': {'is_org_admin': True},
                'internal': {'org_id': '000001'},
            }
        }
        session.headers['x-rh-identity'] = base64.b64encode(json.dumps(identity).encode('utf8'))

        return session.post(
            url,
            files=files,
            proxies=self._proxies(),
        )

    def _mutual_tls(self, url, files):
        session = self._session()

        # a single file (containing the private key and the certificate)
        # or a tuple of both files paths (cert_file, keyfile)
        session.cert = (
            '/etc/pki/consumer/cert.pem',
            '/etc/pki/consumer/key.pem',
        )

        return session.post(
            url,
            files=files,
            proxies=self._proxies(),
        )

    def _service_account(self, url, files):
        session = self._session()

        access_token = self._bearer()
        session.headers['authorization'] = f'Bearer {access_token}'

        return session.post(
            url,
            files=files,
            proxies=self._proxies(),
        )

    def _user_pass(self, url, files):
        session = self._session()
        session.auth = (self._get_rh_user(), self._get_rh_password())

        return session.post(
            url,
            files=files,
            proxies=self._proxies(),
        )

    def _request(self, url, files):
        url = self._get_ingress_url()
        mode = self._shipping_auth()
        if mode == 'identity':
            return self._identity(url, files)
        elif mode == 'mutual_tls':
            return self._mutual_tls(url, files)
        elif mode == 'service_account':
            return self._service_account(url, files)
        elif mode == 'user_pass':
            return self._user_pass(url, files)
        else:
            raise MetricsException(f'Invalid METRICS_UTILITY_SHIP_AUTH {mode}: identity | mutual_tls | service_account (default) | user_pass')
