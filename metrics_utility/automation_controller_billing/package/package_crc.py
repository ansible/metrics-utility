import json
import os

import requests

from awx.main.utils import get_awx_http_client_headers
from django.conf import settings

import metrics_utility.base as base

from metrics_utility.exceptions import FailedToUploadPayload
from metrics_utility.logger import logger


class PackageCRC(base.Package):
    CERT_PATH = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
    PAYLOAD_CONTENT_TYPE = 'application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz'

    SHIPPING_AUTH_SERVICE_ACCOUNT = 'service-account'

    def _tarname_base(self):
        timestamp = self.collector.gather_until
        return f'{settings.SYSTEM_UUID}-{timestamp.strftime("%Y-%m-%d-%H%M%S%z")}'

    def get_sso_url(self):
        return os.getenv('METRICS_UTILITY_CRC_SSO_URL', 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token')

    def get_ingress_url(self):
        return os.getenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')

    def get_proxy_url(self):
        return os.getenv('METRICS_UTILITY_PROXY_URL')

    def _get_rh_user(self):
        return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID')

    def _get_rh_password(self):
        return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET')

    def _get_http_request_headers(self):
        return get_awx_http_client_headers()

    def shipping_auth_mode(self):
        # TODO make this as a configuration so we can use this for local testing,
        # for now, uncomment when testin locally in docker
        # return self.SHIPPING_AUTH_IDENTITY

        return self.SHIPPING_AUTH_SERVICE_ACCOUNT

    def is_shipping_configured(self):
        # TODO: move to base, or children
        ret = super()
        if ret is False:
            return False

        if self.shipping_auth_mode() == self.SHIPPING_AUTH_SERVICE_ACCOUNT:
            if not self.get_ingress_url():
                logger.error('METRICS_UTILITY_CRC_INGRESS_URL is not set')
                return False

            if not self.get_sso_url():
                logger.error('METRICS_UTILITY_CRC_SSO_URL is not set')
                return False

            if not self._get_rh_user():
                logger.error('METRICS_UTILITY_SERVICE_ACCOUNT_ID is not set')
                return False

            if not self._get_rh_password():
                logger.error('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET is not set')
                return False
        return True

    def _send_data(self, url, files, session):
        # TODO: move to base
        if self.shipping_auth_mode() == self.SHIPPING_AUTH_SERVICE_ACCOUNT:
            sso_url = self.get_sso_url()
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}

            data = {'client_id': self._get_rh_user(), 'client_secret': self._get_rh_password(), 'grant_type': 'client_credentials'}

            r = requests.post(sso_url, headers=headers, data=data, verify=self.CERT_PATH, timeout=(31, 31))
            access_token = json.loads(r.content)['access_token']

            #################################
            ## Query crc with bearer token
            headers = session.headers
            headers['authorization'] = f'Bearer {access_token}'

            proxies = {}
            if self.get_proxy_url():
                proxies = {'https': self.get_proxy_url()}

            response = session.post(
                url,
                files=files,
                verify=self.CERT_PATH,
                proxies=proxies,
                headers=headers,
                timeout=(31, 31),
            )

        elif self.shipping_auth_mode() == self.SHIPPING_AUTH_USERPASS:
            response = session.post(
                url,
                files=files,
                verify=self.CERT_PATH,
                auth=(self._get_rh_user(), self._get_rh_password()),
                headers=session.headers,
                timeout=(31, 31),
            )

        else:
            response = session.post(url, files=files, headers=session.headers, timeout=(31, 31))

        # Accept 2XX status_codes
        if response.status_code >= 300:
            raise FailedToUploadPayload(f'Upload failed with status {response.status_code}, {response.text}')

        return True
