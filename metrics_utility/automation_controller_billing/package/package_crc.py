"""Package implementation that ships billing data to the Red Hat CRC ingress API."""

import json
import os
import time

import requests

from awx.main.utils import get_awx_http_client_headers
from django.conf import settings

import metrics_utility.base as base

from metrics_utility.exceptions import FailedToUploadPayload
from metrics_utility.logger import logger


class PackageCRC(base.Package):
    """Package that ships tarballs to Red Hat's CRC (console.redhat.com) ingress API.

    Uses service-account OAuth2 credentials to obtain a bearer token from the
    SSO endpoint before uploading.  The token is cached in an instance variable
    and reused across uploads until it is within TOKEN_EXPIRY_BUFFER_SECONDS of
    expiry, at which point a fresh token is fetched.
    """

    CERT_PATH = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
    PAYLOAD_CONTENT_TYPE = 'application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz'

    SHIPPING_AUTH_SERVICE_ACCOUNT = 'service-account'

    # Refresh the cached token this many seconds before it actually expires so
    # that in-flight uploads never use a token that is about to become invalid.
    TOKEN_EXPIRY_BUFFER_SECONDS = 60

    def __init__(self, collector):
        super().__init__(collector)
        self._cached_token = None
        self._token_expiry = 0.0  # Unix timestamp after which the token must not be used

    def _bearer(self):
        """Return a valid OAuth2 bearer token, fetching a fresh one only when needed.

        The token is cached as an instance variable.  A new token is requested
        whenever the cached token is absent or will expire within
        TOKEN_EXPIRY_BUFFER_SECONDS seconds.  If SSO is temporarily unavailable
        and a cached (still-valid) token exists, that token is returned so that
        uploads can continue without interruption.
        """
        now = time.monotonic()
        if self._cached_token is not None and now < self._token_expiry:
            return self._cached_token

        sso_url = self.get_sso_url()
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': self._get_rh_user(),
            'client_secret': self._get_rh_password(),
            'grant_type': 'client_credentials',
        }

        try:
            r = requests.post(sso_url, headers=headers, data=data, verify=self.CERT_PATH, timeout=(31, 31))
            r.raise_for_status()
            payload = json.loads(r.content)
            access_token = payload['access_token']
            expires_in = int(payload.get('expires_in', 300))
            self._cached_token = access_token
            self._token_expiry = now + expires_in - self.TOKEN_EXPIRY_BUFFER_SECONDS
            return self._cached_token
        except Exception as exc:
            if self._cached_token is not None:
                # SSO is temporarily unavailable; reuse the cached token if it
                # has not yet expired (ignoring the buffer window so we stay
                # functional as long as possible).
                logger.warning(
                    f'Failed to refresh OAuth2 token ({exc}); reusing cached token.'
                )
                return self._cached_token
            raise

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
            #################################
            ## Query crc with bearer token
            headers = session.headers
            headers['authorization'] = f'Bearer {self._bearer()}'

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
