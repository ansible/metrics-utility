"""Package implementation that ships billing data to the Red Hat CRC ingress API."""

import json
import os
import tempfile

from datetime import datetime, timezone

import requests

from awx.main.utils import get_awx_http_client_headers
from cryptography import x509
from django.conf import settings

import metrics_utility.base as base

from metrics_utility.exceptions import FailedToUploadPayload
from metrics_utility.logger import logger


def _is_cert_valid(cert_pem: str) -> bool:
    """Return True if cert_pem is a parseable, non-expired X.509 certificate."""
    try:
        cert = x509.load_pem_x509_certificate(cert_pem.encode('utf-8'))
        if cert.not_valid_after_utc < datetime.now(timezone.utc):
            logger.warning(f'Candlepin cert expired at {cert.not_valid_after_utc}; falling back to service account auth')
            return False
        return True
    except Exception as e:
        logger.warning(f'Could not parse Candlepin cert: {e}')
        return False


class PackageCRC(base.Package):
    """Package that ships tarballs to Red Hat's CRC (console.redhat.com) ingress API.

    Uses service-account OAuth2 credentials to obtain a bearer token from the
    SSO endpoint before uploading.
    """

    CERT_PATH = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
    PAYLOAD_CONTENT_TYPE = 'application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz'

    SHIPPING_AUTH_SERVICE_ACCOUNT = 'service-account'

    def __init__(self, collector):
        super().__init__(collector)
        self._resolved_auth_mode = None
        self._temp_cert_path = None
        self._temp_key_path = None

    def _candlepin_cert_pem(self):
        return (self.collector.billing_provider_params or {}).get('candlepin_cert_pem')

    def _candlepin_key_pem(self):
        return (self.collector.billing_provider_params or {}).get('candlepin_key_pem')

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

    def _get_client_certificates(self):
        if self._temp_cert_path and self._temp_key_path:
            return (self._temp_cert_path, self._temp_key_path)
        return super()._get_client_certificates()

    def ship(self):
        if self.shipping_auth_mode() == self.SHIPPING_AUTH_SERVICE_ACCOUNT:
            return super().ship()

        # mTLS path: write cert + key to secure temp files, then delegate to base ship().
        # Temp files must remain on disk across _get_client_certificates() and the POST.
        cert_fd = key_fd = cert_path = key_path = None
        try:
            cert_fd, cert_path = tempfile.mkstemp(prefix='metrics_cert_', suffix='.pem')
            os.chmod(cert_path, 0o600)
            with os.fdopen(cert_fd, 'w') as f:
                f.write(self._candlepin_cert_pem())
            cert_fd = None  # fdopen took ownership

            key_fd, key_path = tempfile.mkstemp(prefix='metrics_key_', suffix='.pem')
            os.chmod(key_path, 0o600)
            with os.fdopen(key_fd, 'w') as f:
                f.write(self._candlepin_key_pem())
            key_fd = None

            self._temp_cert_path = cert_path
            self._temp_key_path = key_path
            return super().ship()

        except requests.exceptions.SSLError as e:
            # Before falling back, verify that service account credentials are present.
            # In an mTLS-only deployment they won't be, and a blind retry would silently
            # return False with no actionable error context for the operator.
            if not self._get_rh_user() or not self._get_rh_password():
                raise FailedToUploadPayload(
                    f'mTLS upload failed and no service account credentials are configured to fall back to '
                    f'(METRICS_UTILITY_SERVICE_ACCOUNT_ID / METRICS_UTILITY_SERVICE_ACCOUNT_SECRET are not set). '
                    f'Original SSL error: {e}'
                ) from e
            logger.error(f'mTLS upload failed ({e}); retrying with service account auth')
            self._resolved_auth_mode = self.SHIPPING_AUTH_SERVICE_ACCOUNT
            self._temp_cert_path = None
            self._temp_key_path = None
            return super().ship()

        finally:
            for fd in [cert_fd, key_fd]:
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            for path in [cert_path, key_path]:
                if path and os.path.exists(path):
                    try:
                        os.unlink(path)
                    except OSError as e:
                        logger.warning(f'Could not remove temp cert file {path}: {e}')
            self._temp_cert_path = None
            self._temp_key_path = None

    def shipping_auth_mode(self):
        if self._resolved_auth_mode is not None:
            return self._resolved_auth_mode

        cert_pem = self._candlepin_cert_pem()
        key_pem = self._candlepin_key_pem()
        if cert_pem and key_pem and _is_cert_valid(cert_pem):
            self._resolved_auth_mode = self.SHIPPING_AUTH_CERTIFICATES
        else:
            self._resolved_auth_mode = self.SHIPPING_AUTH_SERVICE_ACCOUNT

        return self._resolved_auth_mode

    def is_shipping_configured(self):
        # TODO: move to base, or children
        ret = super().is_shipping_configured()
        if ret is False:
            return False

        if self.shipping_auth_mode() == self.SHIPPING_AUTH_CERTIFICATES:
            if not self.get_ingress_url():
                logger.error('METRICS_UTILITY_CRC_INGRESS_URL is not set')
                return False
            return True

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

        elif self.shipping_auth_mode() == self.SHIPPING_AUTH_CERTIFICATES:
            # session.cert is already set by base ship(); just POST with server cert verification.
            proxies = {'https': self.get_proxy_url()} if self.get_proxy_url() else {}
            response = session.post(
                url,
                files=files,
                verify=self.CERT_PATH,
                proxies=proxies,
                headers=session.headers,
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
