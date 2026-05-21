"""CRC (console.redhat.com) transport: OAuth2 token exchange and tarball upload."""

import json
import os

import requests

from awx.main.utils import get_awx_http_client_headers

from metrics_utility.exceptions import FailedToUploadPayload
from metrics_utility.logger import logger


CERT_PATH = '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem'
PAYLOAD_CONTENT_TYPE = 'application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz'


def get_sso_url():
    return os.getenv('METRICS_UTILITY_CRC_SSO_URL', 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token')


def get_ingress_url():
    return os.getenv('METRICS_UTILITY_CRC_INGRESS_URL', 'https://console.redhat.com/api/ingress/v1/upload')


def get_proxy_url():
    return os.getenv('METRICS_UTILITY_PROXY_URL')


def _get_rh_user():
    return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_ID')


def _get_rh_password():
    return os.getenv('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET')


def is_shipping_configured():
    if not get_ingress_url():
        logger.error('METRICS_UTILITY_CRC_INGRESS_URL is not set')
        return False

    if not get_sso_url():
        logger.error('METRICS_UTILITY_CRC_SSO_URL is not set')
        return False

    if not _get_rh_user():
        logger.error('METRICS_UTILITY_SERVICE_ACCOUNT_ID is not set')
        return False

    if not _get_rh_password():
        logger.error('METRICS_UTILITY_SERVICE_ACCOUNT_SECRET is not set')
        return False

    return True


def ship(tar_path):
    with open(tar_path, 'rb') as f:
        files = {
            'file': (
                os.path.basename(tar_path),
                f,
                PAYLOAD_CONTENT_TYPE,
            ),
        }

        session = requests.Session()
        session.headers = get_awx_http_client_headers()
        session.headers.pop('Content-Type')

        # Get OAuth2 bearer token
        r = requests.post(
            get_sso_url(),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={'client_id': _get_rh_user(), 'client_secret': _get_rh_password(), 'grant_type': 'client_credentials'},
            verify=CERT_PATH,
            timeout=(31, 31),
        )
        access_token = json.loads(r.content)['access_token']

        headers = session.headers
        headers['authorization'] = f'Bearer {access_token}'

        proxies = {}
        if get_proxy_url():
            proxies = {'https': get_proxy_url()}

        response = session.post(
            get_ingress_url(),
            files=files,
            verify=CERT_PATH,
            proxies=proxies,
            headers=headers,
            timeout=(31, 31),
        )

    if response.status_code >= 300:
        raise FailedToUploadPayload(f'Upload failed with status {response.status_code}, {response.text}')

    return True
