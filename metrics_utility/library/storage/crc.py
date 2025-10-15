import json

from importlib.metadata import version

import requests


class Base:
    def __init__(self, **settings):
        self.ingress_url = settings.get('ingress_url', 'https://console.redhat.com/api/ingress/v1/upload')
        self.proxy_url = settings.get('proxy_url')
        self.verify_cert_path = settings.get('verify_cert_path', '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem')

    def _session(self):
        session = requests.Session()
        session.headers = {
            'User-Agent': f'metrics-utility {version("metrics-utility")}',
        }

        session.verify = self.verify_cert_path
        session.timeout = (31, 31)

        return session

    def _proxies(self):
        if not self.proxy_url:
            return {}

        return {'https': self.proxy_url}

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None):
        # FIXME: only for .tar.gz
        tgz_content_type = 'application/vnd.redhat.aap-billing-controller.aap_billing_controller_payload+tgz'

        if filename:
            with open(filename, 'rb') as f:
                self._put((artifact_name, f, tgz_content_type))

        if fileobj:
            self._put((artifact_name, fileobj, tgz_content_type))

        if dict:
            self._put((artifact_name, json.dumps(dict)))

    def _put(self, file_tuple):
        response = self._request({'file': file_tuple})

        # Accept 2XX status_codes
        if response.status_code >= 300:
            raise Exception(f'{self.__class__.__name__}: Upload failed with status {response.status_code}: {response.text}')


class StorageCRC(Base):
    def __init__(self, **settings):
        super().__init__(**settings)

        self.sso_url = settings.get('sso_url', 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token')
        self.client_id = settings.get('client_id')
        self.client_secret = settings.get('client_secret')

        if not self.client_id:
            raise Exception('StorageCRC: client_id not set')

        if not self.client_secret:
            raise Exception('StorageCRC: client_secret not set')

    def _bearer(self):
        response = requests.post(
            self.sso_url,
            data={
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'client_credentials',
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=(31, 31),
            verify=self.verify_cert_path,
        )

        return json.loads(response.content)['access_token']

    def _request(self, files):
        session = self._session()

        access_token = self._bearer()
        session.headers['authorization'] = f'Bearer {access_token}'

        return session.post(
            self.ingress_url,
            files=files,
            proxies=self._proxies(),
        )


class StorageCRCMutual(Base):
    def __init__(self, **settings):
        super().__init__(**settings)

        self.session_cert = settings.get(
            'session_cert',
            (
                '/etc/pki/consumer/cert.pem',
                '/etc/pki/consumer/key.pem',
            ),
        )

    def _request(self, files):
        session = self._session()

        # a single file (containing the private key and the certificate)
        # or a tuple of both files paths (cert_file, keyfile)
        session.cert = self.session_cert

        return session.post(
            self.ingress_url,
            files=files,
            proxies=self._proxies(),
        )
