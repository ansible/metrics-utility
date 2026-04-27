"""Storage backends for uploading artifacts to Red Hat CRC (console.redhat.com)."""

import json

from importlib.metadata import version

import requests


class Base:
    """Abstract base class for CRC storage backends.

    Concrete subclasses implement :meth:`_request` to perform the authenticated
    HTTP upload.
    """

    def __init__(self, **settings):
        """Initialise with ingress/proxy/cert settings.

        Args:
            **settings: Accepts ``'ingress_url'``, ``'proxy_url'``, and
                ``'verify_cert_path'`` keys.
        """
        self.ingress_url = settings.get('ingress_url', 'https://console.redhat.com/api/ingress/v1/upload')
        self.proxy_url = settings.get('proxy_url')
        self.verify_cert_path = settings.get('verify_cert_path', '/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem')

    def _session(self):
        """Create and return a configured :class:`requests.Session` instance."""
        session = requests.Session()
        session.headers = {
            'User-Agent': f'metrics-utility {version("metrics-utility")}',
        }

        session.verify = self.verify_cert_path
        session.timeout = (31, 31)

        return session

    def _proxies(self):
        """Return a proxies dict for requests, or an empty dict if none configured."""
        if not self.proxy_url:
            return {}

        return {'https': self.proxy_url}

    def put(self, artifact_name, *, filename=None, fileobj=None, dict=None):
        """Upload an artifact to the CRC ingress API.

        Accepts *filename*, *fileobj*, and/or *dict*. If multiple are provided,
        each input is uploaded in sequence.

        Args:
            artifact_name: The artifact file name sent as part of the multipart request.
            filename: Path to a local file to upload.
            fileobj: An open file-like object to upload.
            dict: A dict that will be JSON-serialised and uploaded.
        """
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
            raise RuntimeError(f'{self.__class__.__name__}: Upload failed with status {response.status_code}: {response.text}')


class StorageCRC(Base):
    """CRC storage backend that authenticates using service-account OAuth2 credentials."""

    def __init__(self, **settings):
        super().__init__(**settings)

        self.sso_url = settings.get('sso_url', 'https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token')
        self.client_id = settings.get('client_id')
        self.client_secret = settings.get('client_secret')

        if not self.client_id:
            raise ValueError('StorageCRC: client_id not set')

        if not self.client_secret:
            raise ValueError('StorageCRC: client_secret not set')

    def _bearer(self):
        """Obtain an OAuth2 bearer token from the SSO endpoint.

        Returns:
            Bearer token string.
        """
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
        """Send *files* to the ingress URL with a fresh bearer token.

        Args:
            files: Multipart files dict passed to :func:`requests.Session.post`.

        Returns:
            :class:`requests.Response`.
        """
        session = self._session()

        access_token = self._bearer()
        session.headers['authorization'] = f'Bearer {access_token}'

        return session.post(
            self.ingress_url,
            files=files,
            proxies=self._proxies(),
        )


class StorageCRCMutual(Base):
    """CRC storage backend that authenticates using mutual TLS (RHSM certificates)."""

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
