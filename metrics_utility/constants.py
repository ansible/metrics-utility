"""Named constants and environment-variable-tunable defaults for metrics-utility.

Hardcoded "magic values" have been extracted here so they are easy to find,
document, and override without touching business logic.  Tunable values are
read from environment variables at import time so that operator deployments can
adjust them without code changes.
"""

import os


# ---------------------------------------------------------------------------
# TLS / Certificate paths
# ---------------------------------------------------------------------------

#: System CA bundle used when verifying HTTPS connections to Red Hat services.
#: Override via the ``METRICS_UTILITY_CA_BUNDLE_PATH`` environment variable.
CA_BUNDLE_PATH: str = os.getenv(
    "METRICS_UTILITY_CA_BUNDLE_PATH",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
)

# ---------------------------------------------------------------------------
# HTTP timeouts (seconds)
# ---------------------------------------------------------------------------

#: ``(connect_timeout, read_timeout)`` tuple used for outbound HTTP requests.
#: Override the connect timeout via ``METRICS_UTILITY_HTTP_CONNECT_TIMEOUT``
#: and the read timeout via ``METRICS_UTILITY_HTTP_READ_TIMEOUT``.
_http_connect_timeout: int = int(os.getenv("METRICS_UTILITY_HTTP_CONNECT_TIMEOUT", "31"))
_http_read_timeout: int = int(os.getenv("METRICS_UTILITY_HTTP_READ_TIMEOUT", "31"))
HTTP_TIMEOUT: tuple = (_http_connect_timeout, _http_read_timeout)

# ---------------------------------------------------------------------------
# Upload / file-size limits
# ---------------------------------------------------------------------------

#: Maximum payload size (bytes) before a collection is split into smaller
#: chunks.  Defaults to 200 MiB (the upload limit is 100 MiB; 50 % compression
#: is assumed).  Override via ``METRICS_UTILITY_MAX_DATA_SIZE_MB`` (in MiB).
_max_data_size_mb: int = int(os.getenv("METRICS_UTILITY_MAX_DATA_SIZE_MB", "200"))
MAX_DATA_SIZE: int = _max_data_size_mb * 1048576

# ---------------------------------------------------------------------------
# AWX / Automation Controller path
# ---------------------------------------------------------------------------

#: Filesystem path where the AWX Python package can be found when it is not
#: already on ``sys.path``.  Override via the ``AWX_PATH`` environment
#: variable.
DEFAULT_AWX_PATH: str = os.getenv("AWX_PATH", "/awx_devel")

# ---------------------------------------------------------------------------
# Red Hat SSO and CRC Ingress URLs
# ---------------------------------------------------------------------------

#: OAuth2 token endpoint used to obtain a bearer token for CRC uploads.
#: Override via ``METRICS_UTILITY_CRC_SSO_URL``.
DEFAULT_CRC_SSO_URL: str = os.getenv(
    "METRICS_UTILITY_CRC_SSO_URL",
    "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token",
)

#: CRC Ingress API endpoint where billing payloads are uploaded.
#: Override via ``METRICS_UTILITY_CRC_INGRESS_URL``.
DEFAULT_CRC_INGRESS_URL: str = os.getenv(
    "METRICS_UTILITY_CRC_INGRESS_URL",
    "https://console.redhat.com/api/ingress/v1/upload",
)
