import os

from metrics_utility.exceptions import MetricsException
from metrics_utility.logger import logger


class KubernetesClient:
    """
    Simplified Kubernetes client for service account token operations.

    This class assumes running in a Kubernetes pod with standard service account
    files mounted at /var/run/secrets/kubernetes.io/serviceaccount/
    """

    def __init__(self):
        """Initialize the client and validate service account files are available."""
        self._validate_service_account_files()

    def _validate_service_account_files(self):
        """Validate that required service account files exist."""
        token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
        namespace_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'

        if not os.path.exists(token_path):
            raise MetricsException('Service account token not found at /var/run/secrets/kubernetes.io/serviceaccount/token')

        if not os.path.exists(namespace_path):
            raise MetricsException('Service account namespace not found at /var/run/secrets/kubernetes.io/serviceaccount/namespace')

        logger.info('Service account files validated')

    def get_current_token(self) -> str:
        """
        Get the current pod's service account token from the mounted file.

        Returns:
            Current service account token

        Raises:
            MetricsException: If token cannot be read
        """
        token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'

        try:
            with open(token_path, 'r') as f:
                token = f.read().strip()
            logger.info("Retrieved current pod's mounted token")
            logger.info(f'   Token Length: {len(token)} characters')
            return token
        except Exception as e:
            raise MetricsException(f'Error reading token: {e}')
