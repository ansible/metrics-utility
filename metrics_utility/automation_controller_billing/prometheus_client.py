from typing import Optional

import requests

from metrics_utility.exceptions import MetricsException
from metrics_utility.logger import logger

from .kubernetes_client import KubernetesClient


class PrometheusClient:
    """
    Prometheus client with Kubernetes service account authentication support.

    This class handles:
    - Service account token retrieval from Kubernetes
    - Prometheus connection management
    - Query execution with proper error handling
    """

    def __init__(self, url: str, use_mounted_token: bool = False, timeout: int = 30):
        """
        Initialize Prometheus client.

        Args:
            url: Prometheus server URL
            token: the token of the service account having permission to access prometheus
        """
        self.url = url.rstrip('/')  # Remove trailing slash
        self.timeout = timeout
        self.token = None
        self.session = requests.Session()

        kube_client = KubernetesClient()
        self.token = kube_client.create_token_for_current_service_account(use_mounted_token)
        if not self.token:
            raise MetricsException('Unable to create the token for the current service account')

        # Create PrometheusConnect client
        self._setup_session()

    # def _format_timestamp(ts: int) -> str:
    #     return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    def _setup_session(self):
        """Setup HTTP session with authentication headers"""
        if self.token:
            logger.info('Creating authenticated Prometheus client')
            logger.info(f'   URL: {self.url}')

            self.session.headers.update({'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/x-www-form-urlencoded'})
        else:
            logger.info('Creating unauthenticated Prometheus client')
            logger.info(f'   URL: {self.url}')

        self.session.cert = ('/etc/tls/tls.crt', '/etc/tls/tls.key')

        # Disable SSL warnings
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session.verify = False

    # def _verify_service_account(self) -> bool:
    #     """
    #     Verify that the configured service account exists.

    #     Returns:
    #         True if service account exists or no service account is configured, False otherwise
    #     """
    #     if not self.service_account_name or not self.k8s_client:
    #         return True  # No service account configured, so nothing to verify

    #     return self.k8s_client.verify_service_account_exists(
    #         self.service_account_name,
    #         self.service_account_namespace
    #     )

    def query(self, query: str, time_param: Optional[float] = None) -> Optional[list]:
        """
        Execute instant PromQL query.

        Args:
            query: PromQL query string
            time_param: Optional timestamp for the query

        Returns:
            Query results as list, or raise MetricsException if failed
        """
        try:
            url = f'{self.url}/api/v1/query'
            params = {'query': query}

            if time_param:
                params['time'] = time_param

            response = self.session.get(url, params=params, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success':
                    return data.get('data', {}).get('result', [])
                else:
                    raise MetricsException(f'Prometheus API error: {data.get("error", "Unknown error")}')
            else:
                raise MetricsException(f'HTTP error {response.status_code}: {response.text}')

        except Exception as e:
            raise MetricsException(f'Query failed: {e}')

    def get_current_value(self, query: str) -> Optional[float]:
        """
        Get current value from an instant query.

        Args:
            query: PromQL query string

        Returns:
            Current value as float, or None if result is empty
        """
        result = self.query(query)
        if result and len(result) > 0:
            return float(result[0]['value'][1])
        return None
