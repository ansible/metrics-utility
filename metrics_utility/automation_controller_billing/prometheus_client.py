import os

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

    def __init__(self, url: str, timeout: int = 30):
        """
        Initialize Prometheus client.

        Args:
            url: Prometheus server URL
            timeout: Request timeout in seconds (default: 30)
        """
        self.url = url.rstrip('/')  # Remove trailing slash
        self.timeout = timeout
        self.token = None
        self.ca_cert_path = None
        self.session = requests.Session()

        kube_client = KubernetesClient()
        self.token = kube_client.get_current_token()
        if not self.token:
            raise MetricsException('Unable to retrieve the token for the current service account')

        self.ca_cert_path = kube_client.get_ca_cert_path()

        # Setup session
        self._setup_session()

    def _setup_session(self):
        """Setup HTTP session with authentication headers and CA certificate"""
        if self.token:
            logger.info('Creating authenticated Prometheus client')
            logger.info(f'   URL: {self.url}')

            self.session.headers.update({'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/x-www-form-urlencoded'})
        else:
            logger.info('Creating unauthenticated Prometheus client')
            logger.info(f'   URL: {self.url}')

        # Use service CA certificate for SSL verification
        if os.path.exists(self.ca_cert_path):
            self.session.verify = self.ca_cert_path
            logger.info(f'Using service CA certificate: {self.ca_cert_path}')
        else:
            raise MetricsException(f'CA_CERT not found at {self.ca_cert_path}')

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

            logger.debug(f'response: {response}')
            if response.status_code == 200:
                data = response.json()
                logger.debug(f'data: {data}')
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

    def query_range(self, query: str, start_time: float, end_time: float, step: str = '5m') -> Optional[dict]:
        """
        Execute a range query against Prometheus.
        Args:
            query: PromQL instant query (not range query)
            start_time: Start time (Unix timestamp)
            end_time: End time (Unix timestamp)
            step: Query resolution step (e.g., '1m', '5m')
        """
        params = {'query': query, 'start': start_time, 'end': end_time, 'step': step}

        try:
            url = f'{self.url}/api/v1/query_range'
            logger.debug(f'Range query URL: {url}')
            logger.debug(f'Range query params: {params}')

            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            if data.get('status') == 'success':
                return data
            else:
                logger.error(f'Prometheus range query failed: {data.get("error", "Unknown error")}')
                return None

        except Exception as e:
            logger.error(f'Range query failed: {e}')
            raise MetricsException(e)
