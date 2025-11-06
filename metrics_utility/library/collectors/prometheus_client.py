from typing import Optional

import requests

from metrics_utility.logger import logger


class PrometheusClient:
    """
    Prometheus client with Kubernetes service account authentication support.

    This class handles:
    - Prometheus connection management
    - Query execution with proper error handling
    """

    def __init__(self, url: str, timeout: int = 30, token=None, ca_cert_path=None):
        """
        Initialize Prometheus client.

        Args:
            url: Prometheus server URL
            timeout: Request timeout in seconds (default: 30)
        """
        self.url = url.rstrip('/')  # Remove trailing slash
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/x-www-form-urlencoded'})

        if ca_cert_path:
            # Use service CA certificate for SSL verification
            self.session.verify = ca_cert_path
            logger.info(f'Using service CA certificate: {ca_cert_path}')

        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})
            logger.info('Creating authenticated Prometheus client')
        else:
            logger.info('Creating unauthenticated Prometheus client')

        logger.info(f'   URL: {self.url}')

    def _get(self, url, params):
        logger.debug(f'GET URL: {url}')
        logger.debug(f'params: {params}')

        response = self.session.get(url, params=params, timeout=self.timeout)
        logger.debug(f'response: {response}')
        if response.status_code != 200:
            raise Exception(f'HTTP error {response.status_code}: {response.text}')

        data = response.json()
        logger.debug(f'data: {data}')
        if data.get('status') != 'success':
            raise Exception(f'Prometheus API error: {data.get("error", "Unknown error")}')

        return data

    def query(self, query: str, time_param: Optional[float] = None) -> Optional[list]:
        """
        Execute instant PromQL query.

        Args:
            query: PromQL query string
            time_param: Optional timestamp for the query

        Returns:
            Query results as list, or raise exception if failed
        """
        url = f'{self.url}/api/v1/query'
        params = {'query': query}

        if time_param:
            params['time'] = time_param

        return self._get(url, params).get('data', {}).get('result', [])

    def query_range(self, query: str, start_time: float, end_time: float, step: str = '5m') -> Optional[dict]:
        """
        Execute a range query against Prometheus.
        Args:
            query: PromQL instant query (not range query)
            start_time: Start time (Unix timestamp)
            end_time: End time (Unix timestamp)
            step: Query resolution step (e.g., '1m', '5m')
        """
        url = f'{self.url}/api/v1/query_range'
        params = {'query': query, 'start': start_time, 'end': end_time, 'step': step}

        return self._get(url, params)

    def get_current_value(self, query: str) -> Optional[float]:
        """
        Get current value from an instant query.

        Args:
            query: PromQL query string

        Returns:
            Current value as float, or None if result is empty
        """
        result = self.query(query)
        if not result:
            return None

        return float(result[0]['value'][1])
