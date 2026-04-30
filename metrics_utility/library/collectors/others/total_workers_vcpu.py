"""Collector that queries Prometheus for total worker vCPU usage in the previous hour."""

from datetime import datetime, timezone
from typing import Optional, Tuple

import requests

from ..util import DictOutput, collector


@collector
def total_workers_vcpu(*, cluster_name=None, metering_enabled=False, prometheus_url=None, ca_cert_path=None, token=None, output=DictOutput()):
    """Collect total worker vCPU count for the previous hour from Prometheus.

    When *metering_enabled* is False, returns a synthetic value of 1.
    When enabled, queries Prometheus using a service-account bearer token and
    CA certificate for TLS verification.

    Args:
        cluster_name: Human-readable cluster identifier included in the output.
        metering_enabled: When True, contact Prometheus; when False use a stub value.
        prometheus_url: Base URL of the Prometheus API.
        ca_cert_path: Path to the CA certificate for SSL verification.
        token: Service-account bearer token for Prometheus authentication.
        output: Output adapter (defaults to :class:`~..util.DictOutput`).

    Returns:
        Dict with collection metadata and ``total_workers_vcpu``, or None if
        Prometheus has no data for the previous hour.
    """
    now = datetime.now(timezone.utc)
    current_ts = now.timestamp()
    prev_hour_start, prev_hour_end = get_hour_boundaries(current_ts)

    info = {
        'cluster_name': cluster_name,
        'collection_timestamp': timestamp_format(current_ts),
        'start_timestamp': timestamp_format(prev_hour_start),
        'end_timestamp': timestamp_format(prev_hour_end),
        'usage_based_billing_enabled': metering_enabled,
        # total_workers_vcpu
        # promql_query
        # timeline
    }

    if not metering_enabled:
        info['total_workers_vcpu'] = 1
        return output.dict(info)

    prom = PrometheusClient(url=prometheus_url, ca_cert_path=ca_cert_path, token=token)

    total_workers_vcpu_val, promql_query = get_total_workers_cpu(prom, prev_hour_start)
    timeline = get_cpu_timeline(prom, prev_hour_start, prev_hour_end)

    info['promql_query'] = promql_query
    info['timeline'] = timeline

    # None can happen when the prev_hour_start doesn't have data, could be the cluster just started
    # return None, the cli raises an exception
    if total_workers_vcpu_val is None:
        return None

    info['total_workers_vcpu'] = int(total_workers_vcpu_val)
    return output.dict(info)


def get_hour_boundaries(current_timestamp: float) -> Tuple[float, float]:
    """Return the start and end timestamps of the hour preceding *current_timestamp*.

    Args:
        current_timestamp: Unix timestamp representing "now".

    Returns:
        Tuple of ``(previous_hour_start, previous_hour_end)`` as floats.
    """
    current_hour_start = (current_timestamp // 3600) * 3600

    previous_hour_start = current_hour_start - 3600
    previous_hour_end = current_hour_start - 0.001  # End at .999 milliseconds

    return (previous_hour_start, previous_hour_end)


def get_total_workers_cpu(prom, base_timestamp: float) -> Tuple[float, str]:
    """Query Prometheus for the maximum total CPU cores during the previous hour.

    Args:
        prom: :class:`PrometheusClient` instance.
        base_timestamp: Start of the previous hour as a Unix timestamp.

    Returns:
        Tuple of ``(total_vcpu_value, promql_query_string)``.
    """
    promql_query = f'max_over_time(sum(machine_cpu_cores)[59m59s999ms:5m] @ {base_timestamp})'
    total_workers_vcpu = prom.get_current_value(promql_query)

    return (total_workers_vcpu, promql_query)


def timestamp_format(timestamp_val):
    """Format a Unix timestamp as an ISO 8601 UTC string with millisecond precision.

    Args:
        timestamp_val: Unix timestamp (float or int).

    Returns:
        String like ``'2024-01-15T14:00:00.000Z'``.
    """
    return datetime.fromtimestamp(timestamp_val, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def get_cpu_timeline(prom, previous_hour_start, previous_hour_end: float) -> list:
    """
    Get array of timestamp/CPU pairs for the hour leading up to previous_hour_end
    Returns:
        List of dicts with 'timestamp' (ISO format) and 'cpu_sum' keys
    """
    # Use instant query - query_range will handle the time range
    query = 'sum(machine_cpu_cores)'

    response = prom.query_range(query=query, start_time=previous_hour_start, end_time=previous_hour_end, step='5m')

    result = []
    if response and 'data' in response and 'result' in response['data']:
        for series in response['data']['result']:
            if 'values' in series:
                for timestamp_val, cpu_val in series['values']:
                    result.append(
                        {
                            'timestamp': timestamp_format(float(timestamp_val)),
                            'cpu_sum': float(cpu_val),
                        }
                    )

    # Sort by timestamp
    result.sort(key=lambda x: x['timestamp'])
    return result


class PrometheusClient:
    """
    Prometheus client with Kubernetes service account authentication support.
    """

    def __init__(self, url: str, timeout: int = 30, token=None, ca_cert_path=None):
        """Initialise the Prometheus client.

        Args:
            url: Base URL of the Prometheus API (trailing slashes are stripped).
            timeout: HTTP request timeout in seconds (default 30).
            token: Optional Bearer token for authentication.
            ca_cert_path: Optional path to a CA certificate file for TLS
                verification; if provided, overrides the default trust store.
        """
        self.url = url.rstrip('/')  # no trailing slash
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/x-www-form-urlencoded'})

        if token:
            self.session.headers.update({'Authorization': f'Bearer {token}'})

        if ca_cert_path:
            # Use service CA certificate for SSL verification
            self.session.verify = ca_cert_path

    def _get(self, url, params):
        """Perform a GET request and return the parsed JSON response.

        Args:
            url: Full URL to request.
            params: Query parameters dict.

        Returns:
            Parsed JSON dict.

        Raises:
            Exception: On non-200 HTTP status or a Prometheus API error.
        """
        response = self.session.get(url, params=params, timeout=self.timeout)
        if response.status_code != 200:
            raise Exception(f'HTTP error {response.status_code}: {response.text}')

        data = response.json()
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

        if time_param is not None:
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
