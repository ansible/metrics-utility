import json

from datetime import datetime, timezone
from typing import Tuple

from metrics_utility.logger import logger

from ..util import collector
from .prometheus_client import PrometheusClient


@collector
def total_workers_vcpu(*, cluster_name=None, metering_enabled=False, prometheus_url=None, ca_cert_path=None, token=None):
    now = datetime.now(timezone.utc)
    current_ts = now.timestamp()
    prev_hour_start, prev_hour_end = get_hour_boundaries(current_ts)

    info = {
        'cluster_name': cluster_name,
        'collection_timestamp': datetime.fromtimestamp(current_ts).isoformat(),
        'start_timestamp': datetime.fromtimestamp(prev_hour_start).isoformat(),
        'end_timestamp': datetime.fromtimestamp(prev_hour_end).isoformat(),
        'usage_based_billing_enabled': metering_enabled,
        # total_workers_vcpu
        # promql_query
        # timeline
    }

    if not metering_enabled:
        info['total_workers_vcpu'] = 1

        # This message must always appear in the log regardless of the log level.
        logger.info(json.dumps(info, indent=2))
        return {
            'timestamp': info['end_timestamp'],
            'cluster_name': info['cluster_name'],
            'total_workers_vcpu': info['total_workers_vcpu'],
        }

    prom = PrometheusClient(url=prometheus_url, ca_cert_path=ca_cert_path, token=token)

    total_workers_vcpu_val, promql_query = get_total_workers_cpu(prom, prev_hour_start)
    timeline = get_cpu_timeline(prom, prev_hour_start, prev_hour_end)

    info['promql_query'] = promql_query
    info['timeline'] = timeline

    logger.debug(f'total_workers_vcpu: {total_workers_vcpu_val}')

    # This can happen when the prev_hour_start doesn't have data, it could be when the cluster just started or
    # if for some reasons prometheus loss some data.
    if total_workers_vcpu_val is None:
        logger.warning('No data available yet, the cluster is probably running for less than an hour')
        return None

    info['total_workers_vcpu'] = int(total_workers_vcpu_val)

    # This message must always appear in the log regardless of the log level.
    logger.info(json.dumps(info, indent=2))
    return {
        'timestamp': info['end_timestamp'],
        'cluster_name': info['cluster_name'],
        'total_workers_vcpu': info['total_workers_vcpu'],
    }


def get_hour_boundaries(current_timestamp: float) -> Tuple[float, float]:
    current_hour_start = (current_timestamp // 3600) * 3600

    previous_hour_start = current_hour_start - 3600
    previous_hour_end = current_hour_start - 1

    return (previous_hour_start, previous_hour_end)


def get_total_workers_cpu(prom, base_timestamp: float) -> Tuple[float, str]:
    promql_query = f'max_over_time(sum(machine_cpu_cores)[59m59s:5m] @ {base_timestamp})'
    total_workers_vcpu = prom.get_current_value(promql_query)

    return (total_workers_vcpu, promql_query)


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
                            'timestamp': datetime.fromtimestamp(float(timestamp_val), timezone.utc).isoformat(),
                            'cpu_sum': float(cpu_val),
                        }
                    )

    # Sort by timestamp
    result.sort(key=lambda x: x['timestamp'])
    return result
