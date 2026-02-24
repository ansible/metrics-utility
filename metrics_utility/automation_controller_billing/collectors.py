import json
import os

from django.db import connection
from django.db.utils import ProgrammingError
from django.utils.timezone import now, timedelta

from metrics_utility.automation_controller_billing.helpers import get_last_entries_from_db
from metrics_utility.base import register
from metrics_utility.base.utils import get_max_gather_period_days, get_optional_collectors
from metrics_utility.exceptions import MetricsException, MissingRequiredEnvVar
from metrics_utility.library.collectors.controller import (
    config,
    controller_version_service,
    credentials_service,
    execution_environments,
    job_host_summary,
    job_host_summary_service,
    main_host,
    main_host_daily,
    main_indirectmanagednodeaudit,
    main_jobevent,
    main_jobevent_service,
    table_metadata,
    unified_jobs,
)
from metrics_utility.library.collectors.others import total_workers_vcpu
from metrics_utility.logger import logger


try:
    from psycopg.errors import UndefinedTable
except ImportError:

    class UndefinedTable(Exception):
        pass


"""
This module is used to define metrics collected by the gather_automation_controller_billing_data CLI command.
Each function is decorated with a key name, and should return a data structure that can be serialized to JSON,
or a list of CSV filenames.

    @register('something', '1.0')
    def cli_something(since, until, output):
        # either: the generated archive will contain a `something.json` w/ this JSON
        return output.json({'some': 'json'})
        # or: the generated archive(s) will contain a `something.csv` (numbered when more than one)
        return output.csv(['tempfile.csv', 'tempfile2.csv'])

The since/until arguments are datetimes with timezone,
representing the start (included) and end (excluded) of the desired collection interval.
Some collectors will use them, others will not.
"""


def daily_slicing(key, last_gather, **kwargs):
    since, until = kwargs.get('since', None), kwargs.get('until', now())
    if since is not None:
        last_entry = since
    else:
        horizon = until - timedelta(days=get_max_gather_period_days())
        last_entries = get_last_entries_from_db()
        try:
            last_entry = max(last_entries.get(key) or last_gather, horizon)
        except TypeError:  # last_entries has a stale non-datetime entry for this collector
            last_entry = max(last_gather, horizon)

    start, end = last_entry, None
    start_beginning_of_next_day = start.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    # If the date range is over one day, we want first interval to contain the rest of the day
    # then we'll cycle by full days
    if until > start_beginning_of_next_day:
        yield (start, start_beginning_of_next_day)
        start = start_beginning_of_next_day

    while start < until:
        end = min(start + timedelta(days=1), until)
        yield (start, end)
        start = end


def until_slicing(**kwargs):
    # For tables where we always need to do a table full scan, ignoring since & until
    # Always store the inventory snapshot into the last daily partition (until - 1 day)
    until = kwargs.get('until', now())
    last_day = until - timedelta(days=1)
    yield (last_day, last_day)


def bool_from_env(name, default=None):
    s = os.getenv(name, None)
    if s is None:
        return default

    b = s.lower() in {'1', 'true'}
    return b


### shared collectors


# FIXME: move this one to caller?
@register('config', '1.0', config=True)
def cli_config(_since, _until, output):
    # runs once, used in all the tarballs
    # FIXME: , billing_provider_params={dict} rather than {} getting overwritten in collector.gather
    collector = config(db=connection)
    return output.json(collector.gather())


### CCSP & CCSPv2 collectors


@register('job_host_summary', '1.2', format='csv', fnc_slicing=daily_slicing)
def cli_job_host_summary(since, until, output):
    # enabled by default, disable using METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR=true
    if bool_from_env('METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR'):
        return None

    collector = job_host_summary(db=connection, since=since, until=until)
    return output.csv(collector.gather())


@register('main_host', '1.0', format='csv', fnc_slicing=until_slicing)
def cli_main_host(_since, _until, output):
    if 'main_host' not in get_optional_collectors():
        return None

    collector = main_host(db=connection)
    return output.csv(collector.gather())


@register('main_host_daily', '1.0', format='csv', fnc_slicing=daily_slicing)
def cli_main_host_daily(since, until, output):
    if 'main_host_daily' not in get_optional_collectors():
        return None

    collector = main_host_daily(db=connection, since=since, until=until)
    return output.csv(collector.gather())


@register('main_indirectmanagednodeaudit', '1.0', format='csv', fnc_slicing=daily_slicing)
def cli_main_indirectmanagednodeaudit(since, until, output):
    if 'main_indirectmanagednodeaudit' not in get_optional_collectors():
        return None

    # the table does not exist in 2.4, may be 2.6+
    try:
        collector = main_indirectmanagednodeaudit(db=connection, since=since, until=until)
        return output.csv(collector.gather())
    except (ProgrammingError, UndefinedTable) as e:
        logger.warning(
            'main_indirectmanagednodeaudit table missing in the database schema: %s. '
            'Falling back to behavior without indirect managed node audit data.',
            e,
        )
        return None


@register('main_jobevent', '1.0', format='csv', fnc_slicing=daily_slicing)
def cli_main_jobevent(since, until, output):
    if 'main_jobevent' not in get_optional_collectors():
        return None

    collector = main_jobevent(db=connection, since=since, until=until)
    return output.csv(collector.gather())


### vcpu prometheus collector


@register('total_workers_vcpu', '1.0', format='json', fnc_slicing=until_slicing)
def cli_total_workers_vcpu(since, until, output):
    if 'total_workers_vcpu' not in get_optional_collectors():
        return None

    cluster_name = os.getenv('METRICS_UTILITY_CLUSTER_NAME')
    prometheus_url = os.getenv('METRICS_UTILITY_PROMETHEUS_URL')
    red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID')  # only used for log messages
    metering_enabled = bool_from_env('METRICS_UTILITY_USAGE_BASED_METERING_ENABLED')

    log_prefix = f'[METRICS_UTILITY_VCPU]: cluster_name: {cluster_name}, red_hat_org_id: {red_hat_org_id}'

    def log_error(fmt, *data):
        logger.error(f'%s, {fmt}', log_prefix, *data)

    def log_warning(fmt, *data):
        logger.warning(f'%s, {fmt}', log_prefix, *data)

    def log_info(fmt, *data):
        logger.info(f'%s, {fmt}', log_prefix, *data)

    def log_debug(fmt, *data):
        logger.debug(f'%s, {fmt}', log_prefix, *data)

    if not cluster_name:
        log_error('environment variable METRICS_UTILITY_CLUSTER_NAME is not set')
        raise MissingRequiredEnvVar('environment variable METRICS_UTILITY_CLUSTER_NAME is not set')

    if not prometheus_url:
        prometheus_default_url = 'https://prometheus-k8s.openshift-monitoring.svc.cluster.local:9091'
        log_info('environment variable METRICS_UTILITY_PROMETHEUS_URL is not set, default %s will be assigned', prometheus_default_url)
        prometheus_url = prometheus_default_url

    # only require these when intending to talk to prometheus
    token = None
    ca_cert_path = None
    if metering_enabled:
        token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
        if not os.path.exists(token_path):
            raise MetricsException(f'Service account token not found at {token_path}')

        ca_cert_path = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
        if not os.path.exists(ca_cert_path):
            raise MetricsException(f'CA_CERT not found at {ca_cert_path}')

        with open(token_path, 'r') as f:
            token = f.read().strip()
        if not token:
            raise MetricsException(f'Unable to retrieve the token for the current service account from {token_path}')

    def log_info_data(info, data):
        # This message must always appear in the log regardless of the log level.
        log_info('info: %s', json.dumps(info))
        data = {
            'timestamp': info['end_timestamp'],
            'cluster_name': info['cluster_name'],
            'total_workers_vcpu': info['total_workers_vcpu'],
        }
        log_info('data: %s', json.dumps(data))
        return data

    collector = total_workers_vcpu(
        cluster_name=cluster_name,
        metering_enabled=metering_enabled,
        prometheus_url=prometheus_url,
        ca_cert_path=ca_cert_path,
        token=token,
    )
    info = collector.gather()

    total = info['total_workers_vcpu']
    log_debug('total_workers_vcpu: %s', total)
    if total is None:
        log_warning('No data available yet, the cluster is probably running for less than an hour')
        raise MetricsException('No data available yet, the cluster is probably running for less than an hour')

    return output.json(log_info_data(info))


### anonymized collectors


@register('controller_version_service', '1.4', format='csv', fnc_slicing=until_slicing)
def cli_controller_version_service(_since, _until, output):
    if 'controller_version_service' not in get_optional_collectors():
        return None

    collector = controller_version_service(db=connection)
    return output.csv(collector.gather())


@register('credentials_service', '1.4', format='csv', fnc_slicing=daily_slicing)
def cli_credentials_service(since, until, output):
    if 'credentials_service' not in get_optional_collectors():
        return None

    collector = credentials_service(db=connection, since=since, until=until)
    return output.csv(collector.gather())


@register('execution_environments', '1.4', format='csv', fnc_slicing=until_slicing)
def cli_execution_environments(_since, _until, output):
    if 'execution_environments' not in get_optional_collectors():
        return None

    collector = execution_environments(db=connection)
    return output.csv(collector.gather())


@register('job_host_summary_service', '1.4', format='csv', fnc_slicing=daily_slicing)
def cli_job_host_summary_service(since, until, output):
    if 'job_host_summary_service' not in get_optional_collectors():
        return None

    collector = job_host_summary_service(db=connection, since=since, until=until)
    return output.csv(collector.gather())


@register('main_jobevent_service', '1.4', format='csv', fnc_slicing=daily_slicing)
def cli_main_jobevent_service(since, until, output):
    if 'main_jobevent_service' not in get_optional_collectors():
        return None

    collector = main_jobevent_service(db=connection, since=since, until=until)
    return output.csv(collector.gather())


@register('table_metadata', '1.4', format='csv', fnc_slicing=until_slicing)
def cli_table_metadata(_since, _until, output):
    if 'table_metadata' not in get_optional_collectors():
        return None

    collector = table_metadata(db=connection)
    return output.csv(collector.gather())


@register('unified_jobs', '1.4', format='csv', fnc_slicing=daily_slicing)
def cli_unified_jobs(since, until, output):
    if 'unified_jobs' not in get_optional_collectors():
        return None

    collector = unified_jobs(db=connection, since=since, until=until)
    return output.csv(collector.gather())
