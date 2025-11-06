import os

from django.db import connection
from django.db.utils import ProgrammingError
from django.utils.timezone import now, timedelta
from django.utils.translation import gettext_lazy as _

from metrics_utility.automation_controller_billing.helpers import get_last_entries_from_db
from metrics_utility.base import register
from metrics_utility.base.utils import get_max_gather_period_days, get_optional_collectors
from metrics_utility.exceptions import MetricsException, MissingRequiredEnvVar
from metrics_utility.library.collectors import config as config_collector
from metrics_utility.library.collectors import execution_environments as execution_environments_collector
from metrics_utility.library.collectors import job_host_summary as job_host_summary_collector
from metrics_utility.library.collectors import job_host_summary_service as job_host_summary_service_collector
from metrics_utility.library.collectors import main_host as main_host_collector
from metrics_utility.library.collectors import main_indirectmanagednodeaudit as main_indirectmanagednodeaudit_collector
from metrics_utility.library.collectors import main_jobevent as main_jobevent_collector
from metrics_utility.library.collectors import main_jobevent_service as main_jobevent_service_collector
from metrics_utility.library.collectors import total_workers_vcpu as total_workers_vcpu_collector
from metrics_utility.library.collectors import unified_jobs as unified_jobs_collector
from metrics_utility.logger import logger


try:
    from psycopg.errors import UndefinedTable
except ImportError:

    class UndefinedTable(Exception):
        pass


"""
This module is used to define metrics collected by
gather_automation_controller_billing_data command. Each function is
decorated with a key name, and should return a data structure that
can be serialized to JSON.

@register('something', '1.0')
def something(since):
    # the generated archive will contain a `something.json` w/ this JSON
    return {'some': 'json'}

All functions - when called - will be passed a datetime.datetime object,
`since`, which represents the last time analytics were gathered (some metrics
functions - like those that return metadata about playbook runs, may return
data _since_ the last report date - i.e., new data in the last 24 hours)
"""


def daily_slicing(key, last_gather=None, **kwargs):
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


def limit_slicing(key, **kwargs):
    # For tables where we always need to do a table full scan, we want to load batches

    # TODO: skip today's collection if it already happened, so we don't load full inventory
    # every collection, which can be e.g. every 10 minutes. If the last_gather_was from today
    # we should be able to skip the collection.

    # For now, we'll always store the inventory snapshot into daily partition.
    # It's not possible to collect historical state of inventory, so we always insert it
    # into a daily partition of now.
    today = now().replace(hour=0, minute=0, second=0, microsecond=0)

    # TODO: we should load day in batches of i.e. 100k nodes, just doing marker based pagination based
    # on primary key
    yield (today, today)


def until_slicing(key, **kwargs):
    # exactly like limit_slicing, but uses the `until` timestamp, or now
    # TODO: just replace limit_slicing?
    until = kwargs.get('until', now())
    yield (until, until)


@register('config', '1.1', description=_('General platform configuration.'), config=True)
def config(billing_provider_params, **kwargs):
    return config_collector(db=connection, billing_provider_params=billing_provider_params).gather()


@register('job_host_summary', '1.2', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def job_host_summary_table(since, until, output_dir=None, **kwargs):
    disable = os.getenv('METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR', 'false').lower()
    if disable == 'true':
        return None

    return job_host_summary_collector(db=connection, since=since, until=until, output_dir=output_dir).gather()


@register('main_jobevent', '1.0', format='csv', description=_('Content usage'), fnc_slicing=daily_slicing)
def main_jobevent_table(since, until, output_dir=None, **kwargs):
    if 'main_jobevent' not in get_optional_collectors():
        return None

    return main_jobevent_collector(db=connection, since=since, until=until, output_dir=output_dir).gather()


@register('main_indirectmanagednodeaudit', '1.0', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def main_indirectmanagednodeaudit_table(since, until, output_dir=None, **kwargs):
    if 'main_indirectmanagednodeaudit' not in get_optional_collectors():
        return None

    try:
        return main_indirectmanagednodeaudit_collector(db=connection, since=since, until=until, output_dir=output_dir).gather()
    except (ProgrammingError, UndefinedTable) as e:
        logger.warning(
            'main_indirectmanagednodeaudit table missing in the database schema: %s. '
            'Falling back to behavior without indirect managed node audit data.',
            e,
        )
        return None


@register('main_host', '1.0', format='csv', description=_('Inventory data'), fnc_slicing=limit_slicing)
def main_host_table(output_dir=None, **kwargs):
    if 'main_host' not in get_optional_collectors():
        return None

    return main_host_collector(db=connection, output_dir=output_dir).gather()


@register('total_workers_vcpu', '1.0', format='json', description=_('Total workers vCPU'), fnc_slicing=limit_slicing)
def total_workers_vcpu(**kwargs):
    if 'total_workers_vcpu' not in get_optional_collectors():
        return None

    # If METRICS_UTILITY_USAGE_BASED_METERING_ENABLED is not set or set to false then it returns 1
    usage_based_billing_enabled_str = os.getenv('METRICS_UTILITY_USAGE_BASED_METERING_ENABLED')
    metering_enabled = bool(usage_based_billing_enabled_str and (usage_based_billing_enabled_str.lower() == 'true'))

    cluster_name = os.getenv('METRICS_UTILITY_CLUSTER_NAME')
    if not cluster_name:
        logger.error('environment variable METRICS_UTILITY_CLUSTER_NAME is not set')
        raise MissingRequiredEnvVar('environment variable METRICS_UTILITY_CLUSTER_NAME is not set')

    prometheus_url = os.getenv('METRICS_UTILITY_PROMETHEUS_URL')
    if not prometheus_url:
        prometheus_url = 'https://prometheus-k8s.openshift-monitoring.svc.cluster.local:9091'
        logger.info(f'environment variable METRICS_UTILITY_PROMETHEUS_URL is not set, default {prometheus_url} will be assigned')

    token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
    if not os.path.exists(token_path):
        raise MetricsException(f'Service account token not found at {token_path}')

    ca_cert_path = '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
    if not os.path.exists(ca_cert_path):
        raise MetricsException(f'CA_CERT not found at {ca_cert_path}')

    token = None
    with open(token_path, 'r') as f:
        token = f.read().strip()
    if not token:
        raise MetricsException(f'Unable to retrieve the token for the current service account from {token_path}')

    return total_workers_vcpu_collector(
        ca_cert_path=ca_cert_path,
        cluster_name=cluster_name,
        metering_enabled=metering_enabled,
        prometheus_url=prometheus_url,
        token=token,
    ).gather()


@register('unified_jobs', '1.4', format='csv', description=_('Data on jobs run'), fnc_slicing=daily_slicing)
def unified_jobs_table(since, until, output_dir=None, **kwargs):
    if 'unified_jobs' not in get_optional_collectors():
        return None

    return unified_jobs_collector(db=connection, since=since, until=until, output_dir=output_dir).gather()


@register('job_host_summary_service', '1.4', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def job_host_summary_service_table(since, until, output_dir=None, **kwargs):
    if 'job_host_summary_service' not in get_optional_collectors():
        return None

    return job_host_summary_service_collector(db=connection, since=since, until=until, output_dir=output_dir).gather()


@register('main_jobevent_service', '1.4', format='csv', description=_('Content usage'), fnc_slicing=daily_slicing)
def main_jobevent_service_table(since, until, output_dir=None, **kwargs):
    if 'main_jobevent_service' not in get_optional_collectors():
        return None

    return main_jobevent_service_collector(db=connection, since=since, until=until, output_dir=output_dir).gather()


@register('execution_environments', '1.4', format='csv', description=_('Execution environments'), fnc_slicing=until_slicing)
def execution_environments_table(output_dir=None, **kwargs):
    if 'execution_environments' not in get_optional_collectors():
        return None

    return execution_environments_collector(db=connection, output_dir=output_dir).gather()
