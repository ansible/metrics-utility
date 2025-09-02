import json
import os
import os.path
import platform

from datetime import datetime, timezone
from importlib.metadata import version
from typing import Tuple

import distro

from django.db import connection
from django.db.utils import ProgrammingError
from django.utils.timezone import now, timedelta
from django.utils.translation import gettext_lazy as _

from metrics_utility.automation_controller_billing.helpers import (
    get_config_and_settings_from_db,
    get_controller_version_from_db,
    get_last_entries_from_db,
)
from metrics_utility.base import register
from metrics_utility.base.utils import get_max_gather_period_days, get_optional_collectors
from metrics_utility.exceptions import MetricsException, MissingRequiredEnvVar
from metrics_utility.library import CsvFileSplitter
from metrics_utility.library.collectors.util import date_where
from metrics_utility.logger import logger, logger_info_level

from .prometheus_client import PrometheusClient


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


def limit_slicing(key, last_gather, **kwargs):
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


def get_install_type():
    if os.getenv('container') == 'oci':
        return 'openshift'

    if os.getenv('KUBERNETES_SERVICE_PORT'):
        return 'k8s'

    return 'traditional'


@register('config', '1.0', description=_('General platform configuration.'), config=True)
def config(since, **kwargs):
    license_info, settings_info = get_config_and_settings_from_db()
    return {
        'platform': {
            'system': platform.system(),
            'dist': distro.linux_distribution(),
            'release': platform.release(),
            'type': get_install_type(),
        },
        'install_uuid': settings_info.get('install_uuid'),
        'instance_uuid': settings_info.get('system_uuid', '00000000-0000-0000-0000-000000000000'),
        'controller_url_base': settings_info.get('tower_url_base'),
        'controller_version': get_controller_version_from_db(),
        'license_type': license_info.get('license_type', 'UNLICENSED'),
        'license_date': license_info.get('license_date'),
        'subscription_name': license_info.get('subscription_name', ''),
        'sku': license_info.get('sku'),
        'support_level': license_info.get('support_level'),
        'usage': license_info.get('usage'),
        'product_name': license_info.get('product_name'),
        'valid_key': license_info.get('valid_key'),
        'satellite': license_info.get('satellite'),
        'pool_id': license_info.get('pool_id'),
        'subscription_id': license_info.get('subscription_id'),
        'account_number': license_info.get('account_number'),
        'current_instances': license_info.get('current_instances'),
        'automated_instances': license_info.get('automated_instances'),
        'automated_since': license_info.get('automated_since'),
        'trial': license_info.get('trial'),
        'grace_period_remaining': license_info.get('grace_period_remaining'),
        'compliant': license_info.get('compliant'),
        'date_warning': license_info.get('date_warning'),
        'date_expired': license_info.get('date_expired'),
        'subscription_usage_model': settings_info.get('subscription_usage_model', ''),  # 1.5+
        'free_instances': license_info.get('free_instances', 0),
        'total_licensed_instances': license_info.get('instance_count', 0),
        'license_expiry': license_info.get('time_remaining', 0),
        'pendo_tracking': settings_info.get('pendo_tracking_state', ''),
        'authentication_backends': settings_info.get('authentication_backends', ''),
        'logging_aggregators': settings_info.get('log_aggregator_loggers', ''),
        'external_logger_enabled': settings_info.get('log_aggregator_enabled', False),
        'external_logger_type': settings_info.get('log_aggregator_type', None),
        'metrics_utility_version': version('metrics-utility'),  # version from setup.cfg
        'billing_provider_params': {},  # Is being overwritten in collector.gather by set ENV VARS
    }


def _copy_table(table, query, path, prepend_query=None):
    file_path = os.path.join(path, table + '_table.csv')
    file = CsvFileSplitter(filespec=file_path)

    with connection.cursor() as cursor:
        if prepend_query:
            cursor.execute(prepend_query)

        if hasattr(cursor, 'copy_expert') and callable(cursor.copy_expert):
            _copy_table_aap_2_4_and_below(cursor, query, file)
        else:
            _copy_table_aap_2_5_and_above(cursor, query, file)

    return file.file_list(keep_empty=True)


def _copy_table_aap_2_4_and_below(cursor, query, file):
    # Automation Controller 4.4 and below use psycopg2 with .copy_expert() method
    cursor.copy_expert(query, file)


def _copy_table_aap_2_5_and_above(cursor, query, file):
    # Automation Controller 4.5 and above use psycopg3 with .copy() method
    with cursor.copy(query) as copy:
        while data := copy.read():
            byte_data = bytes(data)
            file.write(byte_data.decode())


def yaml_and_json_parsing_functions():
    query = """
            -- Define function for parsing field out of yaml encoded as text
            CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field(
                str text,
                field text
            )
            RETURNS text AS
            $$
            DECLARE
                line_re text;
                field_re text;
            BEGIN
                field_re := ' *[:=] *(.+?) *$';
                line_re := '(?n)^' || field || field_re;
                RETURN trim(both '"' from substring(str from line_re) );
            END;
            $$
            LANGUAGE plpgsql;

            -- Define function to check if field is a valid json
            CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json(p_json text)
                returns boolean
            AS
            $$
            BEGIN
                RETURN (p_json::json is not null);
            EXCEPTION
                WHEN others THEN
                    RETURN false;
            END;
            $$
            LANGUAGE plpgsql;
        """
    return query


@register('job_host_summary', '1.2', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def job_host_summary_table(since, full_path, until, **kwargs):
    disable_job_host_summary_str = os.getenv('METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR', 'false')
    disable_job_host_summary = False
    if disable_job_host_summary_str and (disable_job_host_summary_str.lower() == 'true'):
        disable_job_host_summary = True

    if disable_job_host_summary:
        return None

    # TODO: controler needs to have an index on main_jobhostsummary.modified
    prepend_query = """
        -- Define function for parsing field out of yaml encoded as text
        CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field(
            str text,
            field text
        )
        RETURNS text AS
        $$
        DECLARE
            line_re text;
            field_re text;
        BEGIN
            field_re := ' *[:=] *(.+?) *$';
            line_re := '(?n)^' || field || field_re;
            RETURN trim(both '"' from substring(str from line_re) );
        END;
        $$
        LANGUAGE plpgsql;

        -- Define function to check if field is a valid json
        CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json(p_json text)
            returns boolean
        AS
        $$
        BEGIN
            RETURN (p_json::json is not null);
        EXCEPTION
            WHEN others THEN
                RETURN false;
        END;
        $$
        LANGUAGE plpgsql;
    """
    query = f"""
    WITH
    filtered_hosts AS (
        SELECT DISTINCT main_jobhostsummary.host_id
        FROM main_jobhostsummary
        WHERE (main_jobhostsummary.modified >= '{since.isoformat()}'
        AND main_jobhostsummary.modified < '{until.isoformat()}')
    ),
    hosts_variables as (
        SELECT
            filtered_hosts.host_id,
            CASE
                WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_host'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                END AS ansible_host_variable,
                CASE
                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_connection'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                END AS ansible_connection_variable

        FROM filtered_hosts
        LEFT JOIN main_host ON main_host.id = filtered_hosts.host_id)

        SELECT main_jobhostsummary.id,
            main_jobhostsummary.created,
            main_jobhostsummary.modified,
            main_jobhostsummary.host_name,
            main_jobhostsummary.host_id as host_remote_id,
            hosts_variables.ansible_host_variable,
            hosts_variables.ansible_connection_variable,
            main_jobhostsummary.changed,
            main_jobhostsummary.dark,
            main_jobhostsummary.failures,
            main_jobhostsummary.ok,
            main_jobhostsummary.processed,
            main_jobhostsummary.skipped,
            main_jobhostsummary.failed,
            main_jobhostsummary.ignored,
            main_jobhostsummary.rescued,
            main_unifiedjob.created AS job_created,
            main_jobhostsummary.job_id AS job_remote_id,
            main_unifiedjob.unified_job_template_id AS job_template_remote_id,
            main_unifiedjob.name AS job_template_name,
            main_inventory.id AS inventory_remote_id,
            main_inventory.name AS inventory_name,
            main_organization.id AS organization_remote_id,
            main_organization.name AS organization_name,
            main_unifiedjobtemplate_project.id AS project_remote_id,
            main_unifiedjobtemplate_project.name AS project_name
            FROM main_jobhostsummary
            -- connect to main_job, that has connections into inventory and project
            LEFT JOIN main_job ON main_jobhostsummary.job_id = main_job.unifiedjob_ptr_id
            -- get project name from project_options
            LEFT JOIN main_unifiedjobtemplate AS main_unifiedjobtemplate_project ON main_unifiedjobtemplate_project.id = main_job.project_id
            -- get inventory name from main_inventory
            LEFT JOIN main_inventory ON main_inventory.id = main_job.inventory_id
            -- get job name from main_unifiedjob
            LEFT JOIN main_unifiedjob ON main_unifiedjob.id = main_jobhostsummary.job_id
            -- get organization name from main_organization
            LEFT JOIN main_organization ON main_organization.id = main_unifiedjob.organization_id
            -- get variables from precomputed hosts_variables
            LEFT JOIN hosts_variables ON hosts_variables.host_id = main_jobhostsummary.host_id
            WHERE (main_jobhostsummary.modified >= '{since.isoformat()}'
            AND main_jobhostsummary.modified < '{until.isoformat()}')
            ORDER BY main_jobhostsummary.modified ASC
    """

    return _copy_table(table='main_jobhostsummary', query=f'COPY ({query}) TO STDOUT WITH CSV HEADER', path=full_path, prepend_query=prepend_query)


@register('main_jobevent', '1.0', format='csv', description=_('Content usage'), fnc_slicing=daily_slicing)
def main_jobevent_table(since, full_path, until, **kwargs):
    if 'main_jobevent' not in get_optional_collectors():
        return None

    tbl = 'main_jobevent'
    event_data = rf"replace({tbl}.event_data, '\u', '\u005cu')::jsonb"

    query = f"""
        WITH job_scope AS (
            SELECT main_jobhostsummary.id AS main_jobhostsummary_id,
                   main_jobhostsummary.created AS main_jobhostsummary_created,
                   main_jobhostsummary.modified AS main_jobhostsummary_modified,
                   main_unifiedjob.created AS job_created,
                   main_jobhostsummary.job_id AS job_id,
                   main_jobhostsummary.host_name
            FROM main_jobhostsummary
            JOIN main_unifiedjob ON main_unifiedjob.id = main_jobhostsummary.job_id
            WHERE (main_jobhostsummary.modified >= '{since.isoformat()}' AND main_jobhostsummary.modified < '{until.isoformat()}')
        )
        SELECT
            job_scope.main_jobhostsummary_id,
            job_scope.main_jobhostsummary_created,
            {tbl}.id,
            {tbl}.created,
            {tbl}.modified,
            {tbl}.job_created as job_created,
            {tbl}.event,
            ({event_data}->>'task_action')::TEXT AS task_action,
            ({event_data}->>'resolved_action')::TEXT AS resolved_action,
            ({event_data}->>'resolved_role')::TEXT AS resolved_role,
            ({event_data}->>'duration')::TEXT AS duration,
            {tbl}.failed,
            {tbl}.changed,
            {tbl}.playbook,
            {tbl}.play,
            {tbl}.task,
            {tbl}.role,
            {tbl}.job_id as job_remote_id,
            {tbl}.host_id as host_remote_id,
            {tbl}.host_name

        FROM {tbl}
        JOIN job_scope ON job_scope.job_created = {tbl}.job_created AND job_scope.job_id={tbl}.job_id AND job_scope.host_name={tbl}.host_name
        WHERE {tbl}.event IN ('runner_on_ok',
                              'runner_on_failed',
                              'runner_on_unreachable',
                              'runner_on_skipped',
                              'runner_retry',
                              'runner_on_async_ok',
                              'runner_item_on_ok',
                              'runner_item_on_failed',
                              'runner_item_on_skipped')
        """
    return _copy_table(table=tbl, query=f'COPY ({query}) TO STDOUT WITH CSV HEADER', path=full_path)


@register('main_indirectmanagednodeaudit', '1.0', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def main_indirectmanagednodeaudit_table(since, full_path, until, **kwargs):
    if 'main_indirectmanagednodeaudit' not in get_optional_collectors():
        return None

    try:
        query = f"""
            (
                SELECT
                    main_indirectmanagednodeaudit.id,
                    main_indirectmanagednodeaudit.created as created,
                    main_indirectmanagednodeaudit.name as host_name,
                    main_indirectmanagednodeaudit.host_id AS host_remote_id,
                    main_indirectmanagednodeaudit.canonical_facts,
                    main_indirectmanagednodeaudit.facts,
                    main_indirectmanagednodeaudit.events,
                    main_indirectmanagednodeaudit.count as task_runs,
                    main_unifiedjob.created AS job_created,
                    main_indirectmanagednodeaudit.job_id AS job_remote_id,
                    main_unifiedjob.unified_job_template_id AS job_template_remote_id,
                    main_unifiedjob.name AS job_template_name,
                    main_inventory.id AS inventory_remote_id,
                    main_inventory.name AS inventory_name,
                    main_organization.id AS organization_remote_id,
                    main_organization.name AS organization_name,
                    main_unifiedjobtemplate_project.id AS project_remote_id,
                    main_unifiedjobtemplate_project.name AS project_name
                FROM main_indirectmanagednodeaudit
                LEFT JOIN main_job
                    ON main_job.unifiedjob_ptr_id = main_indirectmanagednodeaudit.job_id
                LEFT JOIN main_unifiedjob
                    ON main_unifiedjob.id = main_indirectmanagednodeaudit.job_id
                LEFT JOIN main_inventory
                    ON main_inventory.id = main_indirectmanagednodeaudit.inventory_id
                LEFT JOIN main_organization
                    ON main_organization.id = main_unifiedjob.organization_id
                LEFT JOIN main_unifiedjobtemplate AS main_unifiedjobtemplate_project
                    ON main_unifiedjobtemplate_project.id = main_job.project_id
                WHERE (main_indirectmanagednodeaudit.created >= '{since.isoformat()}'
                AND  main_indirectmanagednodeaudit.created < '{until.isoformat()}')
                ORDER BY main_indirectmanagednodeaudit.created ASC
            )
            """

        return _copy_table(
            table='main_indirectmanagednodeaudit',
            query=f'COPY ({query}) TO STDOUT WITH CSV HEADER',
            path=full_path,
        )
    except (ProgrammingError, UndefinedTable) as e:
        logger.warning(
            'main_indirectmanagednodeaudit table missing in the database schema: %s.'
            ' Falling back to behavior without indirect managed node audit data.',
            e,
        )
        return None


# shared function, not a standalone collector
def _main_host_query(where):
    return f"""
        SELECT main_host.name as host_name,
                main_host.id AS host_id,
                main_inventory.id AS inventory_remote_id,
                main_inventory.name AS inventory_name,
                main_organization.id AS organization_remote_id,
                main_organization.name AS organization_name,
                main_unifiedjob.created AS last_automation,

                CASE
                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_host'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                END AS ansible_host_variable,

                jsonb_build_object(
                    'ansible_product_serial', main_host.ansible_facts->>'ansible_product_serial'::TEXT,
                    'ansible_machine_id', main_host.ansible_facts->>'ansible_machine_id'::TEXT,
                    'ansible_host',
                    CASE
                        WHEN (metrics_utility_is_valid_json(main_host.variables))
                            THEN main_host.variables::jsonb->>'ansible_host'
                        ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                    END,
                    'host_name', main_host.name,
                    'ansible_port',
                    CASE
                        WHEN (
                            CASE
                                WHEN (metrics_utility_is_valid_json(main_host.variables))
                                    THEN main_host.variables::jsonb->>'ansible_port'
                                ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_port' )
                            END
                        ) ~ '^[0-9]+$' THEN
                            (
                                CASE
                                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                                        THEN main_host.variables::jsonb->>'ansible_port'
                                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_port' )
                                END
                            )::INTEGER
                        ELSE NULL
                    END
                ) AS canonical_facts,

                jsonb_build_object(
                    'ansible_connection_variable',
                    CASE
                        WHEN (metrics_utility_is_valid_json(main_host.variables))
                            THEN main_host.variables::jsonb->>'ansible_connection'
                        ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                    END,
                    'ansible_virtualization_type',
                    main_host.ansible_facts->>'ansible_virtualization_type'::TEXT,
                    'ansible_virtualization_role',
                    main_host.ansible_facts->>'ansible_virtualization_role'::TEXT,
                    'ansible_system_vendor',
                    main_host.ansible_facts->>'ansible_system_vendor'::TEXT,
                    'ansible_product_name',
                    main_host.ansible_facts->>'ansible_product_name'::TEXT,
                    'ansible_architecture',
                    main_host.ansible_facts->>'ansible_architecture'::TEXT,
                    'ansible_processor',
                    main_host.ansible_facts->>'ansible_processor'::TEXT,
                    'ansible_form_factor',
                    main_host.ansible_facts->>'ansible_form_factor'::TEXT,
                    'ansible_bios_vendor',
                    main_host.ansible_facts->>'ansible_bios_vendor'::TEXT,
                    'ansible_bios_version',
                    main_host.ansible_facts->>'ansible_bios_version'::TEXT,
                    'ansible_board_serial',
                    main_host.ansible_facts->>'ansible_board_serial'::TEXT
                ) AS facts

        FROM main_host
        LEFT JOIN main_inventory
            ON main_inventory.id = main_host.inventory_id
        LEFT JOIN main_organization
            ON main_organization.id = main_inventory.organization_id
        LEFT JOIN main_unifiedjob
            ON main_unifiedjob.id = main_host.last_job_id
        WHERE {where}
        ORDER BY main_host.id ASC
    """


@register('main_host', '1.0', format='csv', description=_('Inventory data'), fnc_slicing=limit_slicing)
def main_host(since, full_path, until, **kwargs):
    if 'main_host' not in get_optional_collectors():
        return None

    query = _main_host_query("enabled='t'")
    return _copy_table(
        table='main_host', query=f'COPY ({query}) TO STDOUT WITH CSV HEADER', path=full_path, prepend_query=yaml_and_json_parsing_functions()
    )


@register('main_host_daily', '1.0', format='csv', description=_('Inventory data - daily active hosts'), fnc_slicing=daily_slicing)
def main_host_daily(since, full_path, until, **kwargs):
    if 'main_host_daily' not in get_optional_collectors():
        return None

    where = f"""
        enabled='t'
        AND ({date_where('main_host.created', since, until)}
        OR {date_where('main_host.modified', since, until)})
    """

    query = _main_host_query(where)
    return _copy_table(
        table='main_host_daily', query=f'COPY ({query}) TO STDOUT WITH CSV HEADER', path=full_path, prepend_query=yaml_and_json_parsing_functions()
    )


@register('total_workers_vcpu', '1.0', format='json', description=_('Total workers vCPU'), fnc_slicing=limit_slicing)
def total_workers_vcpu(since, full_path, until, **kwargs):
    if 'total_workers_vcpu' not in get_optional_collectors():
        return None

    cluster_name = os.getenv('METRICS_UTILITY_CLUSTER_NAME')
    red_hat_org_id = os.getenv('METRICS_UTILITY_RED_HAT_ORG_ID')
    log_prefix = f'[METRICS_UTILITY_VCPU]: cluster_name: {cluster_name}, red_hat_org_id: {red_hat_org_id},'
    if not cluster_name:
        logger.error('%s, environment variable METRICS_UTILITY_CLUSTER_NAME is not set', log_prefix)
        raise MissingRequiredEnvVar('environment variable METRICS_UTILITY_CLUSTER_NAME is not set')

    now = datetime.now(timezone.utc)
    current_ts = now.timestamp()
    prev_hour_start, prev_hour_end = get_hour_boundaries(current_ts)

    info = {
        'cluster_name': cluster_name,
        'collection_timestamp': datetime.fromtimestamp(current_ts).isoformat(),
        'start_timestamp': datetime.fromtimestamp(prev_hour_start).isoformat(),
        'end_timestamp': datetime.fromtimestamp(prev_hour_end).isoformat(),
    }
    # If METRICS_UTILITY_USAGE_BASED_METERING_ENABLED is not set or set to false then it returns 1
    usage_based_billing_enabled_str = os.getenv('METRICS_UTILITY_USAGE_BASED_METERING_ENABLED')
    usage_based_billing_enabled = False
    if usage_based_billing_enabled_str and (usage_based_billing_enabled_str.lower() == 'true'):
        usage_based_billing_enabled = True
    info['usage_based_billing_enabled'] = usage_based_billing_enabled
    if not usage_based_billing_enabled:
        info['total_workers_vcpu'] = 1
        # This message must always appear in the log regardless of the log level.
        logger_info_level.info('%s info: %s', log_prefix, json.dumps(info))
        data = {'timestamp': info['end_timestamp'], 'cluster_name': info['cluster_name'], 'total_workers_vcpu': info['total_workers_vcpu']}
        logger_info_level.info('%s data: %s', log_prefix, json.dumps(data))
        return data

    url = os.getenv('METRICS_UTILITY_PROMETHEUS_URL')
    if not url:
        prometheus_default_url = 'https://prometheus-k8s.openshift-monitoring.svc.cluster.local:9091'
        logger.info(
            '%s environment variable METRICS_UTILITY_PROMETHEUS_URL is not set, \
                    default %s will be assigned',
            log_prefix,
            prometheus_default_url,
        )
        url = prometheus_default_url

    try:
        prom = PrometheusClient(url=url)
    except Exception as e:
        raise MetricsException(f'Can not create a prometheus api client ERROR: {e}')

    try:
        total_workers_vcpu, promql_query = get_total_workers_cpu(prom, prev_hour_start)
        timeline = get_cpu_timeline(prom, prev_hour_start, prev_hour_end)
    except MetricsException as e:
        raise MetricsException(f'Unexpected error when retrieving nodes: {e}')

    info['promql_query'] = promql_query
    info['timeline'] = timeline

    logger.debug('%s total_workers_vcpu: %s', log_prefix, total_workers_vcpu)

    # This can happen when the prev_hour_start doesn't have data, it could be when the cluster just started or
    # if for some reasons prometheus loss some data.
    if total_workers_vcpu is None:
        logger.warning('%s No data availble yet, the cluster is probably running for less than an hour', log_prefix)
        raise MetricsException('No data availble yet, the cluster is probably running for less than an hour')

    info['total_workers_vcpu'] = int(total_workers_vcpu)

    # This message must always appear in the log regardless of the log level.
    logger_info_level.info('%s info: %s', log_prefix, json.dumps(info))

    data = {'timestamp': info['end_timestamp'], 'cluster_name': info['cluster_name'], 'total_workers_vcpu': info['total_workers_vcpu']}
    logger_info_level.info('%s data: %s', log_prefix, json.dumps(data))
    return data


def get_hour_boundaries(current_timestamp: float) -> Tuple[float, float, float]:
    current_hour_start = (current_timestamp // 3600) * 3600
    previous_hour_start = current_hour_start - 3600
    previous_hour_end = current_hour_start - 1
    return previous_hour_start, previous_hour_end


def get_total_workers_cpu(prom: PrometheusClient, base_timestamp: float) -> Tuple[float, str]:
    promql_query = f'max_over_time(sum(machine_cpu_cores)[59m59s:5m] @ {base_timestamp})'

    try:
        total_workers_vcpu = prom.get_current_value(promql_query)
    except Exception as e:
        raise MetricsException(f'Unexpected error when retrieving nodes: {e}')

    return total_workers_vcpu, promql_query


def get_cpu_timeline(prom: PrometheusClient, previous_hour_start, previous_hour_end: float) -> list:
    """
    Get array of timestamp/CPU pairs for the hour leading up to previous_hour_end
    Returns:
        List of dicts with 'timestamp' (ISO format) and 'cpu_sum' keys
    """
    # Use instant query - query_range will handle the time range
    query = 'sum(machine_cpu_cores)'

    try:
        response = prom.query_range(query=query, start_time=previous_hour_start, end_time=previous_hour_end, step='5m')

        result = []
        if response and 'data' in response and 'result' in response['data']:
            for series in response['data']['result']:
                if 'values' in series:
                    for timestamp_val, cpu_val in series['values']:
                        result.append(
                            {'timestamp': datetime.fromtimestamp(float(timestamp_val), timezone.utc).isoformat(), 'cpu_sum': float(cpu_val)}
                        )

        # Sort by timestamp
        result.sort(key=lambda x: x['timestamp'])
        return result

    except Exception as e:
        raise MetricsException(f'Error querying CPU timeline: {e}')


@register('unified_jobs', '1.4', format='csv', description=_('Data on jobs run'), fnc_slicing=daily_slicing)
def unified_jobs_table(since, full_path, until, **kwargs):
    if 'unified_jobs' not in get_optional_collectors():
        return None

    unified_job_query = """COPY (SELECT main_unifiedjob.id,
                                 main_unifiedjob.polymorphic_ctype_id,
                                 django_content_type.model,
                                 main_unifiedjob.organization_id,
                                 main_organization.name as organization_name,
                                 main_executionenvironment.image as execution_environment_image,
                                 main_job.inventory_id,
                                 main_inventory.name as inventory_name,
                                 main_unifiedjob.created,
                                 main_unifiedjob.name,
                                 main_unifiedjob.unified_job_template_id,
                                 main_unifiedjob.launch_type,
                                 main_unifiedjob.schedule_id,
                                 main_unifiedjob.execution_node,
                                 main_unifiedjob.controller_node,
                                 main_unifiedjob.cancel_flag,
                                 main_unifiedjob.status,
                                 main_unifiedjob.failed,
                                 main_unifiedjob.started,
                                 main_unifiedjob.finished,
                                 main_unifiedjob.elapsed,
                                 main_unifiedjob.job_explanation,
                                 main_unifiedjob.instance_group_id,
                                 main_unifiedjob.installed_collections,
                                 main_unifiedjob.ansible_version,
                                 main_job.forks,
                                 main_unifiedjobtemplate.name as job_template_name
                                 FROM main_unifiedjob
                                 LEFT JOIN main_unifiedjobtemplate ON main_unifiedjobtemplate.id = main_unifiedjob.unified_job_template_id
                                 LEFT JOIN django_content_type ON main_unifiedjob.polymorphic_ctype_id = django_content_type.id
                                 LEFT JOIN main_job ON main_unifiedjob.id = main_job.unifiedjob_ptr_id
                                 LEFT JOIN main_inventory ON main_job.inventory_id = main_inventory.id
                                 LEFT JOIN main_organization ON main_organization.id = main_unifiedjob.organization_id
                                 LEFT JOIN main_executionenvironment ON main_executionenvironment.id = main_unifiedjob.execution_environment_id
                                 WHERE ((main_unifiedjob.created >= '{0}' AND main_unifiedjob.created < '{1}')
                                       OR (main_unifiedjob.finished >= '{0}' AND main_unifiedjob.finished < '{1}'))
                                       AND main_unifiedjob.launch_type != 'sync'
                                 ORDER BY main_unifiedjob.id ASC) TO STDOUT WITH CSV HEADER
                        """.format(since.isoformat(), until.isoformat())

    return _copy_table(table='unified_jobs', query=unified_job_query, path=full_path)


@register('job_host_summary_service', '1.4', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def job_host_summary_service_table(since, full_path, until, **kwargs):
    if 'job_host_summary_service' not in get_optional_collectors():
        return None

    prepend_query = """
        -- Define function for parsing field out of yaml encoded as text
        CREATE OR REPLACE FUNCTION metrics_utility_parse_yaml_field(
            str text,
            field text
        )
        RETURNS text AS
        $$
        DECLARE
            line_re text;
            field_re text;
        BEGIN
            field_re := ' *[:=] *(.+?) *$';
            line_re := '(?n)^' || field || field_re;
            RETURN trim(both '"' from substring(str from line_re) );
        END;
        $$
        LANGUAGE plpgsql;

        -- Define function to check if field is a valid json
        CREATE OR REPLACE FUNCTION metrics_utility_is_valid_json(p_json text)
            returns boolean
        AS
        $$
        BEGIN
            RETURN (p_json::json is not null);
        EXCEPTION
            WHEN others THEN
                RETURN false;
        END;
        $$
        LANGUAGE plpgsql;
    """

    query = f"""
    WITH
    -- First: restrict to jobs that FINISHED in the window (uses index on main_unifiedjob.finished if present)
    filtered_jobs AS (
        SELECT mu.id
        FROM main_unifiedjob mu
        WHERE mu.finished >= '{since.isoformat()}'
          AND mu.finished <  '{until.isoformat()}'
          AND mu.finished IS NOT NULL
    ),
    --
    -- Then: only host summaries that belong to those jobs (uses index on main_jobhostsummary.job_id)
    filtered_hosts AS (
        SELECT DISTINCT mjs.host_id
        FROM main_jobhostsummary mjs
        JOIN filtered_jobs fj ON fj.id = mjs.job_id
    ),
    --
    hosts_variables AS (
        SELECT
            fh.host_id,
            CASE
                WHEN metrics_utility_is_valid_json(h.variables)
                    THEN h.variables::jsonb->>'ansible_host'
                ELSE metrics_utility_parse_yaml_field(h.variables, 'ansible_host')
            END AS ansible_host_variable,
            CASE
                WHEN metrics_utility_is_valid_json(h.variables)
                    THEN h.variables::jsonb->>'ansible_connection'
                ELSE metrics_utility_parse_yaml_field(h.variables, 'ansible_connection')
            END AS ansible_connection_variable
        FROM filtered_hosts fh
        LEFT JOIN main_host h ON h.id = fh.host_id
    )

    SELECT
        mjs.id,
        mjs.created,
        mjs.modified,
        mjs.host_name,
        mjs.host_id AS host_remote_id,
        hv.ansible_host_variable,
        hv.ansible_connection_variable,
        mjs.changed,
        mjs.dark,
        mjs.failures,
        mjs.ok,
        mjs.processed,
        mjs.skipped,
        mjs.failed,
        mjs.ignored,
        mjs.rescued,
        mu.created AS job_created,
        mjs.job_id AS job_remote_id,
        mu.unified_job_template_id AS job_template_remote_id,
        mu.name AS job_template_name,
        mi.id AS inventory_remote_id,
        mi.name AS inventory_name,
        mo.id AS organization_remote_id,
        mo.name AS organization_name,
        mup.id AS project_remote_id,
        mup.name AS project_name
    FROM filtered_jobs fj
    JOIN main_jobhostsummary mjs ON mjs.job_id = fj.id
    LEFT JOIN main_job mj ON mjs.job_id = mj.unifiedjob_ptr_id
    LEFT JOIN main_unifiedjob mu ON mu.id = mjs.job_id
    LEFT JOIN main_unifiedjobtemplate AS mup ON mup.id = mj.project_id
    LEFT JOIN main_inventory mi ON mi.id = mj.inventory_id
    LEFT JOIN main_organization mo ON mo.id = mu.organization_id
    LEFT JOIN hosts_variables hv ON hv.host_id = mjs.host_id
    ORDER BY mu.finished ASC
    """

    return _copy_table(table='main_jobhostsummary', query=f'COPY ({query}) TO STDOUT WITH CSV HEADER', path=full_path, prepend_query=prepend_query)


@register('main_jobevent_service', '1.4', format='csv', description=_('Content usage'), fnc_slicing=daily_slicing)
def main_jobevent_service_table(since, full_path, until, **kwargs):
    if 'main_jobevent_service' not in get_optional_collectors():
        return None

    # Use the table alias 'e' here (you alias main_jobevent as e in the FROM)
    event_data = r"replace(e.event_data, '\u', '\u005cu')::jsonb"

    # 1) Load finished jobs in the window
    jobs_query = """
        SELECT uj.id AS job_id,
               uj.created AS job_created
        FROM main_unifiedjob uj
        WHERE uj.finished >= %(since)s
          AND uj.finished <  %(until)s
    """
    jobs = []

    # do raw sql for django.db connection
    with connection.cursor() as cursor:
        cursor.execute(jobs_query, {'since': since, 'until': until})
        jobs = cursor.fetchall()

    # 2) Build a literal WHERE clause that preserves (job_id, job_created) pairing
    if jobs:
        # (e.job_id, e.job_created) IN (VALUES (id1, 'ts1'::timestamptz), ...)
        pairs_sql = ',\n'.join(f"({jid}, '{jcreated.isoformat()}'::timestamptz)" for jid, jcreated in jobs)
        where_clause = f'(e.job_id, e.job_created) IN (VALUES {pairs_sql})'
    else:
        # No jobs in the window → no events
        where_clause = 'FALSE'

    # 3) Final event query
    query = f"""
        SELECT
            e.id,
            e.created,
            e.modified,
            e.job_created,
            uj.finished as job_finished,
            e.uuid,
            e.parent_uuid,
            e.event,

            -- JSON extracted fields
            ({event_data}->>'task_action')       AS task_action,
            ({event_data}->>'resolved_action')   AS resolved_action,
            ({event_data}->>'resolved_role')     AS resolved_role,
            ({event_data}->>'duration')          AS duration,
            ({event_data}->>'start')::timestamptz AS start,
            ({event_data}->>'end')::timestamptz   AS end,
            ({event_data}->>'task_uuid')        AS task_uuid,
            COALESCE( ({event_data}->>'ignore_errors')::boolean, false ) AS ignore_errors,
            e.failed,
            e.changed,
            e.playbook,
            e.play,
            e.task,
            e.role,
            e.job_id  AS job_remote_id,
            e.job_id,
            e.host_id AS host_remote_id,
            e.host_id,
            e.host_name,

            -- Warnings and deprecations (json arrays)
            {event_data}->'res'->'warnings'     AS warnings,
            {event_data}->'res'->'deprecations' AS deprecations,

            CASE WHEN e.event = 'playbook_on_stats'
                 THEN {event_data} - 'artifact_data'
            END AS playbook_on_stats,

            uj.failed as job_failed,
            uj.started as job_started

        FROM main_jobevent e
        LEFT JOIN main_unifiedjob uj ON uj.id = e.job_id
        WHERE {where_clause}
    """

    return _copy_table(table='main_jobevent', query=f'COPY ({query}) TO STDOUT WITH CSV HEADER', path=full_path)


@register('execution_environments', '1.4', format='csv', description=_('Execution environments'), fnc_slicing=daily_slicing)
def execution_environments_table(since, full_path, until, **kwargs):
    if 'execution_environments' not in get_optional_collectors():
        return None

    sql = """
        SELECT
        id,
        created,
        modified,
        description,
        image,
        managed,
        created_by_id,
        credential_id,
        modified_by_id,
        organization_id,
        name,
        pull
        FROM public.main_executionenvironment
    """

    return _copy_table(table='main_executionenvironment', query=f'COPY ({sql}) TO STDOUT WITH CSV HEADER', path=full_path)
