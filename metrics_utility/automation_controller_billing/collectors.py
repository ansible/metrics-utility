import json
import os
import os.path
import platform
import distro

from django.db import connection
from django.conf import settings
from django.utils.timezone import now, timedelta
from django.utils.translation import gettext_lazy as _

from awx.conf.license import get_license
from awx.main.utils import get_awx_version, datetime_hook
# TODO: enhance the CsvFIleSplitter base class and use that
from insights_analytics_collector import register #, CsvFileSplitter
from metrics_utility.automation_controller_billing.csv_file_splitter import CsvFileSplitter

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
        from awx.conf.models import Setting

        horizon = until - timedelta(weeks=4)
        last_entries = Setting.objects.filter(key='AUTOMATION_ANALYTICS_LAST_ENTRIES').first()
        last_entries = json.loads((last_entries.value if last_entries is not None else '') or '{}', object_hook=datetime_hook)
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

@register('config', '1.0', description=_('General platform configuration.'), config=True)
def config(since, **kwargs):
    license_info = get_license()
    install_type = 'traditional'
    if os.environ.get('container') == 'oci':
        install_type = 'openshift'
    elif 'KUBERNETES_SERVICE_PORT' in os.environ:
        install_type = 'k8s'
    return {
        'platform': {
            'system': platform.system(),
            'dist': distro.linux_distribution(),
            'release': platform.release(),
            'type': install_type,
        },
        'install_uuid': settings.INSTALL_UUID,
        'instance_uuid': settings.SYSTEM_UUID,
        'controller_url_base': settings.TOWER_URL_BASE,
        'controller_version': get_awx_version(),
        'license_type': license_info.get('license_type', 'UNLICENSED'),
        'license_date': license_info.get('license_date'),
        'subscription_name': license_info.get('subscription_name'),
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
        'subscription_usage_model': getattr(settings, 'SUBSCRIPTION_USAGE_MODEL', ''),  # 1.5+
        'free_instances': license_info.get('free_instances', 0),
        'total_licensed_instances': license_info.get('instance_count', 0),
        'license_expiry': license_info.get('time_remaining', 0),
        'pendo_tracking': settings.PENDO_TRACKING_STATE,
        'authentication_backends': settings.AUTHENTICATION_BACKENDS,
        'logging_aggregators': settings.LOG_AGGREGATOR_LOGGERS,
        'external_logger_enabled': settings.LOG_AGGREGATOR_ENABLED,
        'external_logger_type': getattr(settings, 'LOG_AGGREGATOR_TYPE', None),
        'metrics_utility_version': "0.2.0", # TODO read from setup.cfg
        'billing_provider_params': {} # Is being overwritten in collector.gather by set ENV VARS
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

    return file.file_list()


def _copy_table_aap_2_4_and_below(cursor, query, file):
    # Automation Controller 4.4 and below use psycopg2 with .copy_expert() method
    cursor.copy_expert(query, file)


def _copy_table_aap_2_5_and_above(cursor, query, file):
    # Automation Controller 4.5 and above use psycopg3 with .copy() method
    with cursor.copy(query) as copy:
        while data := copy.read():
            byte_data = bytes(data)
            file.write(byte_data.decode())


@register('job_host_summary', '1.2', format='csv', description=_('Data for billing'), fnc_slicing=daily_slicing)
def job_host_summary_table(since, full_path, until, **kwargs):
    # TODO: controler needs to have an index on main_jobhostsummary.modified
    prepend_query = '''
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
    '''

    query = '''
        (SELECT main_jobhostsummary.id,
                main_jobhostsummary.created,
                main_jobhostsummary.modified,
                main_jobhostsummary.host_name,
                main_jobhostsummary.host_id as host_remote_id,
                CASE
                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_host'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_host' )
                END AS ansible_host_variable,
                CASE
                    WHEN (metrics_utility_is_valid_json(main_host.variables))
                        THEN main_host.variables::jsonb->>'ansible_connection'
                    ELSE metrics_utility_parse_yaml_field(main_host.variables, 'ansible_connection' )
                END AS ansible_connection_variable,
                -- main_jobhostsummary.constructed_host_id,
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
                -- get variables from main_host
                LEFT JOIN main_host ON main_host.id = main_jobhostsummary.host_id
                WHERE (main_jobhostsummary.modified >= '{0}' AND main_jobhostsummary.modified < '{1}')
                ORDER BY main_jobhostsummary.modified ASC)
        '''.format(
            since.isoformat(), until.isoformat()
    )

    return _copy_table(table='main_jobhostsummary',
                       query=f"COPY {query} TO STDOUT WITH CSV HEADER",
                       path=full_path,
                       prepend_query=prepend_query)


@register('main_jobevent', '1.0', format='csv', description=_('Content usage'), fnc_slicing=daily_slicing)
def main_jobevent_table(since, full_path, until, **kwargs):
    tbl = 'main_jobevent'
    event_data = fr"replace({tbl}.event_data, '\u', '\u005cu')::jsonb"

    query = f'''
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
        '''
    return _copy_table(table=tbl,
                       query=f"COPY ({query}) TO STDOUT WITH CSV HEADER",
                       path=full_path)
