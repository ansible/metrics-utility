import datetime
import logging

import pandas as pd

from django.db import connection


class ExtractorControllerDB():
    LOG_PREFIX = "[ExtractorDirectory]"

    def __init__(self, extra_params, logger=logging.getLogger(__name__)):
        super().__init__()

        self.extension = "parquet"
        self.path = extra_params["ship_path"]
        self.extra_params = extra_params

        self.logger = logger

    def get_report_path(self, date):
        path_prefix = f"{self.path}/reports"

        year = date.strftime("%Y")
        month = date.strftime("%m")

        path = f"{path_prefix}/{year}/{month}"

        return path

    def iter_batches(self):
        with connection.cursor() as cursor:
            cursor.execute(self.pg_functions())

            since = self.extra_params['opt_since']
            if since.tzinfo is None:
                since = since.replace(tzinfo=datetime.timezone.utc)

            marker_cond = ""
            while True:
                cursor.execute(self.host_metric_query(since, marker_cond))
                host_metric = self.dict_fetchall(cursor)

                # Marker based pagination
                if len(host_metric) <= 0:
                    break

                marker_cond = f'''
                    AND CONCAT(main_hostmetric.hostname , '___', COALESCE(main_host.id, 0)) >
                        '{list(host_metric)[-1]['hostname']}___{list(host_metric)[-1]['host_id']}'
                    -- TODO wrong query, has to use several or conditions, but will it help with usage of index?
                    -- AND main_hostmetric.hostname >= '{list(host_metric)[-1]['hostname']}'
                    -- AND COALESCE(main_host.id, 0) > {list(host_metric)[-1]['host_id']}
                '''

                host_metric = pd.DataFrame(host_metric)

                yield {'host_metric': host_metric}

    def dict_fetchall(self, cursor):
        """
        Return all rows from a cursor as a dict.
        Assume the column names are unique.
        """
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def pg_functions(self):
        query = '''
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
        return query

    def host_metric_query(self, since, marker_cond=""):
        query = f'''
            SELECT main_hostmetric.hostname,
                   COALESCE(main_host.id, 0) AS host_id,
                   main_hostmetric.first_automation,
                   main_hostmetric.last_automation,
                   main_hostmetric.automated_counter,
                   main_hostmetric.deleted_counter,
                   main_hostmetric.last_deleted,
                   main_hostmetric.deleted,
                   main_host.ansible_facts->>'ansible_board_serial'::TEXT AS ansible_board_serial,
                   main_host.ansible_facts->>'ansible_machine_id'::TEXT AS ansible_machine_id,
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

            FROM main_hostmetric
            LEFT JOIN main_host ON main_host.name = main_hostmetric.hostname
            WHERE (main_hostmetric.last_automation >= '{since.isoformat()}' {marker_cond})
            ORDER BY CONCAT(main_hostmetric.hostname , '___', COALESCE(main_host.id, 0)) ASC
            -- ORDER BY main_hostmetric.hostname ASC, COALESCE(main_host.id, 0) ASC
            LIMIT {self.limit()}
        '''

        return query

    @staticmethod
    def limit():
        return 1


# ansible_facts | {
#     "ansible_dns": {"options": {"ndots": "0"}, "nameservers": ["10.0.2.3", "8.8.8.8", "8.8.4.4", "2001:4860:4860::8888", "2001:4860:4860::8844"]},
#     "ansible_env": {
#         "_": "/usr/bin/python3",
#         "PWD": "/runner/project",
#         "HOME": "/root",
#         "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
#         "TERM": "xterm",
#         "SHLVL": "1",
#         "JOB_ID": "14",
#         "AWX_HOST": "https://towerhost",
#         "HOSTNAME": "6eb3b25e796b",
#         "LC_CTYPE": "C.UTF-8",
#         "container": "podman",
#         "INVENTORY_ID": "2",
#         "MAX_EVENT_RES": "700000",
#         "PROJECT_REVISION": "347e44fea036c94d5f60e544de006453ee5c71ad",
#         "ANSIBLE_ROLES_PATH": "/runner/requirements_roles:~/.ansible/roles:/usr/share/ansible/roles:/etc/ansible/roles",
#         "RUNNER_OMIT_EVENTS": "False",
#         "ANSIBLE_FORCE_COLOR": "True",
#         "ANSIBLE_CACHE_PLUGIN": "jsonfile",
#         "AWX_PRIVATE_DATA_DIR": "/tmp/awx_14_9vh_59h_",
#         "ANSIBLE_UNSAFE_WRITES": "1",
#         "AWX_ISOLATED_DATA_DIR": "/runner/artifacts/14",
#         "ANSIBLE_BASE_TEAM_MODEL": "main.Team",
#         "ANSIBLE_STDOUT_CALLBACK": "awx_display",
#         "ANSIBLE_CALLBACK_PLUGINS": "/runner/artifacts/14/callback",
#         "ANSIBLE_COLLECTIONS_PATH": "/runner/requirements_collections:~/.ansible/collections:/usr/share/ansible/collections",
#         "ANSIBLE_COLLECTIONS_PATHS": "/runner/requirements_collections:~/.ansible/collections:/usr/share/ansible/collections",
#         "ANSIBLE_HOST_KEY_CHECKING": "False",
#         "PIP_BREAK_SYSTEM_PACKAGES": "1",
#         "RUNNER_ONLY_FAILED_EVENTS": "False",
#         "ANSIBLE_BASE_ROLE_PRECREATE": "{}",
#         "ANSIBLE_RETRY_FILES_ENABLED": "False",
#         "ANSIBLE_SSH_CONTROL_PATH_DIR": "/runner/cp",
#         "ANSIBLE_BASE_ALL_REST_FILTERS": "('ansible_base.rest_filters.rest_framework.type_filter_backend.TypeFilterBackend', 'ansible_base.rest_filters.rest_framework.field_lookup_backend.FieldLookupBackend', 'rest_framework.filters.SearchFilter', 'ansible_base.rest_filters.rest_framework.order_backend.OrderByBackend')",
#         "ANSIBLE_BASE_CREATOR_DEFAULTS": "['change', 'delete', 'execute', 'use', 'adhoc', 'approve', 'update', 'view']",
#         "ANSIBLE_BASE_PERMISSION_MODEL": "main.Permission",
#         "ANSIBLE_BASE_ROLE_CREATOR_NAME": "{cls.__name__} Creator",
#         "ANSIBLE_BASE_ALLOW_CUSTOM_ROLES": "True",
#         "ANSIBLE_BASE_ALLOW_TEAM_PARENTS": "True",
#         "ANSIBLE_BASE_CUSTOM_VIEW_PARENT": "awx.api.generics.APIView",
#         "ANSIBLE_BASE_ORGANIZATION_MODEL": "main.Organization",
#         "ANSIBLE_CACHE_PLUGIN_CONNECTION": "/runner/artifacts/14/fact_cache",
#         "ANSIBLE_BASE_BYPASS_ACTION_FLAGS": "{}",
#         "ANSIBLE_BASE_ALLOW_TEAM_ORG_ADMIN": "False",
#         "ANSIBLE_BASE_ALLOW_TEAM_ORG_PERMS": "True",
#         "ANSIBLE_INVENTORY_UNPARSED_FAILED": "True",
#         "ANSIBLE_PARAMIKO_RECORD_HOST_KEYS": "False",
#         "ANSIBLE_BASE_ROLE_SYSTEM_ACTIVATED": "True",
#         "ANSIBLE_BASE_BYPASS_SUPERUSER_FLAGS": "['is_superuser']",
#         "ANSIBLE_BASE_RESOURCE_CONFIG_MODULE": "awx.resource_api",
#         "ANSIBLE_BASE_ALLOW_CUSTOM_TEAM_ROLES": "False",
#         "ANSIBLE_BASE_CACHE_PARENT_PERMISSIONS": "True",
#         "ANSIBLE_BASE_ALLOW_SINGLETON_ROLES_API": "False",
#         "ANSIBLE_BASE_CHECK_RELATED_PERMISSIONS": "['use', 'change', 'view']",
#         "ANSIBLE_BASE_ALLOW_SINGLETON_TEAM_ROLES": "False",
#         "ANSIBLE_BASE_ALLOW_SINGLETON_USER_ROLES": "True",
#         "ANSIBLE_BASE_REST_FILTERS_RESERVED_NAMES": "('page', 'page_size', 'format', 'order', 'order_by', 'search', 'type', 'host_filter', 'count_disabled', 'no_truncate', 'limit', 'validate')",
#         "ANSIBLE_BASE_EVALUATIONS_IGNORE_CONFLICTS": "True",
#     },
#     "ansible_lsb": {},
#     "ansible_lvm": "N/A",
#     "ansible_fips": false,
#     "ansible_fqdn": "6eb3b25e796b",
#     "module_setup": true,
#     "ansible_local": {},
#     "gather_subset": ["all"],
#     "ansible_domain": "",
#     "ansible_kernel": "6.6.14-0-virt",
#     "ansible_mounts": [],
#     "ansible_python": {
#         "type": "cpython",
#         "version": {"major": 3, "micro": 18, "minor": 9, "serial": 0, "releaselevel": "final"},
#         "executable": "/usr/bin/python3",
#         "version_info": [3, 9, 18, "final", 0],
#         "has_sslcontext": true,
#     },
#     "ansible_system": "Linux",
#     "ansible_cmdline": {"console": "ttyS0,115200", "modules": "loop,squashfs,sd-mod,usb-storage", "BOOT_IMAGE": "/boot/vmlinuz-virt"},
#     "ansible_devices": {
#         "sr0": {
#             "host": "",
#             "size": "36.27 MB",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": "QEMU CD-ROM",
#             "vendor": "QEMU",
#             "holders": [],
#             "sectors": "74276",
#             "virtual": 1,
#             "removable": "1",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "2048",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "vda": {
#             "host": "",
#             "size": "243.10 MB",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": "0x1af4",
#             "holders": [],
#             "sectors": "497864",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {
#                 "vda1": {
#                     "size": "170.00 KB",
#                     "uuid": null,
#                     "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#                     "start": "64",
#                     "holders": [],
#                     "sectors": "340",
#                     "sectorsize": 512,
#                 },
#                 "vda2": {
#                     "size": "1.41 MB",
#                     "uuid": null,
#                     "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#                     "start": "404",
#                     "holders": [],
#                     "sectors": "2880",
#                     "sectorsize": 512,
#                 },
#                 "vda3": {
#                     "size": "241.46 MB",
#                     "uuid": null,
#                     "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#                     "start": "3284",
#                     "holders": [],
#                     "sectors": "494516",
#                     "sectorsize": 512,
#                 },
#             },
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "512",
#             "sas_device_handle": null,
#         },
#         "vdb": {
#             "host": "",
#             "size": "100.00 GB",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": "0x1af4",
#             "holders": [],
#             "sectors": "209715200",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {
#                 "vdb1": {
#                     "size": "100.00 GB",
#                     "uuid": null,
#                     "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#                     "start": "2048",
#                     "holders": [],
#                     "sectors": "209713152",
#                     "sectorsize": 512,
#                 }
#             },
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "512",
#             "sas_device_handle": null,
#         },
#         "loop0": {
#             "host": "",
#             "size": "15.32 MB",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "31368",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop1": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop2": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop3": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop4": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop5": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop6": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#         "loop7": {
#             "host": "",
#             "size": "0.00 Bytes",
#             "links": {"ids": [], "uuids": [], "labels": [], "masters": []},
#             "model": null,
#             "vendor": null,
#             "holders": [],
#             "sectors": "0",
#             "virtual": 1,
#             "removable": "0",
#             "partitions": {},
#             "rotational": "1",
#             "sectorsize": "512",
#             "sas_address": null,
#             "scheduler_mode": "none",
#             "support_discard": "0",
#             "sas_device_handle": null,
#         },
#     },
#     "ansible_hostnqn": "nqn.2014-08.org.nvmexpress:uuid:0b26bcdb-06bb-4cab-85fb-40538ce8c3d7",
#     "ansible_loadavg": {"1m": 0.33, "5m": 0.17, "15m": 0.15},
#     "ansible_machine": "aarch64",
#     "ansible_pkg_mgr": "dnf",
#     "ansible_selinux": {"status": "disabled"},
#     "ansible_user_id": "root",
#     "ansible_apparmor": {"status": "disabled"},
#     "ansible_hostname": "6eb3b25e796b",
#     "ansible_nodename": "6eb3b25e796b",
#     "ansible_user_dir": "/root",
#     "ansible_user_gid": 0,
#     "ansible_user_uid": 0,
#     "ansible_bios_date": "03/01/2023",
#     "ansible_date_time": {
#         "tz": "UTC",
#         "day": "09",
#         "date": "2024-06-09",
#         "hour": "11",
#         "time": "11:42:18",
#         "year": "2024",
#         "epoch": "1717933338",
#         "month": "06",
#         "minute": "42",
#         "second": "18",
#         "tz_dst": "UTC",
#         "iso8601": "2024-06-09T11:42:18Z",
#         "weekday": "Sunday",
#         "epoch_int": "1717933338",
#         "tz_offset": "+0000",
#         "weeknumber": "23",
#         "iso8601_basic": "20240609T114218004129",
#         "iso8601_micro": "2024-06-09T11:42:18.004129Z",
#         "weekday_number": "0",
#         "iso8601_basic_short": "20240609T114218",
#     },
#     "ansible_is_chroot": false,
#     "ansible_iscsi_iqn": "",
#     "ansible_memory_mb": {
#         "real": {"free": 2629, "used": 12335, "total": 14964},
#         "swap": {"free": 0, "used": 0, "total": 0, "cached": 0},
#         "nocache": {"free": 9956, "used": 5008},
#     },
#     "ansible_os_family": "RedHat",
#     "ansible_processor": ["0", "1", "2", "3", "4", "5"],
#     "ansible_board_name": "NA",
#     "ansible_machine_id": "5a1d3c1bf19d40b8831fae26548cc3ee",
#     "ansible_memfree_mb": 2629,
#     "ansible_user_gecos": "root",
#     "ansible_user_shell": "/bin/bash",
#     "ansible_bios_vendor": "EDK II",
#     "ansible_form_factor": "Other",
#     "ansible_memtotal_mb": 14964,
#     "ansible_service_mgr": "dumb-init",
#     "ansible_swapfree_mb": 0,
#     "ansible_architecture": "aarch64",
#     "ansible_bios_version": "edk2-stable202302-for-qemu",
#     "ansible_board_serial": "NA",
#     "ansible_board_vendor": "NA",
#     "ansible_device_links": {"ids": {}, "uuids": {}, "labels": {}, "masters": {}},
#     "ansible_distribution": "CentOS",
#     "ansible_proc_cmdline": {"console": ["hvc0", "tty0", "ttyS0,115200"], "modules": "loop,squashfs,sd-mod,usb-storage", "BOOT_IMAGE": "/boot/vmlinuz-virt"},
#     "ansible_product_name": "QEMU Virtual Machine",
#     "ansible_product_uuid": "NA",
#     "ansible_real_user_id": 0,
#     "ansible_swaptotal_mb": 0,
#     "ansible_board_version": "NA",
#     "ansible_real_group_id": 0,
#     "ansible_system_vendor": "QEMU",
#     "ansible_chassis_serial": "NA",
#     "ansible_chassis_vendor": "QEMU",
#     "ansible_kernel_version": "#1-Alpine SMP Fri, 26 Jan 2024 11:08:07 +0000",
#     "ansible_product_serial": "NA",
#     "ansible_python_version": "3.9.18",
#     "ansible_uptime_seconds": 771138,
#     "ansible_userspace_bits": "64",
#     "_ansible_facts_gathered": true,
#     "ansible_board_asset_tag": "NA",
#     "ansible_chassis_version": "virt-8.2",
#     "ansible_processor_cores": 1,
#     "ansible_processor_count": 6,
#     "ansible_processor_nproc": 6,
#     "ansible_processor_vcpus": 6,
#     "ansible_product_version": "virt-8.2",
#     "ansible_chassis_asset_tag": "NA",
#     "ansible_effective_user_id": 0,
#     "ansible_fibre_channel_wwn": [],
#     "ansible_effective_group_id": 0,
#     "ansible_system_capabilities": ["ep"],
#     "ansible_virtualization_role": "guest",
#     "ansible_virtualization_type": "podman",
#     "ansible_distribution_release": "Stream",
#     "ansible_distribution_version": "9",
#     "ansible_distribution_file_path": "/etc/centos-release",
#     "ansible_selinux_python_present": true,
#     "ansible_distribution_file_parsed": true,
#     "ansible_virtualization_tech_host": [],
#     "ansible_distribution_file_variety": "CentOS",
#     "ansible_virtualization_tech_guest": ["container", "kvm", "podman"],
#     "ansible_distribution_major_version": "9",
#     "ansible_processor_threads_per_core": 1,
#     "ansible_system_capabilities_enforced": "True",
# }
