import json
import random
import uuid

from datetime import timedelta

from modules import MODULES


# ---------------------------------------------------------------------------
# Realistic event_data['res'] generation
# ---------------------------------------------------------------------------

_COMMAND_MODULES = frozenset(
    {
        'ansible.builtin.command',
        'ansible.builtin.shell',
        'cisco.ios.ios_command',
        'cisco.iosxr.iosxr_config',
        'cisco.nxos.nxos_config',
    }
)
_PACKAGE_MODULES = frozenset(
    {
        'ansible.builtin.yum',
        'ansible.builtin.apt',
        'ansible.builtin.dnf',
        'ansible.builtin.package',
        'community.general.npm',
        'redhat.satellite.repository',
    }
)
_FILE_MODULES = frozenset(
    {
        'ansible.builtin.copy',
        'ansible.builtin.template',
        'ansible.builtin.file',
        'ansible.builtin.fetch',
        'ansible.builtin.get_url',
        'ansible.builtin.lineinfile',
        'ansible.builtin.stat',
    }
)
_SERVICE_MODULES = frozenset(
    {
        'ansible.builtin.service',
        'ansible.builtin.systemd',
        'ansible.builtin.cron',
        'ansible.posix.mount',
    }
)
_CLOUD_MODULES = frozenset(
    {
        'amazon.aws.ec2_instance',
        'amazon.aws.s3_bucket',
        'amazon.aws.ec2_vpc_subnet',
        'amazon.aws.ec2_security_group',
        'amazon.aws.rds_instance',
        'amazon.aws.iam_role',
        'azure.azcollection.azure_rm_virtualmachine',
        'azure.azcollection.azure_rm_storageaccount',
        'azure.azcollection.azure_rm_networkinterface',
        'azure.azcollection.azure_rm_securitygroup',
        'google.cloud.gcp_compute_instance',
        'google.cloud.gcp_storage_bucket',
        'google.cloud.gcp_compute_disk',
        'vmware.vmware_rest.vcenter_vm',
        'vmware.vmware_rest.vcenter_datastore',
    }
)
_DB_MODULES = frozenset(
    {
        'community.mysql.mysql_db',
        'community.mysql.mysql_user',
        'community.mysql.mysql_replication',
        'community.postgresql.postgresql_db',
        'community.postgresql.postgresql_user',
        'community.postgresql.postgresql_query',
    }
)
_CONTAINER_MODULES = frozenset(
    {
        'community.general.docker_container',
        'community.general.docker_image',
        'community.general.docker_network',
        'community.docker.docker_container',
        'community.docker.docker_compose',
        'community.docker.docker_swarm',
        'community.kubernetes.k8s',
        'community.kubernetes.helm',
        'community.kubernetes.k8s_info',
    }
)

# Typical stdout line templates for command/shell output
_STDOUT_LINE_TEMPLATES = [
    'Processing item {i} of {total}...',
    '[INFO] Step {i}: completed successfully',
    'OK: package-{i}.noarch already installed',
    'Checking dependency {i}... done',
    '/usr/lib/systemd/system/service-{i}.service: enabled',
    'warning: rpmts_HdrFromFdno: Header V4 RSA/SHA256 Signature, key ID fd431d51: NOKEY',
    'Resolving dependencies... {i}%',
    'Transaction check: {i} packages',
    '  --> Running transaction check',
    '  ---> Package python3-{i}.x86_64 1.{i}.0-1.el9 will be installed',
    'Install  {i} Packages',
    'Upgraded:  package-lib-{i}.x86_64 1.{i}.0',
    'Complete!',
    'Loaded plugins: fastestmirror, langpacks',
    'Loading mirror speeds from cached hostfile',
    ' * base: mirror.example.com',
    ' * extras: extras.example.com',
    'Resolving Dependencies',
    '--> Running transaction check',
    '---> Package {i} will be installed',
    'Nothing to do',
    'Metadata cache created.',
    '[{i}/{total}] Verifying : python3-module-{i}.noarch',
]

_PACKAGE_NAMES = [
    'httpd',
    'nginx',
    'python3',
    'openssl',
    'curl',
    'wget',
    'git',
    'vim',
    'tmux',
    'rsync',
    'tar',
    'gzip',
    'bzip2',
    'unzip',
    'zip',
    'lsof',
    'strace',
    'tcpdump',
    'nmap',
    'iptables',
    'firewalld',
    'selinux-policy',
    'policycoreutils',
    'audit',
    'sssd',
    'krb5-workstation',
    'samba',
    'nfs-utils',
    'autofs',
    'bind',
    'dhcp',
    'postfix',
    'dovecot',
    'sendmail',
    'mutt',
    'procmail',
    'spamassassin',
    'postgresql',
    'mysql',
    'mariadb',
    'redis',
    'memcached',
    'mongodb',
    'java-17-openjdk',
    'nodejs',
    'ruby',
    'perl',
    'php',
    'golang',
    'ansible',
    'puppet',
    'chef',
    'salt',
    'terraform',
    'docker',
    'podman',
]

_SYSTEMD_STATES = ['active', 'inactive', 'failed', 'activating', 'deactivating']
_SYSTEMD_SUBSTATES = ['running', 'dead', 'failed', 'start', 'stop', 'exited']


_NOISE_LEVELS = ['small', 'medium', 'large']
_NOISE_WEIGHTS = [0.50, 0.35, 0.15]  # 50% small, 35% medium, 15% large


def _pick_noise_level(rng):
    """Pick a noise level randomly, weighted towards smaller sizes."""
    r = rng.random()
    cumulative = 0.0
    for level, weight in zip(_NOISE_LEVELS, _NOISE_WEIGHTS):
        cumulative += weight
        if r < cumulative:
            return level
    return _NOISE_LEVELS[-1]


def _noise_line_count(rng, noise_level):
    """Return stdout line count scaled by noise level with some random variance."""
    base = {'small': 20, 'medium': 200, 'large': 2000}[noise_level]
    return int(rng.uniform(0.5, 1.5) * base)


def _generate_stdout_lines(rng, count):
    """Generate realistic-looking stdout lines."""
    lines = []
    for i in range(count):
        tpl = _STDOUT_LINE_TEMPLATES[rng.randint(0, len(_STDOUT_LINE_TEMPLATES) - 1)]
        line = tpl.format(i=i + 1, total=count)
        lines.append(line)
    return lines


def _res_command(rng, noise_level):
    lines = _generate_stdout_lines(rng, _noise_line_count(rng, noise_level))
    stdout = '\n'.join(lines)
    return {
        'rc': 0,
        'cmd': 'systemctl list-units --type=service --state=running',
        'stdout': stdout,
        'stderr': '',
        'stdout_lines': lines,
        'stderr_lines': [],
        'delta': '0:00:00.{:06d}'.format(rng.randint(10000, 999999)),
        'start': '2024-01-15 10:00:00.000000',
        'end': '2024-01-15 10:00:00.500000',
    }


def _res_package(rng, noise_level):
    pkg_count = _noise_line_count(rng, noise_level) // 5
    pkg_count = max(1, pkg_count)
    names = [rng.choice(_PACKAGE_NAMES) for _ in range(pkg_count)]
    results = [f'Installed: {name}-{rng.randint(1, 9)}.{rng.randint(0, 99)}.{rng.randint(0, 9)}-1.el9.noarch' for name in names]
    dep_count = pkg_count * 3
    results += [
        f'Dependency Installed: python3-{rng.choice(_PACKAGE_NAMES)}-{rng.randint(1, 5)}.{rng.randint(0, 9)}.0-1.el9.noarch' for _ in range(dep_count)
    ]
    return {
        'rc': 0,
        'results': results,
        'msg': '',
        'changed': True,
    }


def _res_file(rng):
    checksum = ''.join(['{:02x}'.format(rng.randint(0, 255)) for _ in range(20)])
    return {
        'dest': f'/etc/app/config-{rng.randint(1, 100)}.conf',
        'src': f'/tmp/ansible-tmp-{rng.randint(100000, 999999)}/source',
        'md5sum': checksum[:32],
        'checksum': checksum,
        'size': rng.randint(512, 65536),
        'uid': 0,
        'gid': 0,
        'mode': '0644',
        'owner': 'root',
        'group': 'root',
        'secontext': 'system_u:object_r:etc_t:s0',
        'diff': {
            'before': {'path': f'/etc/app/config-{rng.randint(1, 100)}.conf'},
            'after': {'path': f'/etc/app/config-{rng.randint(1, 100)}.conf'},
        },
    }


def _res_service(rng):
    name = rng.choice(['nginx', 'httpd', 'sshd', 'firewalld', 'crond', 'auditd', 'rsyslog'])
    state = rng.choice(_SYSTEMD_STATES)
    substate = rng.choice(_SYSTEMD_SUBSTATES)
    return {
        'name': name,
        'state': state,
        'status': {
            'ActiveState': state,
            'SubState': substate,
            'Id': f'{name}.service',
            'Description': f'{name.capitalize()} HTTP Server',
            'ExecMainPID': str(rng.randint(1000, 99999)),
            'ExecMainStartTimestamp': 'Mon 2024-01-15 10:00:00 UTC',
            'FragmentPath': f'/usr/lib/systemd/system/{name}.service',
            'LoadState': 'loaded',
            'MainPID': str(rng.randint(1000, 99999)),
            'MemoryCurrent': str(rng.randint(10000000, 500000000)),
            'NRestarts': str(rng.randint(0, 5)),
            'TasksCurrent': str(rng.randint(1, 50)),
            'Type': 'simple',
            'UnitFileState': 'enabled',
        },
    }


def _res_cloud(rng, noise_level):
    count = _noise_line_count(rng, noise_level) // 20
    count = max(1, count)
    instances = []
    for _ in range(count):
        instances.append(
            {
                'instance_id': 'i-{:017x}'.format(rng.randint(0, 2**64)),
                'instance_type': rng.choice(['t3.micro', 't3.small', 'm5.large', 'c5.xlarge', 'r5.2xlarge']),
                'state': rng.choice(['running', 'stopped', 'terminated', 'pending']),
                'private_ip_address': '{}.{}.{}.{}'.format(*[rng.randint(10, 254) for _ in range(4)]),
                'public_ip_address': '{}.{}.{}.{}'.format(*[rng.randint(1, 254) for _ in range(4)]),
                'tags': {
                    'Name': f'perf-test-{rng.randint(1000, 9999)}',
                    'Environment': rng.choice(['dev', 'staging', 'prod']),
                    'Owner': 'ansible-automation',
                },
                'launch_time': '2024-01-15T10:00:00+00:00',
                'vpc_id': 'vpc-{:08x}'.format(rng.randint(0, 2**32)),
                'subnet_id': 'subnet-{:08x}'.format(rng.randint(0, 2**32)),
            }
        )
    return {'instances': instances, 'changed': True}


def _res_db(rng):
    return {
        'db': rng.choice(['app_db', 'metrics_db', 'users_db', 'analytics_db']),
        'executed': True,
        'changed': rng.choice([True, False]),
        'query_result': [{'id': i, 'name': f'record_{i}', 'value': rng.randint(1, 1000)} for i in range(rng.randint(1, 20))],
        'rowcount': rng.randint(0, 100),
    }


def _res_container(rng, noise_level):
    count = _noise_line_count(rng, noise_level) // 10
    count = max(1, count)
    return {
        'container': {
            'Id': '{:064x}'.format(rng.randint(0, 2**256)),
            'Name': f'/app-container-{rng.randint(1, 999)}',
            'State': {
                'Status': rng.choice(['running', 'exited', 'created']),
                'Running': rng.choice([True, False]),
                'Pid': rng.randint(1000, 99999),
                'ExitCode': 0,
                'StartedAt': '2024-01-15T10:00:00.000000000Z',
            },
            'NetworkSettings': {
                'IPAddress': '{}.{}.{}.{}'.format(*[rng.randint(172, 192) for _ in range(4)]),
                'Ports': {f'{rng.randint(3000, 9000)}/tcp': [{'HostIp': '0.0.0.0', 'HostPort': str(rng.randint(3000, 9000))}]},
            },
            'Mounts': [{'Source': f'/data/vol-{i}', 'Destination': f'/app/data-{i}', 'Mode': 'rw'} for i in range(count)],
        },
        'changed': True,
    }


def _res_generic(rng, noise_level):
    count = _noise_line_count(rng, noise_level) // 10
    count = max(1, count)
    return {
        'changed': rng.choice([True, False]),
        'msg': 'Task completed successfully',
        'result': {f'key_{i}': f'value_{rng.randint(1, 10000)}' for i in range(count)},
        'rc': 0,
    }


def generate_res(module, rng):
    """Return a realistic res dict for event_data based on module type.

    Noise level (small/medium/large) is picked randomly per call, weighted
    towards smaller sizes (50% small, 35% medium, 15% large) to match the
    distribution seen in real controller deployments.
    """
    noise_level = _pick_noise_level(rng)
    if module in _COMMAND_MODULES:
        return _res_command(rng, noise_level)
    if module in _PACKAGE_MODULES:
        return _res_package(rng, noise_level)
    if module in _FILE_MODULES:
        return _res_file(rng)
    if module in _SERVICE_MODULES:
        return _res_service(rng)
    if module in _CLOUD_MODULES:
        return _res_cloud(rng, noise_level)
    if module in _DB_MODULES:
        return _res_db(rng)
    if module in _CONTAINER_MODULES:
        return _res_container(rng, noise_level)
    return _res_generic(rng, noise_level)


# Database connection will be imported from Django after prepare() is called
_db_connection = None


def get_db_connection():
    """Get the Django database connection."""
    global _db_connection
    if _db_connection is None:
        from django.db import connection

        _db_connection = connection
    return _db_connection


def parse_id(result):
    """Parse the ID from a database result.

    Args:
        result: List of tuples from cursor.fetchall()

    Returns:
        First ID value or None
    """
    if result and len(result) > 0 and len(result[0]) > 0:
        return result[0][0]
    return None


def parse_ids(result):
    """Parse multiple IDs from a database result.

    Args:
        result: List of tuples from cursor.fetchall()

    Returns:
        List of ID values
    """
    return [row[0] for row in result if row]


def run(sql_script):
    """Execute SQL script using Django database connection.

    Returns the fetchall() result for SELECT queries or None for other queries.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Execute the SQL (may contain multiple statements)
            cursor.execute(sql_script)

            # Try to fetch results if this was a SELECT/RETURNING query
            try:
                result = cursor.fetchall()
                return result
            except Exception:
                # No results to fetch (DELETE, INSERT without RETURNING, etc.)
                return None

    except Exception as e:
        print(f'ERROR: Exception while executing SQL: {e}')
        import traceback

        traceback.print_exc()
        return None


# =============================================================================
# DELETE FUNCTIONS
# Order matters due to foreign key constraints - delete children before parents
# =============================================================================


def delete_job_events():
    """Delete all job events."""
    sql = """
    DELETE FROM _unpartitioned_main_jobevent;
    DELETE FROM main_jobevent;
    """
    print('Deleting main_jobevent...')
    return run(sql)


def delete_job_host_summaries():
    """Delete all job host summaries."""
    sql = """
    DELETE FROM main_jobhostsummary;
    """
    print('Deleting main_jobhostsummary...')
    return run(sql)


def delete_jobs():
    """Delete all main jobs."""
    sql = """
    DELETE FROM main_job;
    """
    print('Deleting main_job...')
    return run(sql)


def delete_unified_job_credentials():
    """Delete all unified job credentials (many-to-many relationship table)."""
    sql = """
    DELETE FROM main_unifiedjob_credentials;
    """
    print('Deleting main_unifiedjob_credentials...')
    return run(sql)


def delete_unified_job_template_credentials():
    """Delete all unified job template credentials (many-to-many relationship table)."""
    sql = """
    DELETE FROM main_unifiedjobtemplate_credentials;
    """
    print('Deleting main_unifiedjobtemplate_credentials...')
    return run(sql)


def delete_credentials():
    """Delete all credentials."""
    sql = """
    DELETE FROM main_credential;
    """
    print('Deleting main_credential...')
    return run(sql)


def delete_unified_jobs():
    """Delete all unified jobs."""
    sql = """
    DELETE FROM main_unifiedjob;
    """
    print('Deleting main_unifiedjob...')
    return run(sql)


def delete_job_templates():
    """Delete all job templates."""
    sql = """
    DELETE FROM main_jobtemplate;
    """
    print('Deleting main_jobtemplate...')
    return run(sql)


def delete_projects():
    """Delete all projects."""
    sql = """
    DELETE FROM main_project;
    """
    print('Deleting main_project...')
    return run(sql)


def delete_unified_job_templates():
    """Delete all unified job templates."""
    sql = """
    DELETE FROM main_unifiedjobtemplate;
    """
    print('Deleting main_unifiedjobtemplate...')
    return run(sql)


def delete_hosts():
    """Delete all hosts."""
    sql = """
    DELETE FROM main_host;
    """
    print('Deleting main_host...')
    return run(sql)


def delete_instances():
    """Delete all instances."""
    sql = """
    DELETE FROM main_instance;
    """
    print('Deleting main_instance...')
    return run(sql)


def delete_inventories():
    """Delete all inventories."""
    sql = """
    DELETE FROM main_inventory;
    """
    print('Deleting main_inventory...')
    return run(sql)


def delete_organizations():
    """Delete all organizations."""
    sql = """
    DELETE FROM main_organization;
    """
    print('Deleting main_organization...')
    return run(sql)


def delete_execution_environments():
    """Delete all execution environments."""
    sql = """
    DELETE FROM main_executionenvironment;
    """
    print('Deleting main_executionenvironment...')
    return run(sql)


def delete_all():
    """
    Delete all data from tables in correct order (respecting foreign key constraints).

    Order: job_events -> job_host_summaries -> jobs ->
           unified_job_credentials (many-to-many) -> unified_jobs ->
           unified_job_template_credentials (many-to-many) ->
           job_templates -> projects -> unified_job_templates ->
           credentials -> hosts -> instances -> inventories -> organizations ->
           execution_environments
    """
    print('=== Deleting all performance test data ===')

    delete_job_events()
    delete_job_host_summaries()
    delete_jobs()
    delete_unified_job_credentials()  # Delete many-to-many table before unified_jobs
    delete_unified_jobs()
    delete_unified_job_template_credentials()  # Delete many-to-many table before unified_job_templates
    delete_job_templates()
    delete_projects()
    delete_unified_job_templates()
    delete_credentials()  # Delete credentials before organizations
    delete_hosts()
    delete_instances()
    delete_inventories()
    delete_organizations()
    delete_execution_environments()

    print('=== All data deleted ===')


# =============================================================================
# CREATE FUNCTIONS
# Order matters due to foreign key constraints - create parents before children
# =============================================================================


def create_organization(name='Perf Test Organization'):
    """Create an organization and return its auto-generated ID."""
    sql = f"""
    INSERT INTO main_organization (created, modified, name, description, max_hosts)
    VALUES (NOW(), NOW(), '{name}', 'Performance testing organization', 0)
    RETURNING id;
    """
    print(f'Creating organization: {name}...')
    output = run(sql)
    org_id = parse_id(output)
    print(f'Created organization with ID: {org_id}')
    return org_id


def create_inventory(name='Perf Test Inventory', org_id=None):
    """Create an inventory and return its auto-generated ID."""
    sql = f"""
    INSERT INTO main_inventory (
        created, modified, name, description, organization_id, kind, host_filter, variables,
        has_active_failures, total_hosts, hosts_with_active_failures, total_groups,
        has_inventory_sources, total_inventory_sources, inventory_sources_with_failures,
        pending_deletion, prevent_instance_group_fallback
    )
    VALUES (
        NOW(), NOW(), '{name}', 'Performance testing inventory', {org_id}, '', NULL, '',
        FALSE, 0, 0, 0,
        FALSE, 0, 0,
        FALSE, FALSE
    )
    RETURNING id;
    """
    print(f'Creating inventory: {name}...')
    output = run(sql)
    inventory_id = parse_id(output)
    print(f'Created inventory with ID: {inventory_id}')
    return inventory_id


def create_project(name='Perf Test Project', org_id=None):
    """Create a project (via unified_job_template) and return its auto-generated ID."""
    # First create the unified job template entry and get its ID
    # Note: organization_id is on unifiedjobtemplate, not on project
    sql_ujt = f"""
    INSERT INTO main_unifiedjobtemplate (
        created, modified, name, description, polymorphic_ctype_id,
        last_job_failed, status, organization_id, org_unique
    )
    VALUES (
        NOW(), NOW(), '{name}', 'Performance testing project',
        (SELECT id FROM django_content_type WHERE app_label = 'main' AND model = 'project'),
        FALSE, 'never updated', {org_id}, FALSE
    )
    RETURNING id;
    """
    print(f'Creating unified job template for project: {name}...')
    output = run(sql_ujt)
    project_id = parse_id(output)

    # Then create the project entry using the same ID
    sql_project = f"""
    INSERT INTO main_project (
        unifiedjobtemplate_ptr_id, scm_type, scm_url, local_path,
        scm_branch, scm_clean, scm_delete_on_update, scm_update_on_launch,
        scm_update_cache_timeout, timeout, scm_revision, playbook_files,
        inventory_files, scm_refspec, allow_override, scm_track_submodules
    )
    VALUES (
        {project_id}, 'git', 'https://github.com/example/repo.git', '/var/lib/awx/projects/perf_test',
        'main', FALSE, FALSE, FALSE,
        0, 0, '', '[]'::jsonb,
        '[]'::jsonb, '', FALSE, FALSE
    )
    RETURNING unifiedjobtemplate_ptr_id;
    """
    print(f'Creating project: {name}...')
    run(sql_project)
    print(f'Created project with ID: {project_id}')
    return project_id


def create_job_templates(project_id, inventory_id, template_count=10, unique_suffix=None):
    """Create multiple job templates and return dict mapping IDs to names.

    Args:
        project_id: Project ID to link templates to
        inventory_id: Inventory ID to link templates to
        template_count: Number of templates to create
        unique_suffix: Optional unique suffix for template names
    """
    print(f'Creating {template_count} job templates...')
    templates = {}  # {template_id: template_name}
    suffix = f' {unique_suffix}' if unique_suffix else ''

    for i in range(template_count):
        template_name = f'Perf Test Template {i}{suffix}'

        # First create the unified job template entry
        sql_ujt = f"""
        INSERT INTO main_unifiedjobtemplate (
            created, modified, name, description, polymorphic_ctype_id,
            last_job_failed, status, org_unique
        )
        VALUES (
            NOW(), NOW(), '{template_name}', 'Performance testing job template',
            (SELECT id FROM django_content_type WHERE app_label = 'main' AND model = 'jobtemplate'),
            FALSE, 'never updated', FALSE
        )
        RETURNING id;
        """
        output = run(sql_ujt)
        template_id = parse_id(output)

        # Then create the job template entry with all required fields
        sql_jt = f"""
        INSERT INTO main_jobtemplate (
            unifiedjobtemplate_ptr_id, job_type, playbook, forks, "limit", verbosity,
            extra_vars, job_tags, force_handlers, skip_tags, start_at_task,
            become_enabled, host_config_key, ask_variables_on_launch, survey_enabled,
            survey_spec, inventory_id, project_id, ask_limit_on_launch,
            ask_inventory_on_launch, ask_credential_on_launch, ask_job_type_on_launch,
            ask_tags_on_launch, allow_simultaneous, ask_skip_tags_on_launch,
            timeout, use_fact_cache, ask_verbosity_on_launch, ask_diff_mode_on_launch,
            diff_mode, job_slice_count, ask_scm_branch_on_launch, scm_branch,
            webhook_key, webhook_service, ask_execution_environment_on_launch,
            ask_forks_on_launch, ask_instance_groups_on_launch, ask_job_slice_count_on_launch,
            ask_labels_on_launch, ask_timeout_on_launch, prevent_instance_group_fallback
        )
        VALUES (
            {template_id}, 'run', 'site.yml', 5, '', 0,
            '', '', FALSE, '', '',
            FALSE, '', FALSE, FALSE,
            '{{}}'::jsonb, {inventory_id}, {project_id}, FALSE,
            FALSE, FALSE, FALSE,
            FALSE, FALSE, FALSE,
            0, FALSE, FALSE, FALSE,
            FALSE, 1, FALSE, '',
            '', '', FALSE,
            FALSE, FALSE, FALSE,
            FALSE, FALSE, FALSE
        )
        RETURNING unifiedjobtemplate_ptr_id;
        """
        run(sql_jt)
        templates[template_id] = template_name

    print(f'Created {template_count} job templates with IDs: {list(templates.keys())}')
    return templates


def create_hosts(inventory_id=None, host_count=1000, unique_suffix=None):
    """Create multiple hosts for an inventory and return list of auto-generated IDs.

    Args:
        inventory_id: Inventory ID to link hosts to
        host_count: Number of hosts to create
        unique_suffix: Optional unique suffix for host names
    """
    print(f'Creating {host_count} hosts for inventory {inventory_id}...')
    suffix = f'-{unique_suffix}' if unique_suffix else ''

    # Generate bulk insert SQL for hosts (let DB auto-generate IDs)
    values = []
    for i in range(1, host_count + 1):
        host_name = f'host-{i}{suffix}.example.com'

        # Build variables JSON with ansible_host and ansible_connection
        variables = json.dumps({'ansible_host': host_name, 'ansible_connection': 'ssh'}).replace("'", "''")  # Escape single quotes for SQL

        values.append(f"(NOW(), NOW(), '{host_name}', 'Performance test host {i}', {inventory_id}, '{variables}', TRUE, '', '{{}}'::jsonb)")

    sql = f"""
    INSERT INTO main_host (created, modified, name, description, inventory_id, variables, enabled, instance_id, ansible_facts)
    VALUES {','.join(values)}
    RETURNING id;
    """
    output = run(sql)
    host_ids = parse_ids(output)
    print(f'Created {host_count} hosts')
    return host_ids


def create_job(
    name='Perf Test Job',
    inventory_id=None,
    project_id=None,
    org_id=None,
    job_index=0,
    job_template_id=None,
    start_date=None,
    end_date=None,
    execution_environment_id=None,
    installed_collections=None,
):
    """Create a job (via unified_job) and return its auto-generated ID and timestamps."""
    # Get deterministic timestamps for this job
    created, started, finished = get_job_timestamps(job_index, start_date, end_date)
    elapsed = (finished - started).total_seconds()

    created_str = created.strftime('%Y-%m-%d %H:%M:%S+00')
    started_str = started.strftime('%Y-%m-%d %H:%M:%S+00')
    finished_str = finished.strftime('%Y-%m-%d %H:%M:%S+00')

    ujt_value = job_template_id if job_template_id else 'NULL'
    ee_value = execution_environment_id if execution_environment_id else 'NULL'
    collections_sql = f"'{json.dumps(installed_collections)}'::jsonb" if installed_collections else "'[]'::jsonb"

    # First create the unified job entry and get its ID
    sql_uj = f"""
    INSERT INTO main_unifiedjob (
        created, modified, name, description, polymorphic_ctype_id,
        launch_type, cancel_flag, status, failed, started, finished, elapsed,
        job_args, job_cwd, job_explanation, start_args, result_traceback,
        celery_task_id, execution_node, emitted_events, controller_node,
        dependencies_processed, organization_id, installed_collections,
        ansible_version, task_impact, job_env, unified_job_template_id,
        execution_environment_id
    )
    VALUES (
        '{created_str}', '{created_str}', '{name}', 'Performance testing job',
        (SELECT id FROM django_content_type WHERE app_label = 'main' AND model = 'job'),
        'manual', FALSE, 'successful', FALSE, '{started_str}', '{finished_str}', {elapsed},
        '', '', '', '', '',
        '', 'localhost', 0, '',
        TRUE, {org_id}, {collections_sql},
        '2.15.0', 1, '{{}}'::jsonb, {ujt_value},
        {ee_value}
    )
    RETURNING id;
    """
    output = run(sql_uj)
    job_id = parse_id(output)

    # Then create the job entry using the same ID
    sql_job = f"""
    INSERT INTO main_job (
        unifiedjob_ptr_id, job_type, playbook, forks, "limit", verbosity,
        extra_vars, job_tags, force_handlers, skip_tags, start_at_task,
        become_enabled, inventory_id, project_id, allow_simultaneous,
        artifacts, timeout, scm_revision, use_fact_cache, diff_mode,
        job_slice_count, job_slice_number, scm_branch, webhook_guid,
        webhook_service, survey_passwords, job_template_id, event_queries_processed
    )
    VALUES (
        {job_id}, 'run', 'site.yml', 5, '', 0,
        '', '', FALSE, '', '',
        FALSE, {inventory_id}, {project_id}, FALSE,
        '', 0, '', FALSE, FALSE,
        1, 0, 'main', '',
        '', '{{}}'::jsonb, {ujt_value}, FALSE
    )
    RETURNING unifiedjob_ptr_id;
    """
    run(sql_job)
    print(f'Created job {job_index}')

    # Return job_id, created timestamp (needed for events), and finished timestamp (needed for job host summaries)
    return job_id, created, finished


def create_job_host_summaries(job_id, host_count, job_created, job_finished, unique_suffix=None):
    """Create job host summaries for all hosts (batch insert).

    Host names are generated using the same pattern as create_hosts: host-{i}-{suffix}.example.com

    Args:
        job_id: Job ID to link summaries to
        host_count: Number of host summaries to create
        job_created: Job creation timestamp to use for created date
        job_finished: Job finished timestamp to use for modified date (aligns with real AWX behavior)
        unique_suffix: Optional unique suffix for host names (must match create_hosts suffix)
    """
    print(f'Creating {host_count} job host summaries for job {job_id}...')
    suffix = f'-{unique_suffix}' if unique_suffix else ''

    values = []
    for i in range(1, host_count + 1):
        host_name = f'host-{i}{suffix}.example.com'
        # Generate random task counts
        ok = random.randint(5, 50)
        changed = random.randint(0, 10)
        failures = random.randint(0, 3)
        dark = random.randint(0, 2)  # unreachable
        skipped = random.randint(0, 15)
        ignored = random.randint(0, 5)
        rescued = random.randint(0, 2)
        processed = 1
        failed = failures > 0 or dark > 0

        values.append(
            f"('{job_created}', '{job_finished}', '{host_name}', {changed}, {dark}, {failures}, "
            f'{ok}, {processed}, {skipped}, {str(failed).upper()}, NULL, '
            f'{job_id}, {ignored}, {rescued})'
        )

    sql = f"""
    INSERT INTO main_jobhostsummary (
        created, modified, host_name, changed, dark, failures,
        ok, processed, skipped, failed, host_id,
        job_id, ignored, rescued
    )
    VALUES {','.join(values)};
    """
    run(sql)
    print(f'Created {host_count} job host summaries')


# Module definitions for job events - mix of different collection sources
# Random seed for deterministic generation
RANDOM_SEED = 42


def create_jobevent_partitions(start_date, end_date):
    """Create hourly partitions for main_jobevent (batch SQL)."""
    print(f'Creating jobevent partitions from {start_date} to {end_date}...')

    # Build all CREATE TABLE statements in one batch
    statements = []
    current = start_date
    while current < end_date:
        next_hour = current + timedelta(hours=1)
        partition_name = f'main_jobevent_{current.strftime("%Y%m%d_%H")}'

        statements.append(
            f'CREATE TABLE IF NOT EXISTS {partition_name} '
            f'PARTITION OF main_jobevent '
            f"FOR VALUES FROM ('{current.strftime('%Y-%m-%d %H:%M:%S')}+00') "
            f"TO ('{next_hour.strftime('%Y-%m-%d %H:%M:%S')}+00')"
        )
        current = next_hour

    # Execute all statements in one batch
    sql = ';\n'.join(statements) + ';'
    run(sql)

    print(f'Created {len(statements)} hourly partitions')


def get_job_timestamps(job_index, start_date, end_date):
    """Generate deterministic job timestamps within the given date range.

    Returns (created, started, finished) timestamps.
    """
    rng = random.Random(RANDOM_SEED + job_index)

    # Job created: random time within the date range
    total_seconds = int((end_date - start_date).total_seconds())
    created_offset = rng.randint(0, total_seconds - 7200)  # Leave room for job duration
    created = start_date + timedelta(seconds=created_offset)

    # Job started: 1-60 minutes after created (queue wait time)
    wait_seconds = rng.randint(60, 3600)
    started = created + timedelta(seconds=wait_seconds)

    # Job finished: 1-60 minutes after started (job duration)
    duration_seconds = rng.randint(60, 3600)
    finished = started + timedelta(seconds=duration_seconds)

    return created, started, finished


def create_job_events(job_id, host_ids, task_count=50, job_index=0, job_created=None, unique_suffix=None):
    """Create job events for all hosts (batch insert).

    Generates realistic events with:
    - Same task_uuid across all hosts (task runs on all hosts)
    - Each task has one module, but different outcomes per host
    - Mix of success, failed, skipped, unreachable events
    - Some hosts retry (failed then ok with same task_uuid)
    - Realistic event_data['res'] payload (noise level picked randomly per task)
    - ~40% of tasks run inside a role (produces role_stats in rollup output)

    Structure: task -> host -> outcome
    Each task runs the same module on all hosts, but each host can have different outcome.

    Host names are generated using the same pattern as create_hosts: host-{i}-{suffix}.example.com

    Args:
        job_id: Job ID to link events to
        host_ids: List of host IDs to use in the events
        task_count: Number of tasks per job
        job_index: Used for deterministic random seed (not job_id which changes each run)
        job_created: Timestamp from the job (used for partitioning)
        unique_suffix: Optional unique suffix for host names (must match create_hosts suffix)
    """
    # Use deterministic random based on job_index (not job_id which changes)
    rng = random.Random(RANDOM_SEED + job_index)

    # Format job_created for SQL
    job_created_str = job_created.strftime('%Y-%m-%d %H:%M:%S+00')

    host_count = len(host_ids)
    print(f'Creating job events for job {job_id} ({task_count} tasks x {host_count} hosts)...')
    suffix = f'-{unique_suffix}' if unique_suffix else ''

    values = []
    counter = 0

    # Task start time for calculating durations
    task_start_time = job_created

    # Loop over tasks first - each task has same UUID across all hosts
    for task_idx in range(1, task_count + 1):
        # Generate deterministic task_uuid based on job_index and task_idx
        task_uuid = str(uuid.UUID(int=RANDOM_SEED * 1000000 + job_index * 10000 + task_idx))
        task_name = f'Task {task_idx}'

        # Pick one module for this task (same module runs on all hosts)
        module = MODULES[rng.randint(0, len(MODULES) - 1)]

        # Simple duration calculation
        task_duration = 1.5
        start = task_start_time
        end = start + timedelta(seconds=task_duration)
        task_start_time = end  # Next task starts when this one ends

        # Role: ~40% of tasks run inside a role; consistent across all hosts for this task
        task_role = generate_role(rng)

        # Build event_data JSON with required dictionary fields + realistic res payload
        res = generate_res(module, rng)
        event_data_dict = {
            'task_action': module,
            'resolved_action': module,
            'task': task_name,
            'play': 'Main Play',
            'task_uuid': task_uuid,
            'duration': task_duration,
            'start': start.isoformat(),
            'end': end.isoformat(),
            'ignore_errors': False,
            'res': res,
        }
        event_data = json.dumps(event_data_dict).replace("'", "''")  # Escape single quotes for SQL

        # Loop over hosts - each host gets different outcome for this task
        for host_idx, host_id in enumerate(host_ids, 1):
            host_name = f'host-{host_idx}{suffix}.example.com'

            # Decide event outcome with realistic distribution per host
            # 70% clean success, 10% skipped, 15% failed then retry success, 5% failed/unreachable
            outcome = rng.random()

            if outcome < 0.70:
                # Clean success
                changed = rng.choice([True, False])
                events_for_host = [('runner_on_ok', False, changed)]
            elif outcome < 0.80:
                # Skipped
                events_for_host = [('runner_on_skipped', False, False)]
            elif outcome < 0.95:
                # Failed then retry success (2 events for same task on this host)
                changed = rng.choice([True, False])
                events_for_host = [
                    ('runner_on_failed', True, False),
                    ('runner_on_ok', False, changed),
                ]
            else:
                # Failed or unreachable (no retry)
                if rng.random() < 0.7:
                    events_for_host = [('runner_on_failed', True, False)]
                else:
                    events_for_host = [('runner_on_unreachable', True, False)]

            # Create events for this host
            for event_type, failed, changed in events_for_host:
                counter += 1
                values.append(
                    f"('{job_created_str}', '{job_created_str}', '{event_type}', '{event_data}', {str(failed).upper()}, "
                    f"{str(changed).upper()}, '{host_name}', 'Main Play', '{task_role}', '{task_name}', "
                    f"{counter}, {host_id}, {job_id}, '{task_uuid}', '', 0, 'site.yml', 0, '', 0, '{job_created_str}')"
                )

    # Insert into main_jobevent (partitioned table, requires job_created)
    sql = f"""
    INSERT INTO main_jobevent (
        created, modified, event, event_data, failed,
        changed, host_name, play, role, task,
        counter, host_id, job_id, uuid, parent_uuid,
        end_line, playbook, start_line, stdout, verbosity, job_created
    )
    VALUES {','.join(values)};
    """
    run(sql)
    print(f'Created {len(values)} job events ({task_count} tasks x {host_count} hosts)')


# Roles in namespace.collection.rolename (collection role) or namespace.rolename (standalone role) format.
# extract_role_name() recognises both; collection roles also produce collection_source lookups.
_ROLES = [
    # Collection roles — will also appear in collection_stats
    'redhat.rhel_system_roles.network',
    'redhat.rhel_system_roles.selinux',
    'redhat.rhel_system_roles.timesync',
    'redhat.rhel_system_roles.firewall',
    'redhat.rhel_system_roles.storage',
    'ansible.posix.acl',
    'community.general.docker',
    'community.mysql.server',
    'community.postgresql.server',
    'community.grafana.grafana',
    'community.kubernetes.helm',
    'amazon.aws.iam',
    'azure.azcollection.vm',
    # Standalone roles — namespace.rolename
    'geerlingguy.apache',
    'geerlingguy.nginx',
    'geerlingguy.mysql',
    'geerlingguy.java',
    'debops.nginx',
    'debops.postgresql',
    'debops.docker',
    'elastic.elasticsearch',
]
# Probability that a given task is executed inside a role (rest run at play level)
_ROLE_PROBABILITY = 0.40


def generate_role(rng):
    """Return a role name for a task, or empty string if the task is not in a role."""
    if rng.random() < _ROLE_PROBABILITY:
        return rng.choice(_ROLES)
    return ''


_COLLECTION_NAMESPACES = ['ansible', 'community', 'redhat', 'amazon', 'google', 'azure', 'cisco', 'f5', 'netapp', 'vmware']
_COLLECTION_NAMES = ['posix', 'general', 'windows', 'network', 'cloud', 'storage', 'security', 'utils', 'netcommon', 'platform']


def _random_installed_collections(seed, count=50):
    """Generate a random installed_collections dict for an EE."""
    rng = random.Random(seed)
    collections = {}
    while len(collections) < count:
        key = f'{rng.choice(_COLLECTION_NAMESPACES)}.{rng.choice(_COLLECTION_NAMES)}'
        if key not in collections:
            collections[key] = {'version': f'{rng.randint(1, 5)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}'}
    return collections


def create_execution_environment(name, image):
    """Create an execution environment row and return its ID."""
    sql = f"""
    INSERT INTO main_executionenvironment (
        created, modified, name, description, image, managed, pull
    )
    VALUES (
        NOW(), NOW(), '{name}', '', '{image}', FALSE, 'missing'
    )
    RETURNING id;
    """
    output = run(sql)
    ee_id = parse_id(output)
    if ee_id is None:
        raise RuntimeError(f'Failed to create execution environment: {name}')
    return ee_id


def create_execution_environments(count=100, unique_suffix=None):
    """Create execution environments and return list of (ee_id, installed_collections) tuples."""
    results = []
    for i in range(count):
        name = f'Perf Test EE {i + 1} {unique_suffix}'
        image = f'registry.example.com/perf-test-ee-{i + 1}-{unique_suffix}:latest'
        ee_id = create_execution_environment(name, image)
        results.append((ee_id, _random_installed_collections(seed=i)))
    return results


def create_credentials():
    """Create one credential per built-in type and return their IDs."""
    result = run("SELECT id FROM main_credentialtype WHERE managed = TRUE AND name IN ('Machine', 'Vault', 'Amazon Web Services', 'Network');")
    if not result:
        raise RuntimeError('Failed to fetch built-in credential types')
    credential_ids = []
    for (ct_id,) in result:
        sql = f"""
        INSERT INTO main_credential (created, modified, name, description, credential_type_id, inputs, managed)
        VALUES (NOW(), NOW(), 'Perf Test Credential', '', {ct_id}, '{{}}'::jsonb, FALSE)
        RETURNING id;
        """
        cred_id = parse_id(run(sql))
        if cred_id is None:
            raise RuntimeError(f'Failed to create credential for type {ct_id}')
        credential_ids.append(cred_id)
    return credential_ids


def create_job_credentials(job_id, credential_ids):
    """Link credentials to a job."""
    if not credential_ids:
        return
    values = ', '.join(f'({job_id}, {cred_id})' for cred_id in credential_ids)
    run(f'INSERT INTO main_unifiedjob_credentials (unifiedjob_id, credential_id) VALUES {values};')


def create_instance(version='4.5.0', node_type='control'):
    """Create a controller instance row for controller_version_service."""
    instance_uuid = str(uuid.uuid4())
    sql = f"""
    INSERT INTO main_instance (
        created, modified, uuid, hostname, version, node_type,
        enabled, managed_by_policy, managed, ip_address,
        cpu, memory, cpu_capacity, mem_capacity,
        capacity, capacity_adjustment, errors, node_state
    )
    VALUES (
        NOW(), NOW(), '{instance_uuid}', 'perf-test-controller-{instance_uuid}', '{version}', '{node_type}',
        TRUE, TRUE, FALSE, '',
        0, 0, 0, 0,
        0, 1.0, '', 'ready'
    )
    RETURNING id;
    """
    output = run(sql)
    instance_id = parse_id(output)
    if instance_id is None:
        raise RuntimeError('Failed to create main_instance row')
    return instance_id


if __name__ == '__main__':
    delete_all()
