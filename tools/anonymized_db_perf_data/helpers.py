import json
import random
import uuid

from datetime import datetime, timedelta

from modules import MODULES


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

    Order: job_events -> job_host_summaries -> jobs -> unified_jobs ->
           job_templates -> projects -> unified_job_templates ->
           hosts -> instances -> inventories -> organizations ->
           execution_environments
    """
    print('=== Deleting all performance test data ===')

    delete_job_events()
    delete_job_host_summaries()
    delete_jobs()
    delete_unified_jobs()
    delete_job_templates()
    delete_projects()
    delete_unified_job_templates()
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
        last_job_failed, status, organization_id
    )
    VALUES (
        NOW(), NOW(), '{name}', 'Performance testing project',
        (SELECT id FROM django_content_type WHERE app_label = 'main' AND model = 'project'),
        FALSE, 'never updated', {org_id}
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
            last_job_failed, status
        )
        VALUES (
            NOW(), NOW(), '{template_name}', 'Performance testing job template',
            (SELECT id FROM django_content_type WHERE app_label = 'main' AND model = 'jobtemplate'),
            FALSE, 'never updated'
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
        values.append(f"(NOW(), NOW(), '{host_name}', 'Performance test host {i}', {inventory_id}, '', TRUE, '', '{{}}'::jsonb)")

    sql = f"""
    INSERT INTO main_host (created, modified, name, description, inventory_id, variables, enabled, instance_id, ansible_facts)
    VALUES {','.join(values)}
    RETURNING id;
    """
    output = run(sql)
    host_ids = parse_ids(output)
    print(f'Created {host_count} hosts')
    return host_ids


def create_job(name='Perf Test Job', inventory_id=None, project_id=None, org_id=None, job_index=0, job_template_id=None):
    """Create a job (via unified_job) and return its auto-generated ID and timestamps."""
    # Get deterministic timestamps for this job
    created, started, finished = get_job_timestamps(job_index)
    elapsed = (finished - started).total_seconds()

    created_str = created.strftime('%Y-%m-%d %H:%M:%S+00')
    started_str = started.strftime('%Y-%m-%d %H:%M:%S+00')
    finished_str = finished.strftime('%Y-%m-%d %H:%M:%S+00')

    # unified_job_template_id can be NULL or reference a job template
    ujt_value = job_template_id if job_template_id else 'NULL'

    # First create the unified job entry and get its ID
    sql_uj = f"""
    INSERT INTO main_unifiedjob (
        created, modified, name, description, polymorphic_ctype_id,
        launch_type, cancel_flag, status, failed, started, finished, elapsed,
        job_args, job_cwd, job_explanation, start_args, result_traceback,
        celery_task_id, execution_node, emitted_events, controller_node,
        dependencies_processed, organization_id, installed_collections,
        ansible_version, task_impact, job_env, unified_job_template_id
    )
    VALUES (
        '{created_str}', '{created_str}', '{name}', 'Performance testing job',
        (SELECT id FROM django_content_type WHERE app_label = 'main' AND model = 'job'),
        'manual', FALSE, 'successful', FALSE, '{started_str}', '{finished_str}', {elapsed},
        '', '', '', '', '',
        '', 'localhost', 0, '',
        TRUE, {org_id}, '[]'::jsonb,
        '2.15.0', 1, '{{}}'::jsonb, {ujt_value}
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
        webhook_service, survey_passwords, job_template_id
    )
    VALUES (
        {job_id}, 'run', 'site.yml', 5, '', 0,
        '', '', FALSE, '', '',
        FALSE, {inventory_id}, {project_id}, FALSE,
        '', 0, '', FALSE, FALSE,
        1, 0, 'main', '',
        '', '{{}}'::jsonb, {ujt_value}
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

# Job date range: January 2024
JOB_DATE_START = datetime(2024, 1, 1, 0, 0, 0)
JOB_DATE_END = datetime(2024, 1, 31, 23, 59, 59)


def create_jobevent_partitions():
    """Create hourly partitions for main_jobevent for January 2024 (batch SQL)."""
    print('Creating jobevent partitions for January 2024...')

    # Create partitions for each hour in January 2024
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 2, 1, 0, 0, 0)

    # Build all CREATE TABLE statements in one batch
    statements = []
    current = start
    while current < end:
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

    print(f'Created {len(statements)} hourly partitions for January 2024')


def get_job_timestamps(job_index):
    """Generate deterministic job timestamps within January 2024.

    Returns (created, started, finished) timestamps.
    """
    rng = random.Random(RANDOM_SEED + job_index)

    # Job created: random time within January 2024
    total_seconds = int((JOB_DATE_END - JOB_DATE_START).total_seconds())
    created_offset = rng.randint(0, total_seconds - 7200)  # Leave room for job duration
    created = JOB_DATE_START + timedelta(seconds=created_offset)

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

    # Loop over tasks first - each task has same UUID across all hosts
    for task_idx in range(1, task_count + 1):
        # Generate deterministic task_uuid based on job_index and task_idx
        task_uuid = str(uuid.UUID(int=RANDOM_SEED * 1000000 + job_index * 10000 + task_idx))
        task_name = f'Task {task_idx}'

        # Pick one module for this task (same module runs on all hosts)
        module = MODULES[rng.randint(0, len(MODULES) - 1)]

        # Build event_data JSON for this task
        event_data = json.dumps(
            {
                'task_action': module,
                'resolved_action': module,
                'task': task_name,
                'play': 'Main Play',
                'task_uuid': task_uuid,  # Include task_uuid for collector to extract
            }
        ).replace("'", "''")  # Escape single quotes for SQL

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
                    f"{str(changed).upper()}, '{host_name}', 'Main Play', '', '{task_name}', "
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


if __name__ == '__main__':
    delete_all()
