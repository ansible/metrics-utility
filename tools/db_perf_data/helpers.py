import subprocess
import re


def parse_id(output):
    """Parse the ID from psql RETURNING output.

    Expected format:
     id
    ----
      1
    (1 row)
    """
    lines = output.strip().split('\n')
    for line in lines:
        line = line.strip()
        # Skip header lines and row count
        if line and not line.startswith('id') and not line.startswith('-') and not line.startswith('('):
            try:
                return int(line)
            except ValueError:
                continue
    return None


def run(sql_script):
    """Execute SQL script via docker exec to postgres container."""
    command = ['docker', 'exec', '-i', 'postgres', 'psql', '-U', 'awx']

    process = subprocess.run(command, input=sql_script.encode(), capture_output=True)

    print(process.stdout.decode())
    print(process.stderr.decode())

    return process.stdout.decode()


# =============================================================================
# DELETE FUNCTIONS
# Order matters due to foreign key constraints - delete children before parents
# =============================================================================


def delete_job_events():
    """Delete all job events."""
    sql = """
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


def create_hosts(inventory_id=None, host_count=1000):
    """Create multiple hosts for an inventory and return list of auto-generated IDs."""
    print(f'Creating {host_count} hosts for inventory {inventory_id}...')

    # Generate bulk insert SQL for hosts (let DB auto-generate IDs)
    values = []
    for i in range(1, host_count + 1):
        values.append(f"(NOW(), NOW(), 'host-{i}.example.com', 'Performance test host {i}', {inventory_id}, '', TRUE, '', '{{}}'::jsonb)")

    sql = f"""
    INSERT INTO main_host (created, modified, name, description, inventory_id, variables, enabled, instance_id, ansible_facts)
    VALUES {','.join(values)}
    RETURNING id;
    """
    output = run(sql)
    print(f'Created {host_count} hosts')
    return output


if __name__ == '__main__':
    delete_all()

