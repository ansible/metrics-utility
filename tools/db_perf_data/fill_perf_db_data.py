from helpers import (
    create_hosts,
    create_inventory,
    create_job,
    create_job_events,
    create_job_host_summaries,
    create_jobevent_partitions,
    create_organization,
    create_project,
    delete_all,
    run,
)


def print_counts():
    """Print the count of hosts, jobs, job host summaries, and job events in the database."""
    print('=== Database counts ===')
    output = run('SELECT COUNT(*) FROM main_host;')
    host_count = int(output.strip().split('\n')[2].strip())
    output = run('SELECT COUNT(*) FROM main_job;')
    job_count = int(output.strip().split('\n')[2].strip())
    output = run('SELECT COUNT(*) FROM main_jobhostsummary;')
    jhs_count = int(output.strip().split('\n')[2].strip())
    output = run('SELECT COUNT(*) FROM main_jobevent;')
    event_count = int(output.strip().split('\n')[2].strip())
    print(f'Total hosts: {host_count}')
    print(f'Total jobs: {job_count}')
    print(f'Total job host summaries: {jhs_count}')
    print(f'Total job events: {event_count}')


def fill_init_data(host_count=10, task_count=50):
    """Create initial data: organization, inventory, project, and hosts.

    Returns dict with auto-generated IDs for created entities.
    """
    print('=== Creating initial performance test data ===')

    # Create organization first (parent of inventory and project)
    org_id = create_organization(name='Perf Test Organization')

    # Create inventory (depends on organization)
    inventory_id = create_inventory(name='Perf Test Inventory', org_id=org_id)

    # Create project (depends on organization)
    project_id = create_project(name='Perf Test Project', org_id=org_id)

    # Create hosts (depends on inventory)
    create_hosts(inventory_id=inventory_id, host_count=host_count)

    print('=== Initial data created ===')
    print(f'Organization ID: {org_id}')
    print(f'Inventory ID: {inventory_id}')
    print(f'Project ID: {project_id}')

    return {
        'org_id': org_id,
        'inventory_id': inventory_id,
        'project_id': project_id,
        'host_count': host_count,
        'task_count': task_count,
    }


def fill_perf_db_data():
    job_count = 5
    delete_all()

    # Create partitions for January 2024 (required for partitioned main_jobevent)
    create_jobevent_partitions()

    init_data = fill_init_data()

    for i in range(job_count):
        fill_job(init_data, i)

    print_counts()


def fill_job_data(init_data, job_index):
    """Create a job using the init_data IDs. Returns (job_id, job_created)."""
    job_id, job_created = create_job(
        name=f'Perf Test Job {job_index}',
        inventory_id=init_data['inventory_id'],
        project_id=init_data['project_id'],
        org_id=init_data['org_id'],
        job_index=job_index,
    )
    return job_id, job_created


def fill_jobhostsummary(init_data, job_id):
    create_job_host_summaries(job_id, init_data['host_count'])


def fill_jobevent(init_data, job_id, job_index, job_created):
    create_job_events(job_id, init_data['host_count'], init_data['task_count'], job_index, job_created)


def fill_job(init_data, job_index):
    job_id, job_created = fill_job_data(init_data, job_index)
    fill_jobhostsummary(init_data, job_id)
    fill_jobevent(init_data, job_id, job_index, job_created)
    return


fill_perf_db_data()
