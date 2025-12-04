from helpers import (
    delete_all,
    create_organization,
    create_inventory,
    create_project,
    create_hosts,
    create_job,
    run,
)


def print_counts():
    """Print the count of hosts and jobs in the database."""
    print('=== Database counts ===')
    output = run("SELECT COUNT(*) FROM main_host;")
    host_count = int(output.strip().split('\n')[2].strip())
    output = run("SELECT COUNT(*) FROM main_job;")
    job_count = int(output.strip().split('\n')[2].strip())
    print(f'Total hosts: {host_count}')
    print(f'Total jobs: {job_count}')


def fill_init_data(host_count=10):
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
    }

def fill_perf_db_data():
    job_count = 20
    delete_all()

    init_data = fill_init_data()

    for i in range(job_count):
        fill_job(init_data, i)

    print_counts()

def fill_job_data(init_data, job_index):
    """Create a job using the init_data IDs."""
    job_id = create_job(
        name=f'Perf Test Job {job_index}',
        inventory_id=init_data['inventory_id'],
        project_id=init_data['project_id'],
        org_id=init_data['org_id'],
    )
    return job_id

def fill_jobhostsummary(init_data, job_id):
    return

def fill_jobevent(init_data, job_id):
    return

def fill_job(init_data, job_index):
    job_id = fill_job_data(init_data, job_index)
    fill_jobhostsummary(init_data, job_id)
    fill_jobevent(init_data, job_id)
    return

fill_perf_db_data()
