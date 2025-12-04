from helpers import (
    run,
    delete_all,
    create_organization,
    create_inventory,
    create_project,
    create_hosts,
)


def fill_init_data(host_count=1000):
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
    job_count = 100
    delete_all()

    fill_init_data()

    for i in range(job_count):
        fill_job()

    return

def fill_job_data():
    return

def fill_jobhostsummary():
    return

def fill_jobevent():
    return

def fill_job():
    fill_job_data()
    fill_jobhostsummary()
    fill_jobevent()
    return

fill_perf_db_data()
