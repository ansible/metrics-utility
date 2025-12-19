#!/usr/bin/env python3
"""Fill the database with performance test data."""

import argparse
import random
import uuid

from helpers import (
    create_hosts,
    create_inventory,
    create_job,
    create_job_events,
    create_job_host_summaries,
    create_job_templates,
    create_jobevent_partitions,
    create_organization,
    create_project,
    run,
)


def print_counts():
    """Print the count of hosts, jobs, job host summaries, and job events in the database."""
    print('=== Database counts ===')
    result = run('SELECT COUNT(*) FROM main_host;')
    host_count = result[0][0] if result else 0
    result = run('SELECT COUNT(*) FROM main_job;')
    job_count = result[0][0] if result else 0
    result = run('SELECT COUNT(*) FROM main_jobhostsummary;')
    jhs_count = result[0][0] if result else 0
    result = run('SELECT COUNT(*) FROM main_jobevent;')
    event_count = result[0][0] if result else 0
    print(f'Total hosts: {host_count}')
    print(f'Total jobs: {job_count}')
    print(f'Total job host summaries: {jhs_count}')
    print(f'Total job events: {event_count}')


def fill_init_data(host_count=10, task_count=50, template_count=10, unique_suffix=None):
    """Create initial data: organization, inventory, project, job templates, and hosts.

    Args:
        host_count: Number of hosts to create
        task_count: Number of tasks per job
        template_count: Number of job templates to create
        unique_suffix: Optional unique suffix for entity names. If None, generates a random one.

    Returns dict with auto-generated IDs for created entities.
    """
    # Generate unique suffix if not provided (first 8 chars of UUID)
    if unique_suffix is None:
        unique_suffix = str(uuid.uuid4())[:8]

    print('=== Creating initial performance test data ===')
    print(f'Unique suffix: {unique_suffix}')

    # Create organization first (parent of inventory and project)
    org_id = create_organization(name=f'Perf Test Organization {unique_suffix}')

    # Create inventory (depends on organization)
    inventory_id = create_inventory(name=f'Perf Test Inventory {unique_suffix}', org_id=org_id)

    # Create project (depends on organization)
    project_id = create_project(name=f'Perf Test Project {unique_suffix}', org_id=org_id)

    # Create job templates (depends on project and inventory)
    templates = create_job_templates(project_id, inventory_id, template_count, unique_suffix)

    # Create hosts (depends on inventory)
    host_ids = create_hosts(inventory_id=inventory_id, host_count=host_count, unique_suffix=unique_suffix)

    print('=== Initial data created ===')
    print(f'Organization: Perf Test Organization {unique_suffix} (ID: {org_id})')
    print(f'Inventory: Perf Test Inventory {unique_suffix} (ID: {inventory_id})')
    print(f'Project: Perf Test Project {unique_suffix} (ID: {project_id})')
    print(f'Job Template IDs: {list(templates.keys())}')

    return {
        'org_id': org_id,
        'inventory_id': inventory_id,
        'project_id': project_id,
        'templates': templates,  # {template_id: template_name}
        'host_ids': host_ids,
        'host_count': host_count,
        'task_count': task_count,
        'unique_suffix': unique_suffix,
    }


def fill_perf_db_data(host_count=10, job_count=5, task_count=50, template_count=10):
    """Fill the database with performance test data.

    Note: This function does NOT clean existing data. Use clean_all_data.py to clean before filling.

    Args:
        host_count: Number of hosts to create
        job_count: Number of jobs to create
        task_count: Number of tasks per job
        template_count: Number of job templates to create
    """
    print(f'=== Configuration: {host_count} hosts, {job_count} jobs, {task_count} tasks/job, {template_count} templates ===')

    # Create partitions for January 2024 (required for partitioned main_jobevent)
    create_jobevent_partitions()

    init_data = fill_init_data(host_count=host_count, task_count=task_count, template_count=template_count)

    for i in range(job_count):
        fill_job(init_data, i)

    print_counts()


def fill_job_data(init_data, job_index):
    """Create a job using the init_data IDs. Returns (job_id, job_created, job_finished)."""
    # Randomly select a job template
    templates = init_data['templates']
    template_id = random.choice(list(templates.keys()))
    template_name = templates[template_id]

    job_id, job_created, job_finished = create_job(
        name=template_name,  # Use template name as job name (AWX behavior)
        inventory_id=init_data['inventory_id'],
        project_id=init_data['project_id'],
        org_id=init_data['org_id'],
        job_index=job_index,
        job_template_id=template_id,
    )
    return job_id, job_created, job_finished


def fill_jobhostsummary(init_data, job_id, job_created, job_finished):
    create_job_host_summaries(job_id, init_data['host_count'], job_created, job_finished, unique_suffix=init_data.get('unique_suffix'))


def fill_jobevent(init_data, job_id, job_index, job_created):
    create_job_events(job_id, init_data['host_ids'], init_data['task_count'], job_index, job_created, unique_suffix=init_data.get('unique_suffix'))


def fill_job(init_data, job_index):
    job_id, job_created, job_finished = fill_job_data(init_data, job_index)
    fill_jobhostsummary(init_data, job_id, job_created, job_finished)
    fill_jobevent(init_data, job_id, job_index, job_created)
    return


if __name__ == '__main__':
    import os
    import sys

    from pathlib import Path

    # Add current directory to path for imports
    current_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(current_dir))

    # Add metrics_utility to path and activate venv if available
    metrics_utility_path = current_dir.parent.parent
    sys.path.insert(0, str(metrics_utility_path))

    # Check for virtual environment and use it
    venv_path = metrics_utility_path / '.venv'
    if venv_path.exists():
        # Activate venv by updating PATH and VIRTUAL_ENV
        os.environ['VIRTUAL_ENV'] = str(venv_path)
        os.environ['PATH'] = f'{venv_path / "bin"}:{os.environ.get("PATH", "")}'
        # Add venv site-packages to sys.path
        site_packages = list(venv_path.glob('lib/python*/site-packages'))
        if site_packages:
            sys.path.insert(0, str(site_packages[0]))

    from metrics_utility import prepare  # noqa: E402

    # Initialize Django and database connection
    prepare()

    parser = argparse.ArgumentParser(description='Fill database with performance test data')
    parser.add_argument('--host-count', type=int, default=30, help='Number of hosts to create (default: 10)')
    parser.add_argument('--job-count', type=int, default=20, help='Number of jobs to create (default: 5)')
    parser.add_argument('--task-count', type=int, default=50, help='Number of tasks per job (default: 50)')
    parser.add_argument('--template-count', type=int, default=10, help='Number of job templates to create (default: 10)')

    args = parser.parse_args()

    fill_perf_db_data(
        host_count=args.host_count,
        job_count=args.job_count,
        task_count=args.task_count,
        template_count=args.template_count,
    )
