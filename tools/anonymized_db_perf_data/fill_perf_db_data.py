#!/usr/bin/env python3
"""Fill the database with performance test data."""

import argparse
import random
import uuid

from datetime import datetime

from helpers import (
    create_credentials,
    create_execution_environments,
    create_hosts,
    create_indirect_managed_node_audits,
    create_instance,
    create_inventory,
    create_job,
    create_job_credentials,
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
    result = run('SELECT COUNT(*) FROM main_indirectmanagednodeaudit;')
    indirect_count = result[0][0] if result else 0
    print(f'Total hosts: {host_count}')
    print(f'Total jobs: {job_count}')
    print(f'Total job host summaries: {jhs_count}')
    print(f'Total job events: {event_count}')
    print(f'Total indirect managed node audit records: {indirect_count}')


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

    # Create one controller instance (needed by controller_version_service collector)
    create_instance()

    # Create execution environments with distinct installed_collections sets
    ee_list = create_execution_environments(unique_suffix=unique_suffix)

    # Create one credential per built-in type
    credential_ids = create_credentials()

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
        'ee_list': ee_list,  # [(ee_id, installed_collections), ...]
        'credential_ids': credential_ids,
    }


def fill_perf_db_data(
    host_count=10, job_count=5, task_count=50, template_count=10, start_date=None, end_date=None, no_events=False, indirect_count=100
):
    """Fill the database with performance test data.

    Note: This function does NOT clean existing data. Use clean_all_data.py to clean before filling.

    Args:
        host_count: Number of hosts to create
        job_count: Number of jobs to create
        task_count: Number of tasks per job
        template_count: Number of job templates to create
        start_date: Start of the date range for job timestamps
        end_date: End of the date range for job timestamps
        no_events: Skip generating job events entirely
        indirect_count: Number of indirect managed node audit records to create
    """
    print(
        f'=== Configuration: {host_count} hosts, {job_count} jobs, {task_count} tasks/job, '
        f'{template_count} templates, {indirect_count} indirect nodes ==='
    )

    create_jobevent_partitions(start_date, end_date)

    init_data = fill_init_data(host_count=host_count, task_count=task_count, template_count=template_count)

    job_ids = []
    for i in range(job_count):
        job_id = fill_job(init_data, i, start_date, end_date, no_events=no_events)
        job_ids.append(job_id)

    create_indirect_managed_node_audits(
        job_ids=job_ids,
        host_ids=init_data['host_ids'],
        inventory_id=init_data['inventory_id'],
        org_id=init_data['org_id'],
        indirect_count=indirect_count,
        unique_suffix=init_data['unique_suffix'],
    )

    print_counts()


def fill_job_data(init_data, job_index, start_date, end_date):
    """Create a job using the init_data IDs. Returns (job_id, job_created, job_finished)."""
    templates = init_data['templates']
    template_id = random.choice(list(templates.keys()))
    template_name = templates[template_id]

    # Cycle through EEs so jobs are distributed across them
    ee_list = init_data['ee_list']
    ee_id, installed_collections = ee_list[job_index % len(ee_list)]

    job_id, job_created, job_finished = create_job(
        name=template_name,
        inventory_id=init_data['inventory_id'],
        project_id=init_data['project_id'],
        org_id=init_data['org_id'],
        job_index=job_index,
        job_template_id=template_id,
        start_date=start_date,
        end_date=end_date,
        execution_environment_id=ee_id,
        installed_collections=installed_collections,
    )
    return job_id, job_created, job_finished


def fill_jobhostsummary(init_data, job_id, job_created, job_finished):
    create_job_host_summaries(job_id, init_data['host_count'], job_created, job_finished, unique_suffix=init_data.get('unique_suffix'))


def fill_jobevent(init_data, job_id, job_index, job_created):
    create_job_events(job_id, init_data['host_ids'], init_data['task_count'], job_index, job_created, unique_suffix=init_data.get('unique_suffix'))


def fill_job(init_data, job_index, start_date, end_date, no_events=False):
    job_id, job_created, job_finished = fill_job_data(init_data, job_index, start_date, end_date)
    fill_jobhostsummary(init_data, job_id, job_created, job_finished)
    create_job_credentials(job_id, init_data['credential_ids'])
    if not no_events:
        fill_jobevent(init_data, job_id, job_index, job_created)
    return job_id


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

    # Check for virtual environment and use it.. unless already using it
    venv_path = metrics_utility_path / '.venv'
    if not os.getenv('VIRTUAL_ENV') and venv_path.exists():
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
    parser.add_argument(
        '--since',
        type=str,
        default=None,
        help='Start of the datetime range for job timestamps (e.g. "2024-01-01" or "2024-01-01 03:00:00"). Default: 2024-01-01 00:00:00',
    )
    parser.add_argument(
        '--until',
        type=str,
        default=None,
        help='End of the datetime range for job timestamps (e.g. "2024-01-02" or "2024-01-01 06:00:00"). Default: 2024-01-31 23:59:59',
    )
    parser.add_argument('--no-events', action='store_true', default=False, help='Skip generating job events')
    parser.add_argument('--indirect-count', type=int, default=100, help='Number of indirect managed node audit records to create (default: 100)')

    args = parser.parse_args()

    def _parse_dt(value, default):
        if value is None:
            return default
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            parser.error(f'Invalid datetime format: {value!r}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS')

    start_date = _parse_dt(args.since, datetime(2024, 1, 1, 0, 0, 0))
    end_date = _parse_dt(args.until, datetime(2024, 1, 31, 23, 59, 59))

    if start_date >= end_date:
        parser.error(f'--since ({start_date}) must be before --until ({end_date})')

    fill_perf_db_data(
        host_count=args.host_count,
        job_count=args.job_count,
        task_count=args.task_count,
        template_count=args.template_count,
        start_date=start_date,
        end_date=end_date,
        no_events=args.no_events,
        indirect_count=args.indirect_count,
    )
