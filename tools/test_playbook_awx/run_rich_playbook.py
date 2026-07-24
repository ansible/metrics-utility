#!/usr/bin/env python
"""
Deploy rich_playbook.yml into a running AWX dev instance and launch it.

Steps:
  1. Copy rich_playbook.yml into the AWX container's manual project directory
  2. Create a manual project (scm_type="") pointing at that directory
  3. Create an inventory with N localhost hosts (default 5)
  4. Create a job template using the project + inventory
  5. Launch the job and wait for completion
  6. Print stdout

All resources are prefixed with "rich_test_" so they can be identified and
cleaned up. Re-running the script reuses existing resources by name.

Usage:
  python run_rich_playbook.py
  python run_rich_playbook.py --hosts 10
  python run_rich_playbook.py --url https://awx.example.com --user admin --password secret
  python run_rich_playbook.py --cleanup
"""

import argparse
import subprocess
import sys
import time

import requests

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

DEFAULT_URL = 'https://localhost:8043'
DEFAULT_USER = 'admin'
DEFAULT_PASSWORD = 'admin'
DEFAULT_CONTAINER = 'tools_awx_1'
DEFAULT_NUM_HOSTS = 5
POLL_INTERVAL = 3

PREFIX = 'rich_test'
PROJECT_NAME = f'{PREFIX}_project'
PROJECT_DIR = f'{PREFIX}_manual'
INVENTORY_NAME = f'{PREFIX}_inventory'
TEMPLATE_NAME = f'{PREFIX}_template'
PLAYBOOK_FILE = 'rich_playbook.yml'


class AWXClient:
    def __init__(self, base_url: str, user: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = (user, password)

    def _url(self, path: str) -> str:
        return f'{self.base_url}/api/v2/{path}'

    def get(self, path: str, **params) -> dict:
        resp = requests.get(self._url(path), params=params, auth=self.auth, verify=False)
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, data: dict) -> dict:
        resp = requests.post(self._url(path), json=data, auth=self.auth, verify=False)
        resp.raise_for_status()
        return resp.json()

    def delete(self, path: str) -> int:
        resp = requests.delete(self._url(path), auth=self.auth, verify=False)
        return resp.status_code

    def find_by_name(self, path: str, name: str) -> dict | None:
        data = self.get(path, name=name, page_size=50)
        for r in data.get('results', []):
            if r['name'] == name:
                return r
        return None

    def find_or_create(self, path: str, name: str, payload: dict) -> tuple[dict, bool]:
        existing = self.find_by_name(path, name)
        if existing:
            return existing, False
        return self.post(path, payload), True

    def wait_for_job(self, job_id: int) -> dict:
        while True:
            job = self.get(f'jobs/{job_id}/')
            status = job['status']
            if status in ('successful', 'failed', 'error', 'canceled'):
                return job
            print(f'  job {job_id}: {status}...')
            time.sleep(POLL_INTERVAL)

    def get_job_stdout(self, job_id: int) -> str:
        resp = requests.get(
            self._url(f'jobs/{job_id}/stdout/'),
            params={'format': 'txt'},
            auth=self.auth,
            verify=False,
        )
        return resp.text if resp.ok else ''


def copy_playbook_to_container(container: str, project_dir: str) -> None:
    """Copy rich_playbook.yml, collections/, and roles/ into the AWX container's project directory."""
    from pathlib import Path

    script_dir = Path(__file__).resolve().parent
    playbook_src = script_dir / PLAYBOOK_FILE

    if not playbook_src.exists():
        print(f'Error: {playbook_src} not found', file=sys.stderr)
        sys.exit(1)

    remote_dir = f'/var/lib/awx/projects/{project_dir}'

    subprocess.run(
        ['docker', 'exec', container, 'mkdir', '-p', remote_dir],
        check=True,
    )
    subprocess.run(
        ['docker', 'cp', str(playbook_src), f'{container}:{remote_dir}/{PLAYBOOK_FILE}'],
        check=True,
    )
    print(f'Copied {PLAYBOOK_FILE} → {container}:{remote_dir}/{PLAYBOOK_FILE}')

    for subdir in ('roles', 'collections'):
        local = script_dir / subdir
        if local.is_dir():
            subprocess.run(
                ['docker', 'exec', container, 'rm', '-rf', f'{remote_dir}/{subdir}'],
                check=True,
            )
            subprocess.run(
                ['docker', 'cp', str(local), f'{container}:{remote_dir}/{subdir}'],
                check=True,
            )
            print(f'Copied {subdir}/ → {container}:{remote_dir}/{subdir}/')

    subprocess.run(
        ['docker', 'exec', container, 'chown', '-R', '1000:0', remote_dir],
        check=True,
    )


def setup_resources(client: AWXClient, num_hosts: int) -> int:
    """Create project, inventory, hosts, and job template. Returns template id."""

    # Project (manual, scm_type="")
    project, created = client.find_or_create('projects/', PROJECT_NAME, {
        'name': PROJECT_NAME,
        'organization': 1,
        'scm_type': '',
        'local_path': PROJECT_DIR,
    })
    project_id = project['id']
    print(f'Project: {PROJECT_NAME} (id={project_id}) {"[created]" if created else "[exists]"}')

    # Inventory
    inventory, created = client.find_or_create('inventories/', INVENTORY_NAME, {
        'name': INVENTORY_NAME,
        'organization': 1,
    })
    inventory_id = inventory['id']
    print(f'Inventory: {INVENTORY_NAME} (id={inventory_id}) {"[created]" if created else "[exists]"}')

    # Hosts
    existing_hosts = client.get(f'inventories/{inventory_id}/hosts/', page_size=200)
    existing_count = existing_hosts['count']

    if existing_count < num_hosts:
        for i in range(existing_count + 1, num_hosts + 1):
            host_name = f'{PREFIX}_host_{i}'
            client.post(f'inventories/{inventory_id}/hosts/', {
                'name': host_name,
                'variables': 'ansible_connection: local',
            })
        print(f'Hosts: created {num_hosts - existing_count} new (total {num_hosts})')
    else:
        print(f'Hosts: {existing_count} already exist (requested {num_hosts})')

    # Job template
    template, created = client.find_or_create('job_templates/', TEMPLATE_NAME, {
        'name': TEMPLATE_NAME,
        'project': project_id,
        'inventory': inventory_id,
        'playbook': PLAYBOOK_FILE,
    })
    template_id = template['id']
    print(f'Template: {TEMPLATE_NAME} (id={template_id}) {"[created]" if created else "[exists]"}')

    return template_id


def cleanup(client: AWXClient) -> None:
    """Delete all rich_test_ resources."""
    for path, name in [
        ('job_templates/', TEMPLATE_NAME),
        ('projects/', PROJECT_NAME),
        ('inventories/', INVENTORY_NAME),
    ]:
        obj = client.find_by_name(path, name)
        if obj:
            status = client.delete(f'{path}{obj["id"]}/')
            print(f'Deleted {name} (id={obj["id"]}): {status}')
        else:
            print(f'{name}: not found')


def main() -> int:
    parser = argparse.ArgumentParser(description='Deploy and run rich_playbook.yml on AWX.')
    parser.add_argument('--url', default=DEFAULT_URL, help=f'AWX base URL (default: {DEFAULT_URL})')
    parser.add_argument('--user', default=DEFAULT_USER)
    parser.add_argument('--password', default=DEFAULT_PASSWORD)
    parser.add_argument('--container', default=DEFAULT_CONTAINER, help=f'AWX container name (default: {DEFAULT_CONTAINER})')
    parser.add_argument('--hosts', type=int, default=DEFAULT_NUM_HOSTS, help=f'Number of localhost hosts (default: {DEFAULT_NUM_HOSTS})')
    parser.add_argument('--cleanup', action='store_true', help='Delete all rich_test_ resources and exit')
    parser.add_argument('--no-wait', action='store_true', help='Launch and exit without waiting')
    args = parser.parse_args()

    client = AWXClient(args.url, args.user, args.password)

    if args.cleanup:
        cleanup(client)
        return 0

    print(f'AWX      : {args.url}')
    print(f'Container: {args.container}')
    print(f'Hosts    : {args.hosts}')
    print()

    # Step 1: copy playbook into AWX container
    copy_playbook_to_container(args.container, PROJECT_DIR)
    print()

    # Step 2: create AWX resources
    template_id = setup_resources(client, args.hosts)
    print()

    # Step 3: launch
    print('Launching job...')
    job = client.post(f'job_templates/{template_id}/launch/', {})
    job_id = job['id']
    print(f'Job {job_id} launched')

    if args.no_wait:
        print(f'Job URL: {args.url}/#/jobs/playbook/{job_id}')
        return 0

    print()
    result = client.wait_for_job(job_id)
    status = result['status']
    elapsed = result.get('elapsed', '?')
    print(f'Job {job_id} finished: {status}  (elapsed: {elapsed}s)')
    print()

    stdout = client.get_job_stdout(job_id)
    if stdout:
        print(stdout)

    return 0 if status == 'successful' else 1


if __name__ == '__main__':
    sys.exit(main())
