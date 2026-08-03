#!/usr/bin/env python
"""
Launch the "Demo Job Template" on a running AWX instance and wait for it to finish.

Defaults match the AWX docker-compose dev setup (awx/tools/docker-compose):
  https://localhost:8043  admin/admin

Usage:
  python run_demo_template.py
  python run_demo_template.py --url https://awx.example.com --user admin --password secret
  python run_demo_template.py --template "My Custom Template"
"""

import argparse
import sys
import time

import requests


requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

DEFAULT_URL = 'https://localhost:8043'
DEFAULT_USER = 'admin'
DEFAULT_PASSWORD = 'admin'
DEFAULT_TEMPLATE = 'Demo Job Template'
POLL_INTERVAL = 3


def find_template(base_url: str, auth: tuple, name: str) -> dict | None:
    resp = requests.get(
        f'{base_url}/api/v2/job_templates/',
        params={'search': name, 'page_size': 50},
        auth=auth,
        verify=False,  # noqa: S501
    )
    resp.raise_for_status()
    for t in resp.json()['results']:
        if t['name'] == name:
            return t
    return None


def launch(base_url: str, auth: tuple, template_id: int) -> dict:
    resp = requests.post(
        f'{base_url}/api/v2/job_templates/{template_id}/launch/',
        auth=auth,
        verify=False,  # noqa: S501
    )
    resp.raise_for_status()
    return resp.json()


def wait_for_job(base_url: str, auth: tuple, job_id: int) -> dict:
    url = f'{base_url}/api/v2/jobs/{job_id}/'
    while True:
        resp = requests.get(url, auth=auth, verify=False)  # noqa: S501
        resp.raise_for_status()
        job = resp.json()
        status = job['status']
        if status in ('successful', 'failed', 'error', 'canceled'):
            return job
        print(f'  job {job_id}: {status}...')
        time.sleep(POLL_INTERVAL)


def print_job_stdout(base_url: str, auth: tuple, job_id: int) -> None:
    resp = requests.get(
        f'{base_url}/api/v2/jobs/{job_id}/stdout/',
        params={'format': 'txt'},
        auth=auth,
        verify=False,  # noqa: S501
    )
    if resp.ok:
        print(resp.text)


def main() -> int:
    parser = argparse.ArgumentParser(description='Launch a job template on a running AWX instance.')
    parser.add_argument('--url', default=DEFAULT_URL, help=f'AWX base URL (default: {DEFAULT_URL})')
    parser.add_argument('--user', default=DEFAULT_USER, help=f'AWX username (default: {DEFAULT_USER})')
    parser.add_argument('--password', default=DEFAULT_PASSWORD, help=f'AWX password (default: {DEFAULT_PASSWORD})')
    parser.add_argument('--template', default=DEFAULT_TEMPLATE, help=f'Job template name (default: {DEFAULT_TEMPLATE!r})')
    args = parser.parse_args()

    auth = (args.user, args.password)

    print(f'AWX : {args.url}')
    print(f'User: {args.user}')
    print(f'Template: {args.template!r}')
    print()

    template = find_template(args.url, auth, args.template)
    if template is None:
        print(f'Error: job template {args.template!r} not found', file=sys.stderr)
        return 1

    print(f'Found template id={template["id"]}  playbook={template.get("playbook")!r}')
    print('Launching...')

    job = launch(args.url, auth, template['id'])
    job_id = job['id']
    print(f'Job {job_id} launched')
    print()

    result = wait_for_job(args.url, auth, job_id)
    status = result['status']
    elapsed = result.get('elapsed', '?')
    print(f'Job {job_id} finished: {status}  (elapsed: {elapsed}s)')
    print()

    print_job_stdout(args.url, auth, job_id)

    return 0 if status == 'successful' else 1


if __name__ == '__main__':
    sys.exit(main())
