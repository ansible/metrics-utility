#!/usr/bin/env python
"""
Launch the "Demo Job Template" on a running AWX instance and wait for it to finish.

Defaults match the AWX docker-compose dev setup (awx/tools/docker-compose):
  https://localhost:8043  admin/admin

Password resolution order:
  1. --password CLI flag
  2. AWX_PASSWORD environment variable
  3. Default "admin" (local dev only)

Usage:
  python run_demo_template.py
  python run_demo_template.py --url https://awx.example.com --user admin
  python run_demo_template.py --template "My Custom Template"
  python run_demo_template.py --insecure   # skip TLS verification (default for localhost)
"""

import argparse
import os
import sys
import time
import urllib.parse

import requests


DEFAULT_URL = 'https://localhost:8043'
DEFAULT_USER = 'admin'
DEFAULT_PASSWORD = 'admin'
DEFAULT_TEMPLATE = 'Demo Job Template'
POLL_INTERVAL = 3
REQUEST_TIMEOUT = 30
MAX_POLL_TIME = 600


def validate_base_url(raw_url: str) -> str:
    """Validate and normalize a CLI-supplied AWX base URL.

    Only absolute ``http``/``https`` URLs with a hostname are accepted. Embedded
    credentials and whitespace/control characters are rejected, and the returned
    value is rebuilt from the parsed components so downstream ``requests`` calls
    never receive the raw argv string.
    """
    if not raw_url or any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw_url):
        raise ValueError(f'Invalid AWX URL (empty, or contains whitespace/control characters): {raw_url!r}')

    parts = urllib.parse.urlsplit(raw_url)
    scheme = parts.scheme.lower()
    if scheme not in ('http', 'https'):
        raise ValueError(f'Invalid AWX URL scheme {parts.scheme!r}: only http and https are supported')

    hostname = parts.hostname
    if not hostname:
        raise ValueError(f'Invalid AWX URL (missing hostname): {raw_url!r}')
    if parts.username is not None or parts.password is not None:
        raise ValueError('Invalid AWX URL: embedded credentials are not supported, use --user/--password')

    try:
        port = parts.port
    except ValueError:
        raise ValueError(f'Invalid AWX URL (invalid port): {raw_url!r}')

    netloc = f'[{hostname}]' if ':' in hostname else hostname
    if port is not None:
        netloc = f'{netloc}:{port}'

    return f'{scheme}://{netloc}{parts.path}'.rstrip('/')


def find_template(base_url: str, auth: tuple, name: str, *, verify: bool) -> dict | None:
    resp = requests.get(
        f'{base_url}/api/v2/job_templates/',
        params={'search': name, 'page_size': 50},
        auth=auth,
        verify=verify,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    for t in resp.json()['results']:
        if t['name'] == name:
            return t
    return None


def launch(base_url: str, auth: tuple, template_id: int, *, verify: bool) -> dict:
    resp = requests.post(
        f'{base_url}/api/v2/job_templates/{template_id}/launch/',
        auth=auth,
        verify=verify,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def wait_for_job(base_url: str, auth: tuple, job_id: int, *, verify: bool) -> dict:
    url = f'{base_url}/api/v2/jobs/{job_id}/'
    deadline = time.monotonic() + MAX_POLL_TIME
    while True:
        resp = requests.get(url, auth=auth, verify=verify, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        job = resp.json()
        status = job['status']
        if status in ('successful', 'failed', 'error', 'canceled'):
            return job
        if time.monotonic() > deadline:
            raise TimeoutError(f'Job {job_id} did not finish within {MAX_POLL_TIME}s (last status: {status})')
        print(f'  job {job_id}: {status}...')
        time.sleep(POLL_INTERVAL)


def print_job_stdout(base_url: str, auth: tuple, job_id: int, *, verify: bool) -> None:
    resp = requests.get(
        f'{base_url}/api/v2/jobs/{job_id}/stdout/',
        params={'format': 'txt'},
        auth=auth,
        verify=verify,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    print(resp.text)


def _resolve_verify(args) -> bool:
    if args.insecure:
        return False
    if args.url == DEFAULT_URL:
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description='Launch a job template on a running AWX instance.')
    parser.add_argument('--url', default=DEFAULT_URL, help=f'AWX base URL (default: {DEFAULT_URL})')
    parser.add_argument('--user', default=DEFAULT_USER, help=f'AWX username (default: {DEFAULT_USER})')
    parser.add_argument(
        '--password',
        default=None,
        help='AWX password (default: AWX_PASSWORD env var, or "admin")',
    )
    parser.add_argument('--template', default=DEFAULT_TEMPLATE, help=f'Job template name (default: {DEFAULT_TEMPLATE!r})')
    parser.add_argument('--insecure', action='store_true', help='Skip TLS certificate verification')
    args = parser.parse_args()

    password = args.password or os.environ.get('AWX_PASSWORD', DEFAULT_PASSWORD)
    verify = _resolve_verify(args)
    auth = (args.user, password)

    try:
        base_url = validate_base_url(args.url)
    except ValueError as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 2

    if not verify:
        requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    print(f'AWX : {args.url}')
    print(f'User: {args.user}')
    print(f'Template: {args.template!r}')
    print(f'TLS verify: {verify}')
    print()

    template = find_template(base_url, auth, args.template, verify=verify)
    if template is None:
        print(f'Error: job template {args.template!r} not found', file=sys.stderr)
        return 1

    print(f'Found template id={template["id"]}  playbook={template.get("playbook")!r}')
    print('Launching...')

    job = launch(base_url, auth, template['id'], verify=verify)
    job_id = job['id']
    print(f'Job {job_id} launched')
    print()

    result = wait_for_job(base_url, auth, job_id, verify=verify)
    status = result['status']
    elapsed = result.get('elapsed', '?')
    print(f'Job {job_id} finished: {status}  (elapsed: {elapsed}s)')
    print()

    print_job_stdout(base_url, auth, job_id, verify=verify)

    return 0 if status == 'successful' else 1


if __name__ == '__main__':
    sys.exit(main())
