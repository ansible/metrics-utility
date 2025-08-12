#!/usr/bin/env python3
"""
Script to build CCSP reports and copy them to local machine.

Based on instructions in comments:
1. Access environment using SSH (like testathon_data_prepare.py)
2. Set environment variables for build_report command
3. Run the metrics-utility build_report command
4. Copy the generated report to local machine using scp
"""

import os
import subprocess
import sys

from datetime import datetime


def get_environment_config():
    """Get environment configuration from environment variables."""
    environment = os.getenv('ENVIRONMENT', 'local')

    # Set default ship path based on environment like gather_all.py does
    if environment == 'local' or environment == 'containerized':
        default_ship_path = './shipped_data'
    else:
        default_ship_path = '/var/tmp/shipped_data'

    config = {
        'ENVIRONMENT': environment,
        'SSH_URL': os.getenv('SSH_URL'),
        'SSH_USER': os.getenv('SSH_USER', 'ec2-user'),
        'SHIP_PATH': os.getenv('METRICS_UTILITY_SHIP_PATH', default_ship_path),
    }

    print('Environment Configuration:')
    print(f'ENVIRONMENT: {config["ENVIRONMENT"]}')
    print(f'SSH_URL: {config["SSH_URL"]}')
    print(f'SSH_USER: {config["SSH_USER"]}')
    print(f'SHIP_PATH: {config["SHIP_PATH"]}')
    print()

    return config


def get_report_environment_variables():
    """Get all environment variables needed for the build_report command."""
    env_vars = {
        'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_PRICE_PER_NODE': '11.55',
        'METRICS_UTILITY_REPORT_COMPANY_NAME': 'Partner A',
        'METRICS_UTILITY_REPORT_EMAIL': 'email@email.com',
        'METRICS_UTILITY_REPORT_END_USER_CITY': 'Springfield',
        'METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME': 'Customer A',
        'METRICS_UTILITY_REPORT_END_USER_COUNTRY': 'US',
        'METRICS_UTILITY_REPORT_END_USER_STATE': 'TX',
        'METRICS_UTILITY_REPORT_H1_HEADING': 'CCSP NA Direct Reporting Template',
        'METRICS_UTILITY_REPORT_PO_NUMBER': '123',
        'METRICS_UTILITY_REPORT_RHN_LOGIN': 'test_login',
        'METRICS_UTILITY_REPORT_SKU': 'MCT3752MO',
        'METRICS_UTILITY_REPORT_SKU_DESCRIPTION': 'EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)',
        # Set all optional sheets
        'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': (
            'ccsp_summary,indirectly_managed_nodes,inventory_scope,jobs,managed_nodes,'
            'managed_nodes_by_organizations,usage_by_collections,usage_by_modules,'
            'usage_by_organizations,usage_by_roles,data_collection_status'
        ),
    }

    # Allow overriding from environment
    for key, default_value in env_vars.items():
        env_vars[key] = os.getenv(key, default_value)

    return env_vars


def run_build_report_local(env_vars, ship_path, since_date='2022-01-01', until_date='2026-01-01'):
    """Run build_report command in local environment."""
    print('Running build_report in local environment...')

    # Set up environment variables for docker exec
    docker_env = []
    for k, v in env_vars.items():
        docker_env += ['-e', f'{k}={v}']

    # Add ship path
    docker_env += ['-e', f'METRICS_UTILITY_SHIP_PATH={ship_path}']

    # Build docker command
    build_cmd = (
        f'cd awx-dev/metrics-utility && . /var/lib/awx/venv/awx/bin/activate && '
        f'python3 ./manage.py build_report --force --since={since_date} --until={until_date}'
    )

    docker_cmd = ['docker', 'exec', *docker_env, 'tools_awx_1', '/bin/sh', '-c', build_cmd]

    print(f'Executing: {" ".join(docker_cmd)}')
    result = subprocess.run(docker_cmd, check=False, capture_output=True, text=True)

    return result


def run_build_report_rpm(env_vars, ship_path, ssh_url, ssh_user, since_date='2022-01-01', until_date='2026-01-01'):
    """Run build_report command in RPM environment via SSH."""
    print('Running build_report in RPM environment...')

    if not ssh_url or not ssh_user:
        raise ValueError('SSH_URL and SSH_USER must be set for RPM environment')

    # Prepare environment variables with proper quoting
    env_list = []
    for k, v in env_vars.items():
        # Quote values that contain spaces or special characters
        if ' ' in v or '(' in v or ')' in v or ',' in v:
            env_list.append(f"{k}='{v}'")
        else:
            env_list.append(f'{k}={v}')

    # Add ship path with proper quoting
    if ' ' in ship_path:
        env_list.append(f"METRICS_UTILITY_SHIP_PATH='{ship_path}'")
    else:
        env_list.append(f'METRICS_UTILITY_SHIP_PATH={ship_path}')

    # Build SSH command
    ssh_cmd = [
        'ssh',
        f'{ssh_user}@{ssh_url}',
        'sudo',
        '-E',
        'env',
        *env_list,
        'metrics-utility',
        'build_report',
        '--force',
        f'--since={since_date}',
        f'--until={until_date}',
    ]

    print(f'Executing: {" ".join(ssh_cmd)}')
    result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    return result


def run_build_report_containerized(env_vars, ship_path, ssh_url, ssh_user, since_date='2022-01-01', until_date='2026-01-01'):
    """Run build_report command in containerized environment via SSH."""
    print('Running build_report in containerized environment...')

    if not ssh_url or not ssh_user:
        raise ValueError('SSH_URL and SSH_USER must be set for containerized environment')

    # Prepare environment variables with proper quoting
    env_vars_with_path = env_vars.copy()
    env_vars_with_path['METRICS_UTILITY_SHIP_PATH'] = ship_path

    env_list = []
    for k, v in env_vars_with_path.items():
        # Quote values that contain spaces or special characters
        if ' ' in v or '(' in v or ')' in v or ',' in v:
            env_list.append(f"{k}='{v}'")
        else:
            env_list.append(f'{k}={v}')

    env_vars_str = ' '.join(env_list)
    container_cmd = f'{env_vars_str} metrics-utility build_report --force --since={since_date} --until={until_date}'

    # Use podman exec to run the command inside automation-controller-web container
    remote_command = f'echo "{container_cmd}" | podman exec -i automation-controller-web /bin/bash'
    ssh_cmd = ['ssh', f'{ssh_user}@{ssh_url}', remote_command]

    print(f'Executing: {" ".join(ssh_cmd)}')
    result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    return result


def generate_report_filename(report_type, since_date, until_date):
    """Generate the expected report filename."""
    return f'{report_type}-{since_date}--{until_date}.xlsx'


def get_report_path(ship_path, until_date, environment='RPM'):
    """Get the expected report path based on ship_path and date."""
    # Parse until_date to get year and month
    until_dt = datetime.strptime(until_date, '%Y-%m-%d')
    year = until_dt.strftime('%Y')
    month = until_dt.strftime('%m')

    # In containerized environments, the path inside container is /var/lib/awx/{ship_path}
    if environment == 'containerized':
        base_path = f'/var/lib/awx/{ship_path}'
    else:
        base_path = ship_path

    return f'{base_path}/reports/{year}/{month}'


def copy_report_from_remote(ssh_url, ssh_user, remote_report_path, local_destination='.'):
    """Copy the generated report from remote server to local machine using scp."""
    print('Copying report from remote server...')

    # Build scp command
    scp_cmd = ['scp', f'{ssh_user}@{ssh_url}:{remote_report_path}', local_destination]

    print(f'Executing: {" ".join(scp_cmd)}')
    result = subprocess.run(scp_cmd, check=False, capture_output=True, text=True)

    if result.returncode == 0:
        filename = os.path.basename(remote_report_path)
        print(f'Successfully copied report to: {os.path.join(local_destination, filename)}')
    else:
        print(f'Failed to copy report. Error: {result.stderr}')

    return result


def main():
    """Main function to orchestrate the build and copy process."""
    print('=== Build and Copy Report Script ===\n')

    # Parse command line arguments
    since_date = sys.argv[1] if len(sys.argv) > 1 else '2022-01-01'
    until_date = sys.argv[2] if len(sys.argv) > 2 else '2026-01-01'

    print(f'Date range: {since_date} to {until_date}')

    # Get configuration
    config = get_environment_config()
    env_vars = get_report_environment_variables()

    print('Environment variables for build_report:')
    for key, value in env_vars.items():
        print(f'  {key}={value}')
    print()

    # Run build_report command based on environment
    environment = config['ENVIRONMENT']
    ship_path = config['SHIP_PATH']

    try:
        if environment == 'local':
            result = run_build_report_local(env_vars, ship_path, since_date, until_date)
        elif environment == 'RPM':
            result = run_build_report_rpm(env_vars, ship_path, config['SSH_URL'], config['SSH_USER'], since_date, until_date)
        elif environment == 'containerized':
            result = run_build_report_containerized(env_vars, ship_path, config['SSH_URL'], config['SSH_USER'], since_date, until_date)
        else:
            raise ValueError(f'Unsupported environment: {environment}')

        # Print command output
        print('=== Command Output ===')
        print('STDOUT:')
        print(result.stdout)
        if result.stderr:
            print('STDERR:')
            print(result.stderr)

        print(f'Return code: {result.returncode}')

        if result.returncode != 0:
            print('ERROR: build_report command failed!')
            return 1

        # Generate expected report path
        report_filename = generate_report_filename(env_vars['METRICS_UTILITY_REPORT_TYPE'], since_date, until_date)
        report_dir = get_report_path(ship_path, until_date, environment)
        remote_report_path = f'{report_dir}/{report_filename}'

        print(f'\nExpected report location: {remote_report_path}')

        # Copy report to local machine (only for remote environments)
        if environment in ['RPM', 'containerized']:
            copy_result = copy_report_from_remote(config['SSH_URL'], config['SSH_USER'], remote_report_path, '.')
            if copy_result.returncode != 0:
                print('WARNING: Failed to copy report file')
                return 1
        elif environment == 'local':
            print(f'Local environment: Report should be available at: {remote_report_path}')

        print('\n=== Script completed successfully! ===')
        return 0

    except Exception as e:
        print(f'ERROR: {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
