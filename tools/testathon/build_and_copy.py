#!/usr/bin/env python3
"""
Script to build CCSP reports and copy them to local machine.

Based on instructions in comments:
1. Access environment using SSH (like testathon_data_prepare.py)
2. Set environment variables for build_report command
3. Run the metrics-utility build_report command
4. Copy the generated report to local machine using scp

Required/optional environment variables:
- ENVIRONMENT: "RPM", "containerized" or "OpenShift"
- SSH_URL: IP or hostname of the controller instance (required for RPM/containerized)
- SSH_USER: SSH user (default: ec2-user; use "ansible" for containerized)
- METRICS_UTILITY_SHIP_PATH: ship path on remote (defaults to '/var/tmp/shipped_data' for RPM/OpenShift; './shipped_data' for containerized)

OpenShift specific variables:
- OC_LOGIN_COMMAND: oc login command to authenticate to the cluster
  Example: oc login --token=... --server=https://api.example:6443
- NAMESPACE (optional): OpenShift namespace with the controller pod. If not set, it will be auto-detected.
- POD_NAME (optional): OpenShift controller pod name. If not set, it will be auto-detected.

Quick examples:
- RPM environment (with explicit PO number unset):
    ENVIRONMENT=RPM \
    SSH_URL=1.2.3.4 \
    SSH_USER=ec2-user \
    METRICS_UTILITY_REPORT_PO_NUMBER=None \
    tools/testathon/build_and_copy.py --force --since=2022-01-01 --until=2026-01-01

- Containerized environment:
    ENVIRONMENT=containerized \
    SSH_URL=1.2.3.4 \
    SSH_USER=ansible \
    tools/testathon/build_and_copy.py --force --month=2025-07

- OpenShift environment (namespace/pod auto-detected):
    ENVIRONMENT=OpenShift \
    OC_LOGIN_COMMAND='oc login --token=... --server=https://api.example:6443' \
    tools/testathon/build_and_copy.py --force --month=2025-07

Report parameters via METRICS_UTILITY_* variables (optional):
- This script prepares a set of METRICS_UTILITY_* variables (see get_report_environment_variables()).
  If you do not export a variable, a sensible default is used and forwarded to the command.
- To override a default, export the variable with your value, e.g.:
    export METRICS_UTILITY_REPORT_COMPANY_NAME="Acme Corp"
- To UNSET/omit a variable (so it is not sent at all and the downstream tool decides its own default),
  export it with the literal string 'None' (case-insensitive), e.g.:
    export METRICS_UTILITY_REPORT_PO_NUMBER=None
  In this mode, the script still uses its internal default for filename/path computations but will NOT send
  the variable to the remote command.
- If a value contains spaces, commas, or parentheses, quoting is handled automatically when invoking the command.


"""

import os
import subprocess
import sys

from datetime import datetime, timezone

from helper_ocp_prepare import create_oc_environs


def get_environment_config():
    """Get environment configuration from environment variables."""
    environment = os.getenv('ENVIRONMENT', 'RPM')

    # Set default ship path based on environment like gather_all.py does
    if environment == 'containerized':
        default_ship_path = './shipped_data'
    else:
        default_ship_path = '/var/tmp/shipped_data'

    config = {
        'ENVIRONMENT': environment,
        'SSH_URL': os.getenv('SSH_URL'),
        'SSH_USER': os.getenv('SSH_USER', 'ec2-user'),
        'SHIP_PATH': os.getenv('METRICS_UTILITY_SHIP_PATH', default_ship_path),
        'OC_LOGIN_COMMAND': os.getenv('OC_LOGIN_COMMAND', ''),
        'NAMESPACE': os.getenv('NAMESPACE'),
        'POD_NAME': os.getenv('POD_NAME'),
    }

    print('Environment Configuration:')
    print(f'ENVIRONMENT: {config["ENVIRONMENT"]}')
    print(f'SSH_URL: {config["SSH_URL"]}')
    print(f'SSH_USER: {config["SSH_USER"]}')
    print(f'SHIP_PATH: {config["SHIP_PATH"]}')
    if config['ENVIRONMENT'] == 'OpenShift':
        print(f'OC_LOGIN_COMMAND: {config["OC_LOGIN_COMMAND"]}')
        if config['POD_NAME'] and config['NAMESPACE']:
            print(f'POD_NAME: {config["POD_NAME"]}')
            print(f'NAMESPACE: {config["NAMESPACE"]}')
    print()

    return config


def get_report_environment_variables():
    """Build env vars for metrics-utility using defaults and current process env.

    Returns a tuple of (effective_env_vars, overridden_env_vars, env_vars_to_send):
    - effective_env_vars: values used by this script for internal logic (e.g., filename generation).
      If an environment variable is set to the string 'None' (case-insensitive), the default is used here.
    - overridden_env_vars: maps var name -> {'from': default_value, 'to': provided_value or 'UNSET'}
      for any variable provided via environment and changed the default. If provided value is 'None', 'to' is 'UNSET'.
    - env_vars_to_send: the variables actually sent to the metrics-utility command. Variables explicitly set to the string
      'None' (case-insensitive) are omitted entirely and not sent.
    """
    defaults = {
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
            'usage_by_organizations,usage_by_roles,data_collection_status,infrastructure_summary'
        ),
        'METRICS_UTILITY_DEDUPLICATOR': 'ccsp-experimental',
    }

    effective = {}
    overridden = {}
    env_vars_to_send = {}

    for key, default_value in defaults.items():
        provided = os.getenv(key)
        # Normalize textual None sentinel
        is_reset = isinstance(provided, str) and provided.strip().lower() == 'none'
        if provided is None or is_reset:
            # For our internal logic, use default when not provided or reset
            effective[key] = default_value
            if provided is None:
                # Not provided: send default downstream
                env_vars_to_send[key] = default_value
            elif is_reset:
                # Explicit reset: do not send this var at all
                overridden[key] = {'from': default_value, 'to': 'UNSET'}
        else:
            # Provided and not reset sentinel: use provided
            effective[key] = provided
            env_vars_to_send[key] = provided
            if provided != default_value:
                overridden[key] = {'from': default_value, 'to': provided}

    return effective, overridden, env_vars_to_send


def run_build_report_rpm(env_vars, ship_path, ssh_url, ssh_user, user_args):
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

    # Build SSH command - split user_args into individual arguments
    user_args_list = user_args.split()
    ssh_cmd = [
        'ssh',
        f'{ssh_user}@{ssh_url}',
        'sudo',
        '-E',
        'env',
        *env_list,
        'metrics-utility',
        'build_report',
        *user_args_list,
    ]

    print(f'Executing: {" ".join(ssh_cmd)}')
    result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    return result


def run_build_report_containerized(env_vars, ship_path, ssh_url, ssh_user, user_args):
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
    container_cmd = f'{env_vars_str} metrics-utility build_report {user_args}'

    # Use podman exec to run the command inside automation-controller-web container
    remote_command = f'echo "{container_cmd}" | podman exec -i automation-controller-web /bin/bash'
    ssh_cmd = ['ssh', f'{ssh_user}@{ssh_url}', remote_command]

    print(f'Executing: {" ".join(ssh_cmd)}')
    result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    return result


def run_build_report_openshift(env_vars, ship_path, user_args, namespace=None, pod_name=None, oc_login_command=''):
    """Run build_report command in OpenShift environment using oc exec."""
    print('Running build_report in OpenShift environment...')

    # login if command provided
    if oc_login_command:
        print('Logging into OpenShift cluster...')
        subprocess.run(oc_login_command, shell=True, check=False)

    # Ensure NAMESPACE and POD_NAME are available
    if not namespace or not pod_name:
        print('Detecting OpenShift namespace and pod...')
        create_oc_environs()
        namespace = os.getenv('NAMESPACE')
        pod_name = os.getenv('POD_NAME')

    if not namespace or not pod_name:
        raise ValueError('Failed to determine OpenShift NAMESPACE and POD_NAME')

    # Prepare environment variables with proper quoting
    env_vars_with_path = env_vars.copy()
    env_vars_with_path['METRICS_UTILITY_SHIP_PATH'] = ship_path

    env_list = []
    for k, v in env_vars_with_path.items():
        if ' ' in v or '(' in v or ')' in v or ',' in v:
            env_list.append(f"{k}='{v}'")
        else:
            env_list.append(f'{k}={v}')

    env_vars_str = ' '.join(env_list)
    container_cmd = f'{env_vars_str} metrics-utility build_report {user_args}'

    oc_cmd = f'oc exec -n {namespace} {pod_name} -- /bin/bash -c "{container_cmd}"'
    print(f'Executing: {oc_cmd}')
    result = subprocess.run(oc_cmd, shell=True, check=False, capture_output=True, text=True)

    return result


def generate_report_filename(report_type, since_date, until_date):
    """Generate the expected report filename for since/until range."""
    return f'{report_type}-{since_date}--{until_date}.xlsx'


def generate_month_report_filename(report_type, month_str):
    """Generate the expected report filename for monthly reports (YYYY-MM)."""
    return f'{report_type}-{month_str}.xlsx'


def get_report_path(ship_path, date_str, environment='RPM'):
    """Get the expected report path based on ship_path and date.

    date_str can be either 'YYYY-MM-DD' or 'YYYY-MM'.
    """
    # Parse input into year and month
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        dt = datetime.strptime(date_str, '%Y-%m')
    year = dt.strftime('%Y')
    month = dt.strftime('%m')

    # In containerized environments, the path inside container is /var/lib/awx/{ship_path}
    if environment == 'containerized':
        # Clean up the ship_path by removing './' prefix if present
        clean_ship_path = ship_path.lstrip('./')
        base_path = f'/var/lib/awx/{clean_ship_path}'
    else:
        base_path = ship_path

    return f'{base_path}/reports/{year}/{month}'


def copy_report_from_remote(ssh_url, ssh_user, remote_report_path, local_destination='.', environment='RPM'):
    """Copy the generated report from remote server to local machine using scp or oc exec."""
    print('Copying report from remote server...')

    if environment == 'containerized':
        # For containerized environment, we need to copy from container to host first
        print('Copying from container to host first...')

        # Extract filename from remote_report_path
        filename = os.path.basename(remote_report_path)

        # Copy from container to host /tmp directory
        host_temp_path = f'/tmp/{filename}'
        podman_copy_cmd = f'podman cp automation-controller-web:{remote_report_path} {host_temp_path}'

        ssh_copy_cmd = ['ssh', f'{ssh_user}@{ssh_url}', podman_copy_cmd]

        print(f'Executing: {" ".join(ssh_copy_cmd)}')
        copy_result = subprocess.run(ssh_copy_cmd, check=False, capture_output=True, text=True)

        if copy_result.returncode != 0:
            print(f'Failed to copy from container to host. Error: {copy_result.stderr}')
            return copy_result

        print('Successfully copied from container to host')

        # Now copy from host to local machine
        remote_path_for_scp = host_temp_path
    elif environment == 'OpenShift':
        # Copy directly from OpenShift pod to local using oc exec (no oc cp)
        oc_login_command = os.getenv('OC_LOGIN_COMMAND', '')
        if oc_login_command:
            print('Logging into OpenShift cluster before copy...')
            subprocess.run(oc_login_command, shell=True, check=False)

        namespace = os.getenv('NAMESPACE')
        pod_name = os.getenv('POD_NAME')
        if not namespace or not pod_name:
            print('NAMESPACE/POD_NAME not set. Attempting to detect...')
            create_oc_environs()
            namespace = os.getenv('NAMESPACE')
            pod_name = os.getenv('POD_NAME')
        if not namespace or not pod_name:
            raise ValueError('Failed to determine OpenShift NAMESPACE and POD_NAME for copying report')

        print('Copying from OpenShift pod to local using oc exec...')
        filename = os.path.basename(remote_report_path)
        local_file_path = (
            os.path.join(local_destination, filename) if os.path.isdir(local_destination) or local_destination in ('.', '') else local_destination
        )
        oc_exec_cmd = ['oc', 'exec', '-n', namespace, pod_name, '--', '/bin/bash', '-c', f"cat '{remote_report_path}'"]
        print(f'Executing: {" ".join(oc_exec_cmd)} > {local_file_path}')
        stream_result = subprocess.run(oc_exec_cmd, check=False, capture_output=True, text=False)
        if stream_result.returncode == 0:
            try:
                with open(local_file_path, 'wb') as f:
                    f.write(stream_result.stdout)
                print(f'Successfully copied report to: {local_file_path}')
            except Exception as write_err:
                print(f'Failed to write streamed file locally: {write_err}')
                return stream_result
        else:
            stderr = stream_result.stderr.decode('utf-8', errors='ignore') if stream_result.stderr else ''
            print(f'Copy failed using oc exec. Error: {stderr}')
        return stream_result
    else:
        # For non-containerized environments, use the original path
        remote_path_for_scp = remote_report_path

    # Build scp command
    scp_cmd = ['scp', f'{ssh_user}@{ssh_url}:{remote_path_for_scp}', local_destination]

    print(f'Executing: {" ".join(scp_cmd)}')
    result = subprocess.run(scp_cmd, check=False, capture_output=True, text=True)

    if result.returncode == 0:
        filename = os.path.basename(remote_report_path)
        print(f'Successfully copied report to: {os.path.join(local_destination, filename)}')

        # Clean up temporary file on host for containerized environment
        if environment == 'containerized':
            cleanup_cmd = ['ssh', f'{ssh_user}@{ssh_url}', f'rm -f {host_temp_path}']
            print(f'Cleaning up temporary file: {host_temp_path}')
            subprocess.run(cleanup_cmd, check=False, capture_output=True, text=True)
    else:
        print(f'Failed to copy report. Error: {result.stderr}')

    return result


def main():
    """Main function to orchestrate the build and copy process."""
    print('=== Build and Copy Report Script ===\n')

    # Define default arguments string
    default_args = '--force --since=2022-01-01 --until=2026-01-01'

    # Handle command line arguments
    if len(sys.argv) == 1:
        # No arguments provided - use defaults
        user_args = default_args
        print('No arguments provided, using default arguments')
        print(f'Default arguments: {default_args}')
    else:
        # Arguments provided - use them as-is
        user_args = ' '.join(sys.argv[1:])
        print('Using provided arguments')
        print(f'User arguments: {user_args}')

    print()

    # Get configuration
    config = get_environment_config()
    effective_env_vars, overridden_env_vars, env_vars_to_send = get_report_environment_variables()

    print('Environment variables for build_report (sent to metrics-utility):')
    for key, value in env_vars_to_send.items():
        print(f'  {key}={value}')
    print()

    if overridden_env_vars:
        print('Overrides from environment that changed defaults:')
        for key, change in overridden_env_vars.items():
            print(f'  {key}={change["to"]} (default was: {change["from"]})')
        print()

    # Run build_report command based on environment
    environment = config['ENVIRONMENT']
    ship_path = config['SHIP_PATH']

    try:
        if environment == 'RPM':
            result = run_build_report_rpm(env_vars_to_send, ship_path, config['SSH_URL'], config['SSH_USER'], user_args)
        elif environment == 'containerized':
            result = run_build_report_containerized(env_vars_to_send, ship_path, config['SSH_URL'], config['SSH_USER'], user_args)
        elif environment == 'OpenShift':
            result = run_build_report_openshift(
                env_vars_to_send,
                ship_path,
                user_args,
                namespace=config.get('NAMESPACE'),
                pod_name=config.get('POD_NAME'),
                oc_login_command=config.get('OC_LOGIN_COMMAND', ''),
            )
        else:
            raise ValueError(f'Unsupported environment: {environment}. Only "RPM", "containerized" and "OpenShift" are supported.')

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

        # Extract dates from user_args for report path generation
        import re

        since_match = re.search(r'--since=([^\s]+)', user_args)
        until_match = re.search(r'--until=([^\s]+)', user_args)
        month_match = re.search(r'--month=([0-9]{4}-[0-9]{2})', user_args)

        remote_report_path = None

        if since_match:
            since_date = since_match.group(1)
            if until_match:
                until_date = until_match.group(1)
            else:
                # Default until to today's date in UTC when not provided
                until_date = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')

            # Generate expected report path for since/until
            report_filename = generate_report_filename(effective_env_vars['METRICS_UTILITY_REPORT_TYPE'], since_date, until_date)
            report_dir = get_report_path(ship_path, until_date, environment)
            remote_report_path = f'{report_dir}/{report_filename}'
            print(f'\nExpected report location (since/until): {remote_report_path}')
        elif month_match:
            month_str = month_match.group(1)
            # Generate expected report path for monthly report
            report_filename = generate_month_report_filename(effective_env_vars['METRICS_UTILITY_REPORT_TYPE'], month_str)
            report_dir = get_report_path(ship_path, month_str, environment)
            remote_report_path = f'{report_dir}/{report_filename}'
            print(f'\nExpected report location (monthly): {remote_report_path}')
        else:
            print(f'\nWarning: Could not extract date range or month from arguments: {user_args}')
            print('Report location cannot be determined automatically.')

        # Copy report to local machine
        if remote_report_path:
            copy_result = copy_report_from_remote(config.get('SSH_URL'), config.get('SSH_USER'), remote_report_path, '.', environment)
            if copy_result.returncode != 0:
                print('WARNING: Failed to copy report file')
                return 1
        else:
            print('Skipping report copy due to inability to determine report location.')
            print('Please check the build_report output for the actual report location.')

        print('\n=== Script completed successfully! ===')
        return 0

    except Exception as e:
        print(f'ERROR: {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
