#!/usr/bin/env python3
import os
import subprocess
import sys

from datetime import date, timedelta

from helper_ocp_prepare import create_oc_environs


ENVIRONMENT = os.getenv('ENVIRONMENT', 'local')

SSH_URL = os.getenv('SSH_URL')
SSH_USER = os.getenv('SSH_USER', 'ec2-user')


OC_LOGIN_COMMAND = os.getenv('OC_LOGIN_COMMAND', '')

print(f'ENVIRONMENT: {ENVIRONMENT}')

print(f'SSH_URL: {SSH_URL}')
print(f'SSH_USER: {SSH_USER}')

print(f'OC_LOGIN_COMMAND: {OC_LOGIN_COMMAND}')

if os.getenv('POD_NAME') and os.getenv('NAMESPACE'):
    print(f'POD_NAME: {os.getenv("POD_NAME")}')
    print(f'NAMESPACE: {os.getenv("NAMESPACE")}')

# Configure the beginning of your range here
START_DATE = date(2022, 1, 1)
# Uses today() as the end of the range; modify if you need a fixed end
END_DATE = date.today() + timedelta(days=10)  # just for sure we are collecting really all the data

path_to_shipped_data = '/var/tmp/shipped_data'

if ENVIRONMENT == 'local' or ENVIRONMENT == 'containerized':
    path_to_shipped_data = './shipped_data'


def enumerate_ranges(start_date, end_date):
    current = start_date

    while current <= end_date:
        end = current + timedelta(days=28)
        if end > end_date + timedelta(days=1):
            end = end_date + timedelta(days=1)

        yield (current, end)

        current = end


def get_metrics_utility_config():
    env = {
        'METRICS_UTILITY_SHIP_PATH': path_to_shipped_data,
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
        'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,main_jobevent,main_indirectmanagednodeaudit',
    }

    return env


def run_command(args, config):
    """
    Execute the metrics-utility command based on the environment type.
    """
    if ENVIRONMENT == 'local':
        # Local docker exec path
        docker_env = []
        for k, v in config.items():
            docker_env += ['-e', f'{k}={v}']
        docker_cmd = [
            'docker',
            'exec',
            *docker_env,
            'tools_awx_1',
            '/bin/sh',
            '-c',
            f'cd awx-dev/metrics-utility && . /var/lib/awx/venv/awx/bin/activate && python3 ./manage.py {" ".join(args)}',
        ]
        print('Running local:', ' '.join(docker_cmd))
        return subprocess.run(docker_cmd, check=False, capture_output=True, text=True)

    elif ENVIRONMENT == 'RPM':
        # RPM deployment via SSH
        if not SSH_URL or not SSH_USER:
            raise ValueError('SSH_URL and SSH_USER must be set for RPM environment')

        env_list = [f'{k}={v}' for k, v in config.items()]
        ssh_cmd = ['ssh', f'{SSH_USER}@{SSH_URL}', 'sudo', '-E', 'env', *env_list, 'metrics-utility', *args]
        print('Running RPM:', ' '.join(ssh_cmd))
        return subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    elif ENVIRONMENT == 'containerized':
        # Containerized deployment via SSH with podman
        if not SSH_URL or not SSH_USER:
            raise ValueError('SSH_URL and SSH_USER must be set for containerized environment')

        # Build the command to run inside the container
        env_vars = ' '.join([f'{k}={v}' for k, v in config.items()])
        container_cmd = f'{env_vars} metrics-utility {" ".join(args)}'

        # Use podman exec to run the command inside automation-controller-web container
        remote_command = f'echo "{container_cmd}" | podman exec -i automation-controller-web /bin/bash'
        ssh_cmd = ['ssh', f'{SSH_USER}@{SSH_URL}', remote_command]
        print('Running containerized:', ' '.join(ssh_cmd))
        return subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    elif ENVIRONMENT == 'OpenShift':
        # OpenShift deployment via oc command
        env_vars = ' '.join([f'{k}={v}' for k, v in config.items()])
        container_cmd = f'{env_vars} metrics-utility {" ".join(args)}'

        NAMESPACE = os.getenv('NAMESPACE')
        POD_NAME = os.getenv('POD_NAME')

        # Use oc exec to run the command
        oc_cmd = f'oc exec -n {NAMESPACE} {POD_NAME} -- /bin/bash -c "{container_cmd}"'  # noqa: E501
        print('Running OpenShift:', oc_cmd)
        return subprocess.run(oc_cmd, shell=True, check=False, capture_output=True, text=True)

    else:
        raise ValueError(f'Unsupported environment: {ENVIRONMENT}')


def oc_login():
    # subprocess run
    subprocess.run(OC_LOGIN_COMMAND, shell=True)


def list_gathered_files(config):
    """
    List the files that were gathered in the ship directory.
    """
    ship_path = config.get('METRICS_UTILITY_SHIP_PATH', path_to_shipped_data)
    print(f'\n=== Listing gathered files in {ship_path} ===')

    NAMESPACE = os.getenv('NAMESPACE')
    POD_NAME = os.getenv('POD_NAME')

    if ENVIRONMENT == 'local':
        # For local environment, list files in the docker container
        docker_cmd = [
            'docker',
            'exec',
            'tools_awx_1',
            '/bin/sh',
            '-c',
            f'cd awx-dev/metrics-utility && find {ship_path} -type f -ls 2>/dev/null || echo "No files found or directory does not exist"',
        ]
        print('Running local ls:', ' '.join(docker_cmd))
        result = subprocess.run(docker_cmd, check=False, capture_output=True, text=True)

    elif ENVIRONMENT == 'RPM':
        # RPM deployment via SSH
        if not SSH_URL or not SSH_USER:
            print('SSH_URL and SSH_USER must be set for RPM environment')
            return

        ssh_cmd = ['ssh', f'{SSH_USER}@{SSH_URL}', f'find {ship_path} -type f -ls 2>/dev/null || echo "No files found or directory does not exist"']
        print('Running RPM ls:', ' '.join(ssh_cmd))
        result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    elif ENVIRONMENT == 'containerized':
        # Containerized deployment via SSH with podman
        if not SSH_URL or not SSH_USER:
            print('SSH_URL and SSH_USER must be set for containerized environment')
            return

        container_cmd = f'find /var/lib/awx/{ship_path} -type f -ls 2>/dev/null || echo "No files found or directory does not exist"'
        remote_command = f'echo "{container_cmd}" | podman exec -i automation-controller-web /bin/bash'
        ssh_cmd = ['ssh', f'{SSH_USER}@{SSH_URL}', remote_command]
        print('Running containerized ls:', ' '.join(ssh_cmd))
        result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)

    elif ENVIRONMENT == 'OpenShift':
        # OpenShift deployment via oc command
        cmd = f'find {ship_path} -type f -ls 2>/dev/null || echo "No files found or directory does not exist"'
        oc_cmd = f'oc exec -n {NAMESPACE} {POD_NAME} -- /bin/bash -c "{cmd}"'
        print('Running OpenShift ls:', oc_cmd)
        result = subprocess.run(oc_cmd, shell=True, check=False, capture_output=True, text=True)
    else:
        print(f'Unsupported environment for listing files: {ENVIRONMENT}')
        return

    # Print the results
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)


def main():
    if ENVIRONMENT == 'OpenShift':
        oc_login()

        if not os.getenv('POD_NAME') or not os.getenv('NAMESPACE'):
            create_oc_environs()

        print(f'POD_NAME: {os.getenv("POD_NAME")}')
        print(f'NAMESPACE: {os.getenv("NAMESPACE")}')

    config = get_metrics_utility_config()

    for since, until in enumerate_ranges(START_DATE, END_DATE):
        # Base command arguments
        args = [
            'gather_automation_controller_billing_data',
            '--ship',
            f'--since={since.isoformat()}',
            f'--until={until.isoformat()}',
            '--force',
        ]

        try:
            result = run_command(args, config)
        except ValueError as e:
            print(f'Configuration error: {e}', file=sys.stderr)
            sys.exit(1)

        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        # Continue through all date ranges (no premature exit)

    # List all gathered files after completion
    list_gathered_files(config)

    # print commands how to connect manually to the terminal
    if ENVIRONMENT == 'OpenShift':
        NAMESPACE = os.getenv('NAMESPACE')
        POD_NAME = os.getenv('POD_NAME')
        print('To connect manually to the terminal, run:')
        oc_rsh_command = f'oc rsh -n {NAMESPACE} {POD_NAME}'
        print(oc_rsh_command)

    if ENVIRONMENT == 'containerized':
        # ssh and connect to the container
        ssh_command = f'ssh {SSH_USER}@{SSH_URL}'
        print('To connect manually to the terminal, run:')
        print(ssh_command)
        print('podman exec -it automation-controller-web /bin/bash')

    if ENVIRONMENT == 'local':
        print('To connect manually to the terminal, run:')
        print('docker exec -it tools_awx_1 /bin/bash')

    if ENVIRONMENT == 'RPM':
        print('To connect manually to the terminal, run:')
        print(f'ssh {SSH_USER}@{SSH_URL}')


if __name__ == '__main__':
    main()
