#!/usr/bin/env python3
import calendar
import os
import subprocess
import sys

from datetime import date, timedelta


SSH_URL = os.getenv('SSH_URL')
SSH_USER = os.getenv('SSH_USER')

print(f'SSH_URL: {SSH_URL}')
print(f'SSH_USER: {SSH_USER}')

# Configure the beginning of your range here
START_DATE = date(2022, 1, 1)
# Uses today() as the end of the range; modify if you need a fixed end
END_DATE = date.today()


def month_ranges(start_date, end_date):
    """
    Yield tuples (since_date, until_date) covering:
      - First 4 weeks (28 days) of each month
      - Remainder of the month

    Ensure no collecting range extends beyond today's date
    """
    today = date.today()
    current = date(start_date.year, start_date.month, 1)

    while current <= end_date:
        year, month = current.year, current.month
        first_of_month = date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        end_of_month = date(year, month, last_day)

        # Cap end_of_month to today's date
        end_of_month = min(end_of_month, today)

        # Skip if the month starts after today
        if first_of_month > today:
            break

        # First window: first 28 days of month
        first_window_end = first_of_month + timedelta(days=27)
        first_window_end = min(first_window_end, end_of_month, today)

        yield (first_of_month, first_window_end)

        # Second window: remainder of month
        remainder_start = first_window_end + timedelta(days=1)
        if remainder_start <= end_of_month and remainder_start <= today:
            yield (remainder_start, end_of_month)

        # Advance to next month
        next_month = month % 12 + 1
        next_year = year + (month // 12)
        current = date(next_year, next_month, 1)


def get_metrics_utility_config():
    return {
        'METRICS_UTILITY_SHIP_PATH': './shipped_data',
        'METRICS_UTILITY_SHIP_TARGET': 'directory',
    }


def main():
    config = get_metrics_utility_config()

    for since, until in month_ranges(START_DATE, END_DATE):
        # Base command arguments
        args = [
            'gather_automation_controller_billing_data',
            '--ship',
            f'--since={since.isoformat()}',
            f'--until={until.isoformat()}',
            '--force',
        ]

        if SSH_URL and SSH_USER:
            # Build remote SSH command list
            env_list = [f'{k}={v}' for k, v in config.items()]
            ssh_cmd = ['ssh', f'{SSH_USER}@{SSH_URL}', 'sudo', '-E', 'env', *env_list, 'metrics-utility', *args]
            print('Running remote:', ' '.join(ssh_cmd))
            result = subprocess.run(ssh_cmd, check=False, capture_output=True, text=True)
        else:
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
            result = subprocess.run(docker_cmd, check=False, capture_output=True, text=True)

        # Print output
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        # Continue through all date ranges (no premature exit)


if __name__ == '__main__':
    main()
