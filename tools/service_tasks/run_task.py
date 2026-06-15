#!/usr/bin/env python
# ruff: noqa: E402, I001, T201
"""
Run tasks directly from the command line.

Usage:
    uv run scripts/run_task.py <task_name_or_id> [task_params_json]
    uv run scripts/run_task.py --groups                 # List all task groups
    uv run scripts/run_task.py --list-groups            # List all task groups with tasks
    uv run scripts/run_task.py --help <task_or_id>      # Show help for a specific task

Examples:
    # Run task functions directly
    uv run scripts/run_task.py hello_world
    uv run scripts/run_task.py daily_metrics_rollup '{"summary_date": "2024-01-01"}'
    uv run scripts/run_task.py cleanup_old_tasks '{"days_old": 7, "dry_run": true}'
    uv run scripts/run_task.py collect_hourly_metrics '{"collector_type": "job_host_summary_service"}'

    # Run tasks from task groups (uses default args from group config)
    uv run scripts/run_task.py daily_task_cleanup
    uv run scripts/run_task.py hourly_job_host_summary

    # Override default args for task group tasks
    uv run scripts/run_task.py daily_task_cleanup '{"days_old": 10, "dry_run": true}'
    uv run scripts/run_task.py hourly_job_host_summary '{"collector_type": "unified_jobs"}'
"""

import json
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent.parent / 'metrics-service'
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'metrics_service.settings')
import django

django.setup()

# Now we can import from the apps
from apps.tasks.task_groups import TASK_GROUPS, get_all_enabled_tasks
from apps.tasks.tasks import TASK_FUNCTIONS, TASK_METADATA


def list_task_groups():
    """Display all task groups with their tasks."""
    print('\nTask Groups:')
    print('=' * 80)

    for group in TASK_GROUPS:
        print(f'\n{group.name}:')
        print(f'  Description: {group.description}')
        if group.feature_flag:
            print(f'  Feature Flag: {group.feature_flag}')

        enabled_tasks = group.get_enabled_tasks()
        if enabled_tasks:
            print(f'  Enabled Tasks ({len(enabled_tasks)}):')
            for task in enabled_tasks:
                task_id = task['task_id']
                function = task['function']
                description = task.get('description', 'No description')
                cron = task.get('cron', 'N/A')
                print(f'    {task_id:35} -> {function:30} [{cron}]')
                print(f'      {description}')
        else:
            print('  No enabled tasks')

    print('\n' + '=' * 80)
    print('\nUsage: uv run ./run_task.py <task_id> [task_params_json]')


def list_available_tasks():
    """Display all available tasks with their descriptions."""
    print('\nAvailable Task Functions:')
    print('=' * 80)

    # Group by category
    by_category = {}
    for task_name, metadata in TASK_METADATA.items():
        category = metadata.get('category', 'Other')
        if category not in by_category:
            by_category[category] = []
        by_category[category].append((task_name, metadata))

    # Print by category
    for category in sorted(by_category.keys()):
        print(f'\n{category}:')
        print('-' * 80)
        for task_name, metadata in sorted(by_category[category]):
            description = metadata.get('description', 'No description')
            print(f'  {task_name:30} - {description}')

    print('\n' + '=' * 80)
    print(f'Total: {len(TASK_FUNCTIONS)} task functions available')

    # Also show task groups summary
    all_enabled_tasks = get_all_enabled_tasks()
    print(f'\nTask Groups: {len(TASK_GROUPS)} groups, {len(all_enabled_tasks)} enabled tasks')
    print('  Use --groups or --list-groups to see task groups')

    print('\nUsage: uv run ./run_task.py <task_name_or_id> [task_params_json]')


def show_task_help(task_name):  # noqa: PLR0915
    """Show detailed help for a specific task (function or group task ID)."""
    # First check if it's a task group task
    all_enabled_tasks = get_all_enabled_tasks()
    if task_name in all_enabled_tasks:
        task_config = all_enabled_tasks[task_name]
        function_name = task_config['function']

        print(f'\nTask Group Task: {task_name}')
        print('=' * 80)
        print(f'Function: {function_name}')
        print(f'Group: {task_config["group"]}')
        print(f'Description: {task_config.get("description", "No description")}')
        print(f'Category: {task_config.get("category", "Unknown")}')
        print(f'Schedule: {task_config.get("cron", "N/A")}')

        if task_config.get('feature_flag'):
            print(f'Feature Flag: {task_config["feature_flag"]}')

        default_args = task_config.get('args', {})
        if default_args:
            print('\nDefault Arguments:')
            print(f'  {json.dumps(default_args, indent=2)}')

        print('\nExamples:')
        print('  # Run with default args:')
        print(f'    uv run ./run_task.py {task_name}')
        if default_args:
            print('  # Override default args:')
            print(f"    uv run ./run_task.py {task_name} '{json.dumps(default_args)}'")

        # Also show the underlying function metadata if available
        if function_name in TASK_METADATA:
            print(f'\nUnderlying Function: {function_name}')
            metadata = TASK_METADATA[function_name]
            params = metadata.get('parameters', {})
            if params:
                print('Parameters:')
                for param_name, param_info in params.items():
                    required = param_info.get('required', False)
                    param_type = param_info.get('type', 'any')
                    default = param_info.get('default', 'N/A')
                    desc = param_info.get('description', '')
                    req_str = ' (required)' if required else f' (default: {default})'
                    print(f'  {param_name} ({param_type}){req_str}')
                    if desc:
                        print(f'    {desc}')

        print('=' * 80)
        return True

    # Otherwise check if it's a task function
    if task_name not in TASK_METADATA:
        print(f"Error: Task '{task_name}' not found in task functions or task groups")
        return False

    metadata = TASK_METADATA[task_name]
    print(f'\nTask Function: {task_name}')
    print('=' * 80)
    print(f'Category: {metadata.get("category", "Unknown")}')
    print(f'Description: {metadata.get("description", "No description")}')

    params = metadata.get('parameters', {})
    if params:
        print('\nParameters:')
        for param_name, param_info in params.items():
            required = param_info.get('required', False)
            param_type = param_info.get('type', 'any')
            default = param_info.get('default', 'N/A')
            desc = param_info.get('description', '')
            req_str = ' (required)' if required else f' (default: {default})'
            print(f'  {param_name} ({param_type}){req_str}')
            if desc:
                print(f'    {desc}')

    examples = metadata.get('examples', [])
    if examples:
        print('\nExamples:')
        for example in examples:
            name = example.get('name', '')
            data = example.get('data', {})
            data_str = json.dumps(data) if data else ''
            print(f'  {name}:')
            print(f"    uv run ./run_task.py {task_name} '{data_str}'")

    print('=' * 80)
    return True


def run_task(task_name, params=None):
    """Run a task with the given parameters (supports both function names and task group IDs)."""
    # Check if this is a task group task ID
    all_enabled_tasks = get_all_enabled_tasks()
    function_name = task_name
    task_params = {}

    if task_name in all_enabled_tasks:
        # This is a task group task - get function name and default args
        task_config = all_enabled_tasks[task_name]
        function_name = task_config['function']
        task_params = task_config.get('args', {}).copy()

        print(f'\nRunning task group task: {task_name}')
        print(f'Function: {function_name}')
        print(f'Group: {task_config["group"]}')

        # Parse user-provided parameters and merge with defaults
        if params:
            try:
                user_params = json.loads(params)
                task_params.update(user_params)
                print('User parameters merged with defaults')
            except json.JSONDecodeError as e:
                print(f'Error parsing JSON parameters: {e}')
                return False
    else:
        # This is a direct function name
        if function_name not in TASK_FUNCTIONS:
            print(f"Error: Task '{task_name}' not found in task functions or task groups")
            print("\nUse 'uv run ./run_task.py' to see available tasks")
            print("Use 'uv run ./run_task.py --groups' to see task groups")
            return False

        print(f'\nRunning task function: {task_name}')

        # Parse parameters
        if params:
            try:
                task_params = json.loads(params)
            except json.JSONDecodeError as e:
                print(f'Error parsing JSON parameters: {e}')
                return False

    if task_params:
        print(f'Parameters: {json.dumps(task_params, indent=2)}')
    print('-' * 80)

    try:
        # Get the task function
        task_function = TASK_FUNCTIONS[function_name]

        # Run the task
        result = task_function(**task_params)

        # Display result
        print('\nTask completed!')
        print('=' * 80)
        print('Result:')
        print(json.dumps(result, indent=2, default=str))
        print('=' * 80)

        return result['status'] == 'success'

    except Exception as e:
        print(f'\nError running task: {e}')
        import traceback

        traceback.print_exc()
        return False


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        list_available_tasks()
        return 0

    task_name = sys.argv[1]

    # Handle help flags
    if task_name in ['-h', '--help', 'help']:
        if len(sys.argv) > 2:
            # Show help for specific task
            show_task_help(sys.argv[2])
        else:
            list_available_tasks()
        return 0

    # Handle group listing flags
    if task_name in ['--groups', '--list-groups', '-g']:
        list_task_groups()
        return 0

    # Get optional parameters
    params = sys.argv[2] if len(sys.argv) > 2 else None

    # Run the task
    success = run_task(task_name, params)
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
