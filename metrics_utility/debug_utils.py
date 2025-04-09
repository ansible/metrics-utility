import sys

from pprint import pprint


def print_debug(text):
    """
    Prints debug text only if '--verbose' is in the command-line arguments.

    :param text: The debug message to print.
    """
    if '--verbose' not in sys.argv:
        return
    print(text)
    sys.stdout.flush()


def print_data(df, caption):
    """
    Prints a DataFrame with a caption, excluding specific columns,
    only if '--verbose' is in the command-line arguments.

    :param df: The Pandas DataFrame to print.
    :param caption: A string caption describing the data.
    """
    if '--verbose' not in sys.argv:
        return

    df = df.reset_index()
    excluded_columns = [
        'created',
        'modified',
        'job_created',
        'install_uuid',
        'duration',
        'project_remote_id',
        'project_name',
        'original_host_name',
        'organization_remote_id',
        'host_remote_id',
        'inventory_remote_id',
        'ansible_connection_variable',
        'changed',
        'inventory_name',
        'job_template_remote_id',
        'ansible_host_variable',
        'first_automation',
        'last_automation',
        'main_jobhostsummary_id',
        'main_jobhostsummary_created',
        'resolved_action',
        'failed',
        'resolved_role',
    ]

    valid_excluded_columns = [col for col in excluded_columns if col in df.columns]
    pprint('-----------------------------------------------')
    pprint(caption)
    pprint('-----------------------------------------------')
    pprint(df.drop(columns=valid_excluded_columns))
    pprint('-----------------------------------------------')
    sys.stdout.flush()
