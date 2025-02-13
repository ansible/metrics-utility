import os
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
    excluded_columns = ['created', 'modified', 'job_created', 'install_uuid', 'duration',
                        'project_remote_id', 'project_name', 'original_host_name',
                        'organization_remote_id', 'host_remote_id', 'inventory_remote_id',
                        'ansible_connection_variable', 'changed', 'inventory_name',
                        'job_template_remote_id', 'ansible_host_variable',
                        'first_automation', 'last_automation',
                        'main_jobhostsummary_id', 'main_jobhostsummary_created',
                        'resolved_action','failed', 'resolved_role']

    valid_excluded_columns = [col for col in excluded_columns if col in df.columns]
    pprint('-----------------------------------------------')
    pprint(caption)
    pprint('-----------------------------------------------')
    pprint(df.drop(columns=valid_excluded_columns))
    pprint('-----------------------------------------------')

def set_ccspv2_vars():
    """
    Sets environment variables for CCSP v2 report generation. Useful for debugging
    in an editor like VS Code.
    """
    os.environ["METRICS_UTILITY_PRICE_PER_NODE"] = "11.55"
    os.environ["METRICS_UTILITY_REPORT_COMPANY_NAME"] = "Partner A"
    os.environ["METRICS_UTILITY_REPORT_EMAIL"] = "email@email.com"
    os.environ["METRICS_UTILITY_REPORT_END_USER_CITY"] = "Springfield"
    os.environ["METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME"] = "Customer A"
    os.environ["METRICS_UTILITY_REPORT_END_USER_COUNTRY"] = "US"
    os.environ["METRICS_UTILITY_REPORT_END_USER_STATE"] = "TX"
    os.environ["METRICS_UTILITY_REPORT_H1_HEADING"] = "CCSP NA Direct Reporting Template"
    os.environ["METRICS_UTILITY_REPORT_PO_NUMBER"] = "123"
    os.environ["METRICS_UTILITY_REPORT_RHN_LOGIN"] = "test_login"
    os.environ["METRICS_UTILITY_REPORT_SKU"] = "MCT3752MO"
    os.environ["METRICS_UTILITY_REPORT_SKU_DESCRIPTION"] = (
        "EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)"
    )
    os.environ["METRICS_UTILITY_REPORT_TYPE"] = "CCSPv2"
    os.environ["METRICS_UTILITY_SHIP_PATH"] = "./metrics_utility/test/test_data"
    os.environ["METRICS_UTILITY_SHIP_TARGET"] = "directory"

    # Simulating command-line arguments for debugging
    sys.argv = ["manage.py", "build_report", "--month", "2024-03", "--force", "--verbose"]
