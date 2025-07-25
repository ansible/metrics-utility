import copy

from datetime import datetime

from metrics_utility.test.snapshot_tests import snapshot_utils


# You can run this script by using:
# uv run python -m metrics_utility.test.snapshot_tests.CCSP.CCSP_snapshot_generator


def select_env_vars(dictionary, position):
    values = {}

    for key in dictionary:
        strings = dictionary[key]

        if strings is None:
            continue

        if len(strings) == 1:
            values[key] = strings[0]
        else:
            values[key] = strings[position]

    return values


def get_data():
    months = ['2024-02', '2024-03', '2024-04']
    since_until_pairs = [
        {'since': '2024-02-01', 'until': '2024-02-29'},
        {'since': '2024-03-01', 'until': '2024-03-31'},
        {'since': '2024-02-15', 'until': '2024-03-22'},
        {'since': '2024-03-05', 'until': '2024-03-15'},
        {'since': '2024-02-05', 'until': '2024-04-30'},
        {'since': '2024-03-02', 'until': '2024-04-20'},
    ]

    # Base dictionary
    base_dict = {
        'METRICS_UTILITY_SHIP_TARGET': ['directory'],
        'METRICS_UTILITY_SHIP_PATH': ['./metrics_utility/test/test_data'],
        'METRICS_UTILITY_PRICE_PER_NODE': ['11.55', '15.01', '9.99'],
        'METRICS_UTILITY_REPORT_SKU': ['MCT3752MO', 'CSP8794CE', 'RHT1234PL'],
        'METRICS_UTILITY_REPORT_SKU_DESCRIPTION': [
            'EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)',
            'EX: Red Hat OpenShift Container Platform, Premium Support (1 Node, Monthly)',
            'EX: Red Hat Satellite, Standard Support (1 Node, Monthly)',
        ],
        'METRICS_UTILITY_REPORT_H1_HEADING': [
            'CCSP NA Direct Reporting Template',
            'CCSP EU Direct Reporting Template',
            'CCSP APAC Direct Reporting Template',
        ],
        'METRICS_UTILITY_REPORT_COMPANY_NAME': ['Partner A', 'Partner B', 'Partner C'],
        'METRICS_UTILITY_REPORT_EMAIL': ['email@email.com', 'support@partner.com', 'info@company.org'],
        'METRICS_UTILITY_REPORT_RHN_LOGIN': ['test_login', 'admin_user', 'guest_user'],
    }

    # Create env_vars_CCSPv2 by copying base_dict and extending it
    env_vars_ccspv2 = copy.deepcopy(base_dict)
    env_vars_ccspv2.update(
        {
            'METRICS_UTILITY_REPORT_PO_NUMBER': ['123', '456', '789'],
            'METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME': ['Customer A', 'Customer B', 'Customer C'],
            'METRICS_UTILITY_REPORT_END_USER_CITY': ['Springfield', 'Rivertown', 'Lakeview'],
            'METRICS_UTILITY_REPORT_END_USER_STATE': ['TX', 'CA', 'NY'],
            'METRICS_UTILITY_REPORT_END_USER_COUNTRY': ['US', 'Canada', 'Mexico'],
        }
    )

    # Create env_vars_CCSP by copying base_dict and extending it
    env_vars_ccsp = copy.deepcopy(base_dict)
    env_vars_ccsp.update(
        {
            'METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER': ['BUSINESS LEADER', 'DIRECTOR', 'CEO'],
            'METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER': ['PROCUREMENT LEADER', 'PURCHASING HEAD', 'SUPPLY MANAGER'],
        }
    )

    return (months, since_until_pairs, env_vars_ccspv2, env_vars_ccsp)


def generate_monthly_reports(report_type, months, dictionary, custom_params, entry_point_dir):
    for month in months:
        # Default to first set of environment vars
        env_vars = select_env_vars(dictionary, 0)

        # For specific month, use the third set of environment vars
        if month == '2024-04':
            env_vars = select_env_vars(dictionary, 2)

        env_vars['METRICS_UTILITY_REPORT_TYPE'] = report_type

        path = entry_point_dir + f'/data/{report_type}/snapshot_def_{month}.json'
        data = {'env_vars': env_vars, 'params': ['manage.py', 'build_report', f'--month={month}', '--force'], 'custom_params': custom_params}
        snapshot_utils.save_snapshot_definition(data, path)


def generate_since_until_reports(report_type, since_until_pairs, dictionary, custom_params, entry_point_dir):
    for pair in since_until_pairs:
        # Default to second set of environment vars
        env_vars = select_env_vars(dictionary, 1)

        # Adjust for these specific date ranges
        if pair['since'] == '2024-02-01' and pair['until'] == '2024-02-29':
            env_vars = select_env_vars(dictionary, 0)
        if pair['since'] == '2024-03-01' and pair['until'] == '2024-03-31':
            env_vars = select_env_vars(dictionary, 0)

        env_vars['METRICS_UTILITY_REPORT_TYPE'] = report_type

        since = pair['since']
        until = pair['until']
        suffix = since + '--' + until

        path = entry_point_dir + f'/data/{report_type}/snapshot_def_{suffix}.json'
        data = {
            'env_vars': env_vars,
            'params': ['manage.py', 'build_report', f'--since={since}', f'--until={until}', '--force'],
            'custom_params': custom_params,
        }
        snapshot_utils.save_snapshot_definition(data, path)


def generate():
    entry_point_dir = snapshot_utils.get_entry_point_directory()

    months, since_until_pairs, env_vars_ccspv2, env_vars_ccsp = get_data()

    for report_type in ['CCSP', 'CCSPv2']:
        custom_params = {'run_command': 'Yes', 'generated': datetime.now().date().strftime('%Y-%m-%d')}

        # Decide which dictionary to use
        if report_type == 'CCSP':
            dictionary = env_vars_ccsp
        else:
            dictionary = env_vars_ccspv2

        # Generate the monthly and custom range reports
        generate_monthly_reports(report_type, months, dictionary, custom_params, entry_point_dir)
        generate_since_until_reports(report_type, since_until_pairs, dictionary, custom_params, entry_point_dir)

    # Finally, run all generated definitions
    snapshot_utils.run_and_generate_snapshot_definitions(entry_point_dir + '/data/')


if __name__ == '__main__':
    generate()
