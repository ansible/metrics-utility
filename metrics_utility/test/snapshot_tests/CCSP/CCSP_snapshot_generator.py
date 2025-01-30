import random

#sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, os.pardir, os.pardir))))
from .. import snapshot_utils

import copy

entry_point_dir = snapshot_utils.get_entry_point_directory()

months = ['2024-02', '2024-03', '2024-04']
since_until_pairs = [
    {'since' : '2024-01-03', 'until' : '2024-01-07'},
    {'since' : '2024-01-03', 'until' : '2024-01-20'},
    {'since' : '2024-01-01', 'until' : '2024-02-15'},
    {'since' : '2024-01-15', 'until' : '2024-03-22'},
    {'since' : '2024-02-05', 'until' : '2024-03-15'},
    {'since' : '2024-03-02', 'until' : '2024-03-30'},
    {'since' : '2024-01', 'until' : '2024-03'},
    {'since' : '2024-02', 'until' : '2024-03'},
]

# Base dictionary
base_dict = {
    'METRICS_UTILITY_SHIP_TARGET':['directory'],
    'METRICS_UTILITY_SHIP_PATH': ["/awx_devel/awx-dev/metrics-utility/metrics_utility/test/test_data"],
    'METRICS_UTILITY_PRICE_PER_NODE': ['11.55', '15.01', '9.99'],
    'METRICS_UTILITY_REPORT_SKU': ['MCT3752MO', 'CSP8794CE', 'RHT1234PL'],
    'METRICS_UTILITY_REPORT_SKU_DESCRIPTION': [
        "EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)",
        "EX: Red Hat OpenShift Container Platform, Premium Support (1 Node, Monthly)",
        "EX: Red Hat Satellite, Standard Support (1 Node, Monthly)"
    ],
    'METRICS_UTILITY_REPORT_H1_HEADING': [
        "CCSP NA Direct Reporting Template",
        "CCSP EU Direct Reporting Template",
        "CCSP APAC Direct Reporting Template"
    ],
    'METRICS_UTILITY_REPORT_COMPANY_NAME': ['Partner A', 'Partner B', 'Partner C'],
    'METRICS_UTILITY_REPORT_EMAIL': ['email@email.com', 'support@partner.com', 'info@company.org'],
    'METRICS_UTILITY_REPORT_RHN_LOGIN': ['test_login', 'admin_user', 'guest_user'],
}

# Create env_vars_CCSPv2 by copying base_dict and extending it
env_vars_CCSPv2 = copy.deepcopy(base_dict)
env_vars_CCSPv2.update({
    'METRICS_UTILITY_REPORT_PO_NUMBER': ['123', '456', '789'],
    'METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME': ['Customer A', 'Customer B', 'Customer C'],
    'METRICS_UTILITY_REPORT_END_USER_CITY': ['Springfield', 'Rivertown', 'Lakeview'],
    'METRICS_UTILITY_REPORT_END_USER_STATE': ['TX', 'CA', 'NY'],
    'METRICS_UTILITY_REPORT_END_USER_COUNTRY': ['US', 'Canada', 'Mexico']
})

# Create env_vars_CCSP by copying base_dict and extending it
env_vars_CCSP = copy.deepcopy(base_dict)
env_vars_CCSP.update({
    'METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER': ['BUSINESS LEADER', 'DIRECTOR', 'CEO'],
    'METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER': ['PROCUREMENT LEADER', 'PURCHASING HEAD', 'SUPPLY MANAGER']
})



def select_env_vars(dictionary):
    values = {}

    for key in dictionary:
        strings = dictionary[key]
        random_string = random.choice(strings)
        values[key] = random_string
    return values


# generate tests for both reports
#METRICS_UTILITY_REPORT_TYPE

for report_type in ['CCSPv2']:
    dict_name = 'env_vars_' + report_type
    env_vars = select_env_vars(globals()[dict_name])
    env_vars['METRICS_UTILITY_REPORT_TYPE'] = report_type
    custom_params = { 'run_command' : True}

    for month in months:
        path = entry_point_dir + f'/data/{report_type}/snapshot_def_month_{month}.json'
        data = { 'env_vars' : env_vars, 'params' : ['manage.py', 'build_report', f'--month={month}', '--force'], 'custom_params' : custom_params}
        snapshot_utils.save_snapshot_definition(data, path )

    for since_until_pair in since_until_pairs:
        since = since_until_pair['since']
        until = since_until_pair['until']
        suffix = since + "__" + until
        path = entry_point_dir + f'/data/{report_type}/snapshot_def_since_until_{suffix}.json'
        data = { 'env_vars' : env_vars, 'params' : ['manage.py', 'build_report', f'--since={since}', f'--until={until}', '--force'], 'custom_params' : custom_params}
        
        snapshot_utils.save_snapshot_definition(data, path )

# run generated definitions
snapshot_utils.run_and_generate_snapshot_definitions(entry_point_dir + '/data/')


