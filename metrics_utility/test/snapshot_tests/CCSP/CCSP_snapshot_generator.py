from .. import snapshot_utils
from datetime import datetime
import copy

entry_point_dir = snapshot_utils.get_entry_point_directory()

months = ['2024-02', '2024-03', '2024-04']
since_until_pairs = [
    {'since' : '2024-02-01', 'until' : '2024-02-29'},
    {'since' : '2024-03-01', 'until' : '2024-03-31'},
    {'since' : '2024-02-15', 'until' : '2024-03-22'},
    {'since' : '2024-03-05', 'until' : '2024-03-15'},
    {'since' : '2024-02-05', 'until' : '2024-04-30'},
    {'since' : '2024-03-02', 'until' : '2024-04-20'},
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
env_vars_ccspv2 = copy.deepcopy(base_dict)
env_vars_ccspv2.update({
    'METRICS_UTILITY_REPORT_PO_NUMBER': ['123', '456', '789'],
    'METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME': ['Customer A', 'Customer B', 'Customer C'],
    'METRICS_UTILITY_REPORT_END_USER_CITY': ['Springfield', 'Rivertown', 'Lakeview'],
    'METRICS_UTILITY_REPORT_END_USER_STATE': ['TX', 'CA', 'NY'],
    'METRICS_UTILITY_REPORT_END_USER_COUNTRY': ['US', 'Canada', 'Mexico']
})

# Create env_vars_CCSP by copying base_dict and extending it
env_vars_ccsp = copy.deepcopy(base_dict)
env_vars_ccsp.update({
    'METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER': ['BUSINESS LEADER', 'DIRECTOR', 'CEO'],
    'METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER': ['PROCUREMENT LEADER', 'PURCHASING HEAD', 'SUPPLY MANAGER']
})

# generate tests for both reports

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

for report_type in ['CCSP','CCSPv2']:
    custom_params = {'run_command' : 'Yes', 'generated' :  datetime.now().date().strftime("%Y-%m-%d")}
    
    dictionary = None
    if (report_type == 'CCSPv2'):
        dictionary = env_vars_ccspv2

    if (report_type == 'CCSP'):
        dictionary = env_vars_ccsp
    
    # monthly reports
    for month in months:
        env_vars = select_env_vars(dictionary, 0)

        # test also some different env_vars
        if (month == '2024-04'):
            env_vars = select_env_vars(dictionary, 2)
    
        env_vars['METRICS_UTILITY_REPORT_TYPE'] = report_type

        path = entry_point_dir + f'/data/{report_type}/snapshot_def_{month}.json'
        data = { 'env_vars' : env_vars, 'params' : ['manage.py', 'build_report', f'--month={month}', '--force'], 'custom_params' : custom_params}
        snapshot_utils.save_snapshot_definition(data, path )

    # reports with arbitrary ranges
    for since_until_pair in since_until_pairs:
        env_vars = select_env_vars(dictionary, 1)

        # Those files are going to be compared to months above, so they need to have the same env vars
        if (since_until_pair['since'] == '2024-02-01' and since_until_pair['until'] == '2024-02-29'):
            env_vars = select_env_vars(dictionary, 0 )
        
        if (since_until_pair['since'] == '2024-03-01' and since_until_pair['until'] == '2024-03-31'):
            env_vars = select_env_vars(dictionary, 0 )
   
        env_vars['METRICS_UTILITY_REPORT_TYPE'] = report_type

        since = since_until_pair['since']
        until = since_until_pair['until']
        suffix = since + "--" + until
        path = entry_point_dir + f'/data/{report_type}/snapshot_def_{suffix}.json'
        data = { 'env_vars' : env_vars, 'params' : ['manage.py', 'build_report', f'--since={since}', f'--until={until}', '--force'], 'custom_params' : custom_params}
        snapshot_utils.save_snapshot_definition(data, path )

# run generated definitions
snapshot_utils.run_and_generate_snapshot_definitions(entry_point_dir + '/data/')


