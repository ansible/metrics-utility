import datetime
import json
import os
import random
import string
import subprocess
import time

import requests

from helper_ocp_prepare import create_oc_environs


# Configuration

# get api url from env, if not provided, use default
API_URL = os.getenv('API_URL', 'https://localhost:8030/api/controller/v2')

if not API_URL.endswith('/api/controller/v2'):
    API_URL += '/api/controller/v2'

API_GATEWAY_URL = API_URL.replace('/controller/v2', '/gateway/v1')
USERNAME = os.getenv('USERNAME', 'admin')
PASSWORD = os.getenv('PASSWORD', 'admin')

SSH_URL = os.getenv('SSH_URL')
SSH_USER = os.getenv('SSH_USER', 'ec2-user')

OC_LOGIN_COMMAND = os.getenv('OC_LOGIN_COMMAND', '')

ENVIRONMENT = os.getenv('ENVIRONMENT', 'local')
INV_PREFIX = os.getenv('INV_PREFIX')

if not INV_PREFIX:
    # random 3 high letters
    INV_PREFIX = 'mock' + ''.join(random.choices(string.ascii_uppercase, k=4))

if SSH_URL and ENVIRONMENT is None:
    print('ENVIRONMENT are not set, exiting')
    exit(1)

if OC_LOGIN_COMMAND and ENVIRONMENT is None:
    print('ENVIRONMENT are not set, exiting')
    exit(1)

print(f'API_URL: {API_URL}')
print(f'USERNAME: {USERNAME}')
print(f'PASSWORD: {PASSWORD}')
print(f'SSH_URL: {SSH_URL}')
print(f'SSH_USER: {SSH_USER}')
print(f'ENVIRONMENT: {ENVIRONMENT}')
print(f'INV_PREFIX: {INV_PREFIX}')
print(f'OC_LOGIN_COMMAND: {OC_LOGIN_COMMAND}')
print(f'API_GATEWAY_URL: {API_GATEWAY_URL}')

if os.getenv('POD_NAME') and os.getenv('NAMESPACE'):
    print(f'POD_NAME: {os.getenv("POD_NAME")}')
    print(f'NAMESPACE: {os.getenv("NAMESPACE")}')


VERIFY_SSL = False  # Set to True if you have valid SSL certificates
PAGE_SIZE = 100

# Disable warnings for insecure SSL (if VERIFY_SSL=False)
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)


def run(sql_script):
    try:
        print(sql_script)
        stderr = ''
        stdout = ''
        if ENVIRONMENT == 'local':
            command = ['docker', 'exec', '-i', 'tools_postgres_1', 'psql', '-U', 'awx']
            process = subprocess.run(command, input=sql_script.encode(), capture_output=True)
            stderr = process.stderr.decode()
            stdout = process.stdout.decode()

        if ENVIRONMENT == 'RPM':
            # Send SQL over SSH to awx-manage dbshell via stdin
            process = subprocess.run(
                ['ssh', f'{SSH_USER}@{SSH_URL}', 'sudo', 'awx-manage', 'dbshell'], input=sql_script, text=True, capture_output=True
            )
            stdout = process.stdout
            stderr = process.stderr

        if ENVIRONMENT == 'containerized':
            # Stream the SQL over SSH stdin directly into awx-manage dbshell inside the container.
            remote_cmd = 'podman exec -i automation-controller-web awx-manage dbshell'
            process = subprocess.run(['ssh', f'{SSH_USER}@{SSH_URL}', remote_cmd], input=sql_script, text=True, capture_output=True)
            stdout = process.stdout
            stderr = process.stderr

        if ENVIRONMENT == 'OpenShift':
            # extract pod name from OC_COMMAND
            pod_name = os.getenv('POD_NAME')
            namespace = os.getenv('NAMESPACE')

            print(f'pod_name: {pod_name}')
            print(f'namespace: {namespace}')

            # run the command
            # sql_script is the variable that contains the sql script
            # forget the OC_COMMAND, use the pod name and namespace to run the command
            # note that -c does not work for dbshell

            # pipe sql script to the command
            command = [
                'oc',
                'exec',
                '-i',  # keep STDIN open
                '-n',
                namespace,
                pod_name,
                '--',
                'awx-manage',
                'dbshell',
            ]

            # Run the command and pipe the SQL into STDIN
            result = subprocess.run(
                command,
                input=sql_script,  # <<–– here's where the script goes
                text=True,  # treat stdin/stdout as str instead of bytes
                capture_output=True,  # optional: collect results for logging
                check=True,  # raise if the command fails
            )

            stdout = result.stdout
            stderr = result.stderr

        print(stderr)
        return stdout
    except Exception as e:
        print(f'Failed to run SQL script: {e}')
        return ''


def delete_job_templates():
    # https://localhost:8030/api/controller/v2/unified_job_templates
    url = f'{API_URL}/unified_job_templates/?type=job_template%2Cworkflow_job_template&order_by=name&page=1&page_size=100&search=mockA_test'
    resp = requests.get(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)

    resp.raise_for_status()
    data = resp.json()

    # count
    count = data['count']
    print(f'Found {count} job templates.')

    for item in data['results']:
        delete_job_template(item['id'])
        # print(item)


def delete_job_template(id):
    print(f'Deleting job template {id}')
    # https://localhost:8030/api/controller/v2/job_templates/6/
    #
    failed = True
    while True:
        try:
            url = f'{API_URL}/job_templates/{id}/'
            resp = requests.delete(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
            resp.raise_for_status()
            print(f'Deleted job template {id}')
            if resp.status_code not in (200, 202, 204):
                print(f'Failed to delete job template {id}: {resp.status_code} - {resp.text}')
            else:
                failed = False
                break
        except Exception as e:
            print(f'Failed to delete job template {id}: {e}')
            failed = True

        if failed:
            print(f'Failed to delete job template {id}, maybe jobs are still running, trying again in 10 seconds')
            time.sleep(10)
        else:
            break


def delete_projects():
    # delete mock project

    url = f'{API_URL}/projects/'
    resp = requests.get(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    data = resp.json()
    if data['count'] == 0:
        print('No project named "MockA_Test_Project" found to delete.')
        return

    for project in data['results']:
        project_id = project['id']
        del_url = f'{API_URL}/projects/{project_id}/'
        del_resp = requests.delete(del_url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if del_resp.status_code in (200, 202, 204):
            print(f'Deleted project {project["name"]} (id={project_id})')
        else:
            print(f'Failed to delete project {project["name"]} (id={project_id}): {del_resp.status_code} - {del_resp.text}')


def create_main_project(organization_id):
    # https://localhost:8030/api/controller/v2/projects/
    url = f'{API_URL}/projects/'
    data = {
        'name': 'MockA_Test_Project',
        'organization': organization_id,
        'scm_type': 'git',
        'scm_url': 'https://github.com/MilanPospisil/PlaybookExamples',
    }

    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)

    if resp.status_code not in (200, 201, 202):
        print(f'Failed to create project {data["name"]}: {resp.status_code} - {resp.text}')

    print(f'Created project {data["name"]}: {resp.status_code}')

    print('Waiting for sync')

    # repeatedly get the project and check field status: "successful"
    project_id = resp.json()['id']
    status = None
    for _ in range(60):
        proj_resp = requests.get(f'{API_URL}/projects/{project_id}/', auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if proj_resp.status_code not in (200, 201, 202):
            print(f'Failed to get project status: {proj_resp.status_code} - {proj_resp.text}')
            time.sleep(1)
            continue
        status = proj_resp.json().get('status')
        print(f'Project sync status: {status}')
        if status == 'successful':
            break
        time.sleep(5)
    else:
        print(f'Project {project_id} did not reach "successful" status after waiting.')

    return resp.json()['id']


def create_job_template(name, inventory_id, project_id):
    # https://localhost:8030/api/controller/v2/job_templates/
    # create with only name, inventory, project, playbook
    url = f'{API_URL}/job_templates/'

    data = {'name': name, 'inventory': inventory_id, 'project': project_id, 'playbook': 'playbook.yml'}

    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)
    if resp.status_code not in (200, 201, 202):
        print(f'Failed to create job template {name}: {resp.status_code} - {resp.text}')

    return resp.json()['id']


def launch_job_template(job_template_id):
    # https://localhost:8030/api/controller/v2/job_templates/12/launch/
    url = f'{API_URL}/job_templates/{job_template_id}/launch/'
    resp = requests.post(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    if resp.status_code not in (200, 201, 202):
        print(f'Failed to launch job template {job_template_id}: {resp.status_code} - {resp.text}')
    else:
        print(f'Launched job template {job_template_id}')
    return resp.json()['id']


def get_all_inventories():
    """
    Retrieve all inventories via paginated API.
    Returns a list of inventory objects.
    """
    inventories = []
    url = f'{API_URL}/inventories/'
    params = {'order_by': 'name', 'page': 1, 'page_size': PAGE_SIZE}

    while url:
        response = requests.get(url, auth=(USERNAME, PASSWORD), params=params, verify=VERIFY_SSL)
        response.raise_for_status()
        data = response.json()

        # DRF-style pagination: 'results' key
        results = data.get('results', data)
        inventories.extend(results)

        # Next page URL (if any)
        url = data.get('next')
        params = {}  # Only send params on first request

    return inventories


def delete_inventory(inv_id, name):
    """
    Delete a single inventory by ID.
    """
    url = f'{API_URL}/inventories/{inv_id}/'
    resp = requests.delete(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    # wait for 10 seconds
    time.sleep(10)
    if resp.status_code in (200, 202, 204):
        print(f'Deleted inventory {inv_id}, {name}: {resp.status_code}')
    else:
        print(f'Failed to delete inventory {inv_id}, {name}: {resp.status_code} - {resp.text}')


def delete_inventories():
    # get all inventories and delete each
    all_invs = get_all_inventories()
    print(f'Found {len(all_invs)} inventories.')

    # print only the names of the inventories
    for inv in all_invs:
        print(inv['name'])

    # Delete each inventory
    for inv in all_invs:
        # Inventory object may include 'id' key
        inv_id = inv.get('id') if isinstance(inv, dict) else inv
        name = inv.get('name') if isinstance(inv, dict) else inv
        delete_inventory(inv_id, name)


def create_inventory(name, organization_id=1):
    # First, check if inventory already exists
    data = {'name': name, 'organization': organization_id, 'prevent_instance_group_fallback': False, 'variables': ''}
    url = f'{API_URL}/inventories/'
    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)
    if resp.status_code in (200, 201, 202):
        print(f'Created inventory {name}: {resp.status_code}')
    else:
        print(f'Failed to create inventory {name}: {resp.status_code} - {resp.text}')

    return resp.json()['id']


def create_inventory_hosts(inv_id, prefix, count, variables, data):
    if not data:
        data = {}

    host_names = data.get('host_names', None)

    for i in range(count):
        if not host_names:
            create_host(inv_id, f'{prefix}_{i + 1}', variables)
        else:
            create_host(inv_id, host_names[i], variables)


def list_main_jobhostsummary():
    # select all from main_jobhostsummary
    sql = """
    select * from main_jobhostsummary order by modified;
    """
    res = run(sql)
    print(res)


def count_main_jobhostsummary():
    # select count(*) from main_jobhostsummary
    sql = """
    select count(*) from main_jobhostsummary;
    """
    res = run(sql)

    """
     count
    -------
        22
    (1 row)
    """
    # Parse number from example above
    count = res.split('\n')[2]

    # remove all whitespaces
    count = count.strip()
    count = int(count)
    return count


def delete_main_jobhostsummary():
    # delete all from main_jobhostsummary
    sql = """
    DELETE FROM main_jobhostsummary;
    """
    print('Deleting main_jobhostsummary')
    run(sql)


def create_inventory_and_template(name, project_id, organization_id=1):
    """
    Create inventory and job template without creating any hosts.
    Hosts will be added separately using create_host function.
    """
    print(f'Creating inventory {name} for organization {organization_id} (no hosts)')

    inv_id = create_inventory(name, organization_id)
    job_template_id = create_job_template(name, inv_id, project_id)

    return {'inv_id': inv_id, 'job_template_id': job_template_id, 'name': name}


def sql_result_to_list(res):
    # the input is result of psql command
    # out is list of dicts, each dict is a row
    # each dict has keys as column names, values as column values
    lines = res.strip().split('\n')
    header = lines[0].strip().split('|')
    header = [h.strip() for h in header]
    data_lines = lines[1:]

    data = []
    for line in data_lines:
        parts = [p.strip() for p in line.split('|')]
        if len(parts) < 2:
            continue
        data.append(dict(zip(header, parts)))

    # print the whole table to test it
    print(data)

    return data


# dates is list of dates, each date is a string in format '2023-01-01 01:00:00'
# dates[0] is the date for job_id 1, dates[1] is the date for job_id 2, etc.
def set_different_modified_dates(dates):
    print('Updating dates for job host summary table')
    # Load all job_host_summary entries
    sql_select = """
    SELECT id, job_id FROM main_jobhostsummary ORDER BY id;
    """
    res = run(sql_select)

    data = sql_result_to_list(res)

    # select job_id as array from data
    job_ids = [int(row['job_id']) for row in data]

    # select distinct job_ids
    job_ids = list(set(job_ids))

    # sort job_ids
    job_ids.sort()

    print('job_ids after set')
    print(job_ids)

    job_id_to_index = {}
    for i, job_id in enumerate(job_ids):
        job_id_to_index[job_id] = i

    print('job_id_to_index')
    print(job_id_to_index)

    print(dates)
    print(dates)

    # do bulk update, concatenate all sql_update into one string
    sql_update = ''
    for row in data:
        job_id = int(row['job_id'])
        index = job_id_to_index[job_id]
        modified_date = dates[index]
        sql_update += f"""
        UPDATE main_jobhostsummary SET modified = '{modified_date}' WHERE job_id = {int(row['job_id'])};
        """
        sql_update += f"""
        UPDATE main_jobhostsummary SET created = '{modified_date}' WHERE job_id = {int(row['job_id'])};
        """
    print(sql_update)
    run(sql_update)


def oc_login():
    # subprocess run
    subprocess.run(OC_LOGIN_COMMAND, shell=True)


def get_all_organizations():
    """
    Retrieve all organizations via paginated API.
    Returns a list of organization objects.
    """
    organizations = []
    url = f'{API_GATEWAY_URL}/organizations/'
    params = {'order_by': 'name', 'page': 1, 'page_size': PAGE_SIZE}

    while url:
        response = requests.get(url, auth=(USERNAME, PASSWORD), params=params, verify=VERIFY_SSL)
        response.raise_for_status()
        data = response.json()

        # DRF-style pagination: 'results' key
        results = data.get('results', data)
        organizations.extend(results)

        # Next page URL (if any)
        url = data.get('next')
        params = {}  # Only send params on first request

    return organizations


def delete_organization(org_id):
    """
    Delete a single organization by ID.
    """
    url = f'{API_GATEWAY_URL}/organizations/{org_id}/'
    resp = requests.delete(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    if resp.status_code in (200, 202, 204):
        print(f'Deleted organization {org_id}: {resp.status_code}')
    else:
        print(f'Failed to delete organization {org_id}: {resp.status_code} - {resp.text}')


def delete_organizations():
    """
    Get all organizations and delete each one that is not named 'Default'.
    """
    all_orgs = get_all_organizations()
    print(f'Found {len(all_orgs)} organizations.')

    # Delete each organization except Default
    for org in all_orgs:
        if org.get('name') != 'Default':
            org_id = org.get('id') if isinstance(org, dict) else org
            delete_organization(org_id)
        else:
            print(f'Skipping deletion of Default organization (id={org.get("id")})')


def create_organization(name):
    """
    Create a new organization.
    """
    url = f'{API_GATEWAY_URL}/organizations/'
    data = {'name': name}
    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)
    if resp.status_code in (200, 201, 202):
        print(f'Created organization {name}')
    else:
        print(f'Failed to create organization {name}: {resp.status_code} - {resp.text}')

    # give gateway some time to create the organization in controller
    print('Waiting some time for organization to be created in controller')
    time.sleep(30)

    # waiting for controller sync
    id = search_controller_organization_id(name)
    return id


def search_controller_organization_id(name):
    url = f'{API_URL}/organizations?limit=100'
    resp = requests.get(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    results = resp.json()['results']

    for result in results:
        if result['name'] == name:
            print(f'Found organization {name} with id {result["id"]}')
            return result['id']
    return None


def get_all_hosts(inventory_id):
    # https://localhost:8030/api/controller/v2/inventories/487/hosts/
    url = f'{API_URL}/inventories/{inventory_id}/hosts/'
    resp = requests.get(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    return resp.json()['results']


def search_controller_inventory_id(name):
    url = f'{API_URL}/inventories?limit=100'
    resp = requests.get(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    results = resp.json()['results']

    for result in results:
        if result['name'] == name:
            print(f'Found inventory {name} with id {result["id"]}')
            return result['id']
    return None


def update_host_facts(inventory_name, host_name, facts):
    # load all hosts from the inventory and search for host_name, return id
    inventory_id = search_controller_inventory_id(inventory_name)

    hosts = get_all_hosts(inventory_id)
    for host in hosts:
        if host['name'] == host_name:
            print(f'Found host {host_name} from inventory {inventory_id} with id {host["id"]}')
            host_id = host['id']
            break

    # convert facts to jsonb
    facts_json = json.dumps(facts).replace("'", "''")

    # build SQL with JSONB cast
    sql = f"UPDATE main_host SET ansible_facts = '{facts_json}'::jsonb WHERE id = {host_id};"
    run(sql)


def delete_all_hosts():
    # using API
    url = f'{API_URL}/hosts/?limit=100'
    print(f'Deleting all hosts from {url}')
    resp = requests.get(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    hosts = resp.json()['results']
    print(f'Found {len(hosts)} hosts')
    for host in hosts:
        host_id = host['id']
        url = f'{API_URL}/hosts/{host_id}/'
        resp = requests.delete(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        print(f'Deleted host {host_id}: {resp.status_code}')


def create_host(inventory_obj, host_name, variables='', facts=None):
    """
    Append a single host to an existing inventory with specified variables and facts.
    Automatically tracks host count in the inventory object.

    Args:
        inventory_obj (dict): Inventory object with structure {'inv_id': int, 'job_template_id': int, 'name': str, 'hosts_count': int}
        host_name (str): The name of the host to create
        variables (str): Ansible variables for the host (default: empty string)
        facts (dict): Ansible facts to set for the host (default: None)

    Returns:
        int: The ID of the created host
    """
    inventory_id = inventory_obj['inv_id']

    # Create the host first
    print(f'Creating host {host_name} in inventory {inventory_id} ({inventory_obj["name"]})')
    url = f'{API_URL}/inventories/{inventory_id}/hosts/'
    data = {'name': host_name, 'inventory': inventory_id, 'variables': variables}
    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)
    if resp.status_code in (200, 201, 202):
        print(f'Created host {host_name}: {resp.status_code}')
    else:
        print(f'Failed to create host {host_name}: {resp.status_code} - {resp.text}')

    host_id = resp.json()['id']

    # Update facts if provided
    if facts:
        print(f'Updating facts for host {host_name} (id: {host_id})')
        # convert facts to jsonb
        facts_json = json.dumps(facts).replace("'", "''")

        # build SQL with JSONB cast
        sql = f"UPDATE main_host SET ansible_facts = '{facts_json}'::jsonb WHERE id = {host_id};"
        run(sql)
        print(f'Updated facts for host {host_name}')

    # Update host count in inventory object
    if 'hosts_count' not in inventory_obj:
        inventory_obj['hosts_count'] = 1
    else:
        inventory_obj['hosts_count'] += 1

    print(f'Inventory {inventory_obj["name"]} now has {inventory_obj["hosts_count"]} hosts')

    return host_id


"""
awx=> select * from main_indirectmanagednodeaudit;
 id | created | name | canonical_facts | facts | events | count | host_id | inventory_id | job_id | organization_id

 events is jsonb array of events
"""


def create_indirect_host(host_name, inventory_id, job_id, organization_id, facts=None, canonical_facts=None, events=None):
    if not facts:
        facts = {}
    if not events:
        events = []

    sql = f"""
    INSERT INTO main_indirectmanagednodeaudit (
        name, created, inventory_id, job_id, organization_id,
        facts, canonical_facts, events, count
    )
    VALUES (
        '{host_name}', now(), {inventory_id}, {job_id}, {organization_id},
        '{json.dumps(facts)}', '{json.dumps(canonical_facts)}', '{json.dumps(events)}', 0
    );
    """
    run(sql)


def list_indirect_hosts():
    sql = """
    SELECT * FROM main_indirectmanagednodeaudit;
    """
    res = run(sql)
    print(res)


def delete_indirect_hosts():
    sql = """
    DELETE FROM main_indirectmanagednodeaudit;
    """
    run(sql)


def delete_events():
    sql = """
    DELETE FROM main_jobevent;
    """
    run(sql)


def main():
    if ENVIRONMENT == 'OpenShift':
        oc_login()
        if not os.getenv('POD_NAME') or not os.getenv('NAMESPACE'):
            create_oc_environs()

    list_main_jobhostsummary()

    delete_events()
    delete_all_hosts()
    delete_projects()
    delete_job_templates()
    delete_inventories()
    delete_main_jobhostsummary()
    delete_organizations()
    delete_indirect_hosts()

    print('remaining jobs')
    print(list_main_jobhostsummary())

    print('\n\n')

    res = []

    default_org_id = 1
    projectId = create_main_project(default_org_id)

    # create 2 new organizations
    org_id2 = create_organization('mockA_test_org2')
    org_id3 = create_organization('mockA_test_org3')

    projectId2 = create_main_project(org_id2)
    projectId3 = create_main_project(org_id3)

    # Create inventories and templates WITHOUT hosts
    print('Creating inventories and templates...')

    inv1 = create_inventory_and_template(f'{INV_PREFIX}_test1', projectId, default_org_id)
    inv2 = create_inventory_and_template(f'{INV_PREFIX}_test2', projectId, default_org_id)
    inv3 = create_inventory_and_template(f'{INV_PREFIX}_test3', projectId, default_org_id)
    inv4 = create_inventory_and_template(f'{INV_PREFIX}_test4', projectId2, org_id2)
    inv5 = create_inventory_and_template(f'{INV_PREFIX}_test5', projectId3, org_id3)

    # Now create hosts directly using create_host function
    print('Creating hosts...')

    create_host(inv1, 'host1', 'ansible_connection: local')
    create_host(inv1, 'host2_host1', 'ansible_connection: local\nansible_host: host1')

    create_host(inv2, 'host1', 'ansible_connection: local')

    # unreachable host
    create_host(inv3, 'host3', '')
    create_host(inv3, 'host4', 'ansible_connection: local')

    # should not join - different org
    create_host(inv4, 'host1', 'ansible_connection: local')

    # test dedup, hosts should not join
    create_host(inv5, 'host5', 'ansible_connection: local', {'ansible_machine_id': 'machine_id_1'})
    create_host(inv5, 'host6', 'ansible_connection: local', {'ansible_machine_id': 'machine_id_1'})

    create_host(inv5, 'host7', 'ansible_connection: local', {'ansible_product_serial': 'product_serial_1'})
    create_host(inv5, 'host8', 'ansible_connection: local', {'ansible_product_serial': 'product_serial_1'})

    # these should join
    create_host(inv5, 'host9', 'ansible_connection: local', {'ansible_machine_id': 'machine_id_1', 'ansible_product_serial': 'product_serial_1'})
    create_host(inv5, 'host10', 'ansible_connection: local', {'ansible_machine_id': 'machine_id_1', 'ansible_product_serial': 'product_serial_1'})

    create_host(inv4, 'host11', 'ansible_connection: local', {'ansible_machine_id': 'machine_id_1', 'ansible_product_serial': 'product_serial_1'})

    # Collect all inventories for later processing
    res = [inv1, inv2, inv3, inv4, inv5]

    jobs_count = 4

    hosts_count = []
    for r in res:
        hosts_count.append(r['hosts_count'])

    print(hosts_count)

    first_job_id = None
    first_inventory_id = None
    for r in res:
        for i in range(jobs_count):
            id = launch_job_template(r['job_template_id'])
            if first_job_id is None:
                first_job_id = id
            if first_inventory_id is None:
                first_inventory_id = r['inv_id']

    print('Waiting for job completion')

    total_job_hosts_summary_count = sum(hosts_count) * jobs_count
    while True:
        count = count_main_jobhostsummary()
        print(f'Current count: {count} of {total_job_hosts_summary_count}')
        if count >= total_job_hosts_summary_count:
            break
        time.sleep(10)

    dates = []
    basic_date = '2022-01-03 01:00:00'

    jobs_and_inv_count = jobs_count * len(res)

    for i in range(0, int(jobs_and_inv_count / 2)):
        dates.append(basic_date)
        basic_date = (datetime.datetime.strptime(basic_date, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=13)).strftime('%Y-%m-%d %H:%M:%S')

    basic_date = '2024-01-05 01:00:00'

    # make division integer
    for i in range(0, int(jobs_and_inv_count / 2) + 1):
        dates.append(basic_date)
        basic_date = (datetime.datetime.strptime(basic_date, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=13)).strftime('%Y-%m-%d %H:%M:%S')

    set_different_modified_dates(dates)
    list_main_jobhostsummary()

    create_indirect_host(
        'mockA_indirect1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {
            'device_type': 'Containers',
            'infra_type': 'Public Cloud',
            'infra_bucket': 'Storage',
            'ansible_machine_id': 'machine_id_1',
            'ansible_product_serial': 'product_serial_1',
        },
        {
            'ansible_host': 'mockA_indirect1.example.com',
            'ansible_distribution': 'RedHat',
            'ansible_machine_id': 'machine_id_1',
            'ansible_product_serial': 'product_serial_1',
        },
        ['containers.podman.info', 'kubernetes.core.k8s_info'],
    )
    create_indirect_host(
        'mockA_indirect2',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Object Storage', 'infra_type': 'Hybrid Cloud', 'infra_bucket': 'Database'},
        {
            'ansible_host': 'mockA_indirect2.example.com',
            'ansible_distribution': 'Ubuntu',
            'ansible_ec2_instance_id': 'i-1234567890abcdef2',
            'ansible_ec2_placement_region': 'us-west-2',
        },
        ['amazon.aws.ec2_info', 'amazon.aws.s3_bucket'],
    )
    create_indirect_host(
        'mockA_indirect3',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Container Platform', 'infra_type': 'kubernetes', 'infra_bucket': 'container'},
        {'ansible_host': 'mockA_indirect3.k8s.local', 'ansible_distribution': 'CentOS', 'ansible_machine_id': 'k8s_node_machine_id_1'},
        ['kubernetes.core.k8s_info', 'kubernetes.core.k8s_cluster_info'],
    )
    create_indirect_host(
        'mockA_indirect4',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Database', 'infra_type': 'vmware', 'infra_bucket': 'virtual'},
        {
            'ansible_host': 'mockA_indirect4.vmware.local',
            'ansible_distribution': 'Debian',
            'ansible_product_serial': 'VMware-56 4d 3a 2f 8c 9b 12 34',
        },
    )
    create_indirect_host(
        'mockA_indirect5',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Network Equipment', 'infra_type': 'On-Premise', 'infra_bucket': 'Network'},
        {'ansible_host': 'mockA_indirect5.network.local', 'ansible_distribution': 'Cisco', 'ansible_machine_id': 'cisco_device_serial_123'},
    )
    create_indirect_host(
        'mockA_indirect6',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Edge Computing', 'infra_type': 'Edge Cloud', 'infra_bucket': 'Edge'},
        {'ansible_host': 'mockA_indirect6.edge.local', 'ansible_distribution': 'Alpine', 'ansible_machine_id': 'edge_device_id_456'},
    )
    create_indirect_host(
        'mockA_indirect7',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Virtual Machines', 'infra_type': 'Private Cloud', 'infra_bucket': 'Compute'},
        {'ansible_host': 'mockA_indirect7.private.cloud', 'ansible_distribution': 'SUSE', 'ansible_product_serial': 'PRIVATE_CLOUD_VM_789'},
    )
    create_indirect_host(
        'mockA_indirect8',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Load Balancer', 'infra_type': 'Public Cloud', 'infra_bucket': 'Network'},
        {
            'ansible_host': 'mockA_indirect8.aws.amazon.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_instance_id': 'i-0987654321fedcba0',
            'ansible_ec2_placement_region': 'eu-west-1',
        },
        ['amazon.aws.elb_info', 'amazon.aws.ec2_elb_lb'],
    )
    create_indirect_host(
        'mockA_indirect9',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Monitoring', 'infra_type': 'Hybrid Cloud', 'infra_bucket': 'Observability'},
        {'ansible_host': 'mockA_indirect9.monitoring.local', 'ansible_distribution': 'RedHat', 'ansible_machine_id': 'monitoring_node_abc123'},
    )
    create_indirect_host(
        'host1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Physical Server', 'infra_type': 'On-Premise', 'infra_bucket': 'Compute'},
        {
            'ansible_host': 'host1.company.local',
            'ansible_distribution': 'RedHat',
            'ansible_machine_id': 'physical_server_host1',
            'ansible_product_serial': 'PHYS_SRV_001',
        },
    )
    create_indirect_host(
        'mockA_indirect10',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Database', 'infra_type': 'Public Cloud', 'infra_bucket': 'Database'},
        {
            'ansible_host': 'mockA_indirect10.rds.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_instance_id': 'i-db123456789abcdef',
            'ansible_ec2_placement_region': 'us-east-1',
        },
        ['amazon.aws.rds_info', 'amazon.aws.rds_instance'],
    )
    create_indirect_host(
        'mockA_indirect11',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Container Platform', 'infra_type': 'Public Cloud', 'infra_bucket': 'container'},
        {
            'ansible_host': 'mockA_indirect11.eks.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_cluster_name': 'production-cluster',
            'ansible_ec2_placement_region': 'us-west-1',
        },
        ['kubernetes.core.k8s_info', 'amazon.aws.eks_cluster'],
    )
    create_indirect_host(
        'mockA_indirect12',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Virtual Machines', 'infra_type': 'vmware', 'infra_bucket': 'virtual'},
        {
            'ansible_host': 'mockA_indirect12.vcenter.local',
            'ansible_distribution': 'Ubuntu',
            'ansible_product_serial': 'VMware-56 4d 3a 2f 8c 9b 56 78',
            'ansible_machine_id': 'vmware_vm_567890',
        },
        ['community.vmware.vmware_vm_info'],
    )
    create_indirect_host(
        'mockA_indirect13',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Object Storage', 'infra_type': 'Public Cloud', 'infra_bucket': 'Storage'},
        {
            'ansible_host': 'mockA_indirect13.s3.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_placement_region': 'ap-southeast-1',
        },
        ['amazon.aws.s3_bucket', 'amazon.aws.s3_object'],
    )
    create_indirect_host(
        'mockA_indirect14',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Monitoring', 'infra_type': 'On-Premise', 'infra_bucket': 'Observability'},
        {
            'ansible_host': 'mockA_indirect14.monitoring.datacenter.local',
            'ansible_distribution': 'CentOS',
            'ansible_machine_id': 'monitoring_server_789xyz',
            'ansible_product_serial': 'MONITOR_SRV_002',
        },
        ['ansible.builtin.systemd', 'community.general.grafana_dashboard'],
    )

    # Additional hosts to create multiple unique nodes in same infrastructure categories

    # More Public Cloud + Storage + Containers hosts (currently 1, adding 2 more = 3 unique)
    create_indirect_host(
        'mockA_container_node1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {
            'device_type': 'Containers',
            'infra_type': 'Public Cloud',
            'infra_bucket': 'Storage',
            'ansible_machine_id': 'container_node_001',
        },
        {
            'ansible_host': 'container-node1.aws.example.com',
            'ansible_distribution': 'Amazon',
            'ansible_machine_id': 'container_node_001',
        },
        ['containers.podman.info'],
    )
    create_indirect_host(
        'mockA_container_node2',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {
            'device_type': 'Containers',
            'infra_type': 'Public Cloud',
            'infra_bucket': 'Storage',
            'ansible_machine_id': 'container_node_002',
        },
        {
            'ansible_host': 'container-node2.gcp.example.com',
            'ansible_distribution': 'Ubuntu',
            'ansible_machine_id': 'container_node_002',
        },
        ['containers.podman.info', 'kubernetes.core.k8s_info'],
    )

    # More Public Cloud + Database + Database hosts (currently 1, adding 2 more = 3 unique)
    create_indirect_host(
        'mockA_database_cluster1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Database', 'infra_type': 'Public Cloud', 'infra_bucket': 'Database'},
        {
            'ansible_host': 'db-cluster1.rds.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_instance_id': 'i-dbcluster123456789',
            'ansible_ec2_placement_region': 'us-west-2',
        },
        ['amazon.aws.rds_info', 'amazon.aws.rds_cluster'],
    )
    create_indirect_host(
        'mockA_database_replica1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Database', 'infra_type': 'Public Cloud', 'infra_bucket': 'Database'},
        {
            'ansible_host': 'db-replica1.cloudsql.gcp.com',
            'ansible_distribution': 'Ubuntu',
            'ansible_ec2_instance_id': 'i-dbreplica987654321',
            'ansible_ec2_placement_region': 'us-central1',
        },
        ['google.cloud.gcp_sql_instance'],
    )

    # More VMware + virtual + Virtual Machines hosts (currently 1, adding 3 more = 4 unique)
    create_indirect_host(
        'mockA_vm_web1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Virtual Machines', 'infra_type': 'vmware', 'infra_bucket': 'virtual'},
        {
            'ansible_host': 'vm-web1.vcenter.datacenter.local',
            'ansible_distribution': 'RedHat',
            'ansible_product_serial': 'VMware-42 1a 5b 3c 7d 8e 9f 01',
            'ansible_machine_id': 'vmware_web_vm_001',
        },
        ['community.vmware.vmware_vm_info'],
    )
    create_indirect_host(
        'mockA_vm_app1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Virtual Machines', 'infra_type': 'vmware', 'infra_bucket': 'virtual'},
        {
            'ansible_host': 'vm-app1.vcenter.datacenter.local',
            'ansible_distribution': 'CentOS',
            'ansible_product_serial': 'VMware-99 8a 7b 6c 5d 4e 3f 02',
            'ansible_machine_id': 'vmware_app_vm_002',
        },
        ['community.vmware.vmware_vm_info'],
    )
    create_indirect_host(
        'mockA_vm_db1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Virtual Machines', 'infra_type': 'vmware', 'infra_bucket': 'virtual'},
        {
            'ansible_host': 'vm-db1.vcenter.datacenter.local',
            'ansible_distribution': 'SUSE',
            'ansible_product_serial': 'VMware-33 2b 1c 9d 8e 7f 6a 03',
            'ansible_machine_id': 'vmware_db_vm_003',
        },
        ['community.vmware.vmware_vm_info'],
    )

    # More On-Premise + Compute + Physical Server hosts (currently 1, adding 2 more = 3 unique)
    create_indirect_host(
        'mockA_physical_server1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Physical Server', 'infra_type': 'On-Premise', 'infra_bucket': 'Compute'},
        {
            'ansible_host': 'server1.datacenter.company.local',
            'ansible_distribution': 'CentOS',
            'ansible_machine_id': 'physical_server_dc001',
            'ansible_product_serial': 'PHYS_SRV_DC001',
        },
    )
    create_indirect_host(
        'mockA_physical_server2',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Physical Server', 'infra_type': 'On-Premise', 'infra_bucket': 'Compute'},
        {
            'ansible_host': 'server2.datacenter.company.local',
            'ansible_distribution': 'Ubuntu',
            'ansible_machine_id': 'physical_server_dc002',
            'ansible_product_serial': 'PHYS_SRV_DC002',
        },
    )

    # More Public Cloud + container + Container Platform hosts (currently 1, adding 2 more = 3 unique)
    create_indirect_host(
        'mockA_k8s_worker1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Container Platform', 'infra_type': 'Public Cloud', 'infra_bucket': 'container'},
        {
            'ansible_host': 'k8s-worker1.eks.us-east-1.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_cluster_name': 'production-cluster-east',
            'ansible_ec2_placement_region': 'us-east-1',
        },
        ['kubernetes.core.k8s_info', 'amazon.aws.eks_nodegroup'],
    )
    create_indirect_host(
        'mockA_k8s_worker2',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Container Platform', 'infra_type': 'Public Cloud', 'infra_bucket': 'container'},
        {
            'ansible_host': 'k8s-worker2.gke.us-central1.gcp.com',
            'ansible_distribution': 'Ubuntu',
            'ansible_ec2_cluster_name': 'staging-cluster-central',
            'ansible_ec2_placement_region': 'us-central1',
        },
        ['kubernetes.core.k8s_info', 'google.cloud.gcp_container_cluster'],
    )

    # More Public Cloud + Network + Load Balancer hosts (currently 1, adding 2 more = 3 unique)
    create_indirect_host(
        'mockA_alb_frontend',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Load Balancer', 'infra_type': 'Public Cloud', 'infra_bucket': 'Network'},
        {
            'ansible_host': 'alb-frontend.elb.us-east-1.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_instance_id': 'i-alb123456789frontend',
            'ansible_ec2_placement_region': 'us-east-1',
        },
        ['amazon.aws.elb_application_lb', 'amazon.aws.elb_target_group'],
    )
    create_indirect_host(
        'mockA_nlb_backend',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Load Balancer', 'infra_type': 'Public Cloud', 'infra_bucket': 'Network'},
        {
            'ansible_host': 'nlb-backend.elb.us-west-2.amazonaws.com',
            'ansible_distribution': 'Amazon',
            'ansible_ec2_instance_id': 'i-nlb987654321backend',
            'ansible_ec2_placement_region': 'us-west-2',
        },
        ['amazon.aws.elb_network_lb'],
    )

    # More On-Premise + Observability + Monitoring hosts (currently 1, adding 2 more = 3 unique)
    create_indirect_host(
        'mockA_monitoring_prometheus',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Monitoring', 'infra_type': 'On-Premise', 'infra_bucket': 'Observability'},
        {
            'ansible_host': 'prometheus.monitoring.datacenter.local',
            'ansible_distribution': 'Ubuntu',
            'ansible_machine_id': 'prometheus_server_001',
            'ansible_product_serial': 'PROMETHEUS_SRV_001',
        },
        ['community.general.prometheus'],
    )
    create_indirect_host(
        'mockA_monitoring_grafana',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Monitoring', 'infra_type': 'On-Premise', 'infra_bucket': 'Observability'},
        {
            'ansible_host': 'grafana.monitoring.datacenter.local',
            'ansible_distribution': 'RedHat',
            'ansible_machine_id': 'grafana_server_001',
            'ansible_product_serial': 'GRAFANA_SRV_001',
        },
        ['community.general.grafana_dashboard', 'community.general.grafana_datasource'],
    )

    # Add some brand new infrastructure categories with multiple hosts

    # Edge Cloud + Edge + IoT Devices (3 unique nodes)
    create_indirect_host(
        'mockA_iot_sensor1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'IoT Devices', 'infra_type': 'Edge Cloud', 'infra_bucket': 'Edge'},
        {
            'ansible_host': 'iot-sensor1.edge.factory.local',
            'ansible_distribution': 'Alpine',
            'ansible_machine_id': 'iot_sensor_001',
        },
    )
    create_indirect_host(
        'mockA_iot_gateway1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'IoT Devices', 'infra_type': 'Edge Cloud', 'infra_bucket': 'Edge'},
        {
            'ansible_host': 'iot-gateway1.edge.factory.local',
            'ansible_distribution': 'Ubuntu',
            'ansible_machine_id': 'iot_gateway_001',
        },
    )
    create_indirect_host(
        'mockA_iot_controller1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'IoT Devices', 'infra_type': 'Edge Cloud', 'infra_bucket': 'Edge'},
        {
            'ansible_host': 'iot-controller1.edge.factory.local',
            'ansible_distribution': 'CentOS',
            'ansible_machine_id': 'iot_controller_001',
        },
    )

    # Hybrid Cloud + Security + Security Appliances (4 unique nodes)
    create_indirect_host(
        'mockA_firewall1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Security Appliances', 'infra_type': 'Hybrid Cloud', 'infra_bucket': 'Security'},
        {
            'ansible_host': 'firewall1.security.hybrid.local',
            'ansible_distribution': 'FortiOS',
            'ansible_machine_id': 'firewall_001',
        },
    )
    create_indirect_host(
        'mockA_ids1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Security Appliances', 'infra_type': 'Hybrid Cloud', 'infra_bucket': 'Security'},
        {
            'ansible_host': 'ids1.security.hybrid.local',
            'ansible_distribution': 'Suricata',
            'ansible_machine_id': 'ids_001',
        },
    )
    create_indirect_host(
        'mockA_waf1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Security Appliances', 'infra_type': 'Hybrid Cloud', 'infra_bucket': 'Security'},
        {
            'ansible_host': 'waf1.security.hybrid.local',
            'ansible_distribution': 'ModSecurity',
            'ansible_machine_id': 'waf_001',
        },
    )
    create_indirect_host(
        'mockA_proxy1',
        first_inventory_id,
        first_job_id,
        default_org_id,
        {'device_type': 'Security Appliances', 'infra_type': 'Hybrid Cloud', 'infra_bucket': 'Security'},
        {
            'ansible_host': 'proxy1.security.hybrid.local',
            'ansible_distribution': 'Squid',
            'ansible_machine_id': 'proxy_001',
        },
    )

    list_indirect_hosts()


if __name__ == '__main__':
    main()
