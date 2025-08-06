import datetime
import os
import subprocess
import time

import requests

from helper_ocp_prepare import create_oc_environs


# Configuration

# get api url from env, if not provided, use default
API_URL = os.getenv('API_URL', 'https://localhost:8030/api/controller/v2')
USERNAME = os.getenv('USERNAME', 'admin')
PASSWORD = os.getenv('PASSWORD', 'admin')

SSH_URL = os.getenv('SSH_URL', None)
SSH_USER = os.getenv('SSH_USER', 'ec2-user')

OC_LOGIN_COMMAND = os.getenv('OC_LOGIN_COMMAND', '')

ENVIRONMENT = os.getenv('ENVIRONMENT', 'local')

print(f'API_URL: {API_URL}')
print(f'USERNAME: {USERNAME}')
print(f'PASSWORD: {PASSWORD}')
print(f'SSH_URL: {SSH_URL}')
print(f'SSH_USER: {SSH_USER}')
print(f'ENVIRONMENT: {ENVIRONMENT}')
print(f'OC_LOGIN_COMMAND: {OC_LOGIN_COMMAND}')

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
            # Send SQL script over SSH and pipe it into `sudo awx-manage dbshell`
            remote_command = f'echo "{sql_script}" | sudo awx-manage dbshell'
            ssh_command = ['ssh', f'{SSH_USER}@{SSH_URL}', remote_command]
            process = subprocess.run(ssh_command, capture_output=True)
            stderr = process.stderr.decode()
            stdout = process.stdout.decode()

        import base64

        if ENVIRONMENT == 'containerized':
            # base64-encode the whole SQL
            b64 = base64.b64encode(sql_script.encode('utf-8')).decode('ascii')
            remote_command = f'echo {b64} | base64 --decode | podman exec -i automation-controller-web awx-manage dbshell'
            print('remote command:', remote_command)
            process = subprocess.run(['ssh', f'{SSH_USER}@{SSH_URL}', remote_command], capture_output=True, text=True)
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


def delete_main_project():
    # delete mock project

    url = f'{API_URL}/projects/?name=MockA_Test_Project'
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


def create_main_project():
    # https://localhost:8030/api/controller/v2/projects/
    url = f'{API_URL}/projects/'
    data = {
        'name': 'MockA_Test_Project',
        'organization': 1,
        'scm_type': 'git',
        'scm_url': 'https://github.com/ansible/ansible-tower-samples',
    }
    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)

    if resp.status_code not in (200, 201, 202):
        print(f'Failed to create project {data["name"]}: {resp.status_code} - {resp.text}')

    print(f'Created project {data["name"]}: {resp.status_code}')

    print('Waiting for sync')

    # repeatedly get the project and check field status: "successful"
    project_id = resp.json()['id']
    status = None
    for _ in range(60):  # Try for up to 60 seconds
        proj_resp = requests.get(f'{API_URL}/projects/{project_id}/', auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
        if proj_resp.status_code not in (200, 201, 202):
            print(f'Failed to get project status: {proj_resp.status_code} - {proj_resp.text}')
            time.sleep(1)
            continue
        status = proj_resp.json().get('status')
        print(f'Project sync status: {status}')
        if status == 'successful':
            break
        time.sleep(1)
    else:
        print(f'Project {project_id} did not reach "successful" status after waiting.')

    return resp.json()['id']


def create_job_template(name, inventory_id, project_id):
    # https://localhost:8030/api/controller/v2/job_templates/
    # create with only name, inventory, project, playbook
    url = f'{API_URL}/job_templates/'

    data = {'name': name, 'inventory': inventory_id, 'project': project_id, 'playbook': 'hello_world.yml'}

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
    url = f'{API_URL}/inventories/?search=mockA_test'
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


def delete_inventory(inv_id):
    """
    Delete a single inventory by ID.
    """
    url = f'{API_URL}/inventories/{inv_id}/'
    resp = requests.delete(url, auth=(USERNAME, PASSWORD), verify=VERIFY_SSL)
    if resp.status_code in (200, 202, 204):
        print(f'Deleted inventory {inv_id}: {resp.status_code}')
    else:
        print(f'Failed to delete inventory {inv_id}: {resp.status_code} - {resp.text}')


def delete_inventories():
    # get all inventories and delete each
    all_invs = get_all_inventories()
    print(f'Found {len(all_invs)} inventories.')

    # Delete each inventory
    for inv in all_invs:
        # Inventory object may include 'id' key
        inv_id = inv.get('id') if isinstance(inv, dict) else inv
        delete_inventory(inv_id)


def create_inventory(name):
    url = f'{API_URL}/inventories/'
    data = {'name': name, 'organization': 1, 'prevent_instance_group_fallback': False, 'variables': ''}
    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)
    if resp.status_code in (200, 201, 202):
        print(f'Created inventory {name}: {resp.status_code}')
    else:
        print(f'Failed to create inventory {name}: {resp.status_code} - {resp.text}')

    return resp.json()['id']


def create_host(inventory_id, name, variables):
    url = f'{API_URL}/inventories/{inventory_id}/hosts/'
    data = {'name': name, 'inventory': inventory_id, 'variables': variables}
    resp = requests.post(url, auth=(USERNAME, PASSWORD), json=data, verify=VERIFY_SSL)
    if resp.status_code in (200, 201, 202):
        print(f'Created host {name}: {resp.status_code}')
    else:
        print(f'Failed to create host {name}: {resp.status_code} - {resp.text}')

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


# make data optional, if not provided, use default values


def create_inventory_and_template(name, hosts_count, project_id, data={}):
    variables = data.get('variables', '')

    inv_id = create_inventory(name)
    job_template_id = create_job_template(name, inv_id, project_id)
    create_inventory_hosts(inv_id, f'{name}_host', hosts_count, variables, data)

    # return {}
    return {'inv_id': inv_id, 'job_template_id': job_template_id, 'hosts_count': hosts_count}


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

    job_id_to_index = {}
    for i, job_id in enumerate(job_ids):
        job_id_to_index[job_id] = i

    # do bulk update, concatenate all sql_update into one string
    sql_update = ''
    for row in data:
        job_id = int(row['job_id'])
        modified_date = dates[job_id_to_index[job_id]]
        sql_update += f"""
        UPDATE main_jobhostsummary SET modified = '{modified_date}' WHERE id = {int(row['id'])};
        """
        sql_update += f"""
        UPDATE main_jobhostsummary SET created = '{modified_date}' WHERE id = {int(row['id'])};
        """
    print(sql_update)
    run(sql_update)


def oc_login():
    # subprocess run
    subprocess.run(OC_LOGIN_COMMAND, shell=True)


def main():
    if ENVIRONMENT == 'OpenShift':
        oc_login()
        if not os.getenv('POD_NAME') or not os.getenv('NAMESPACE'):
            create_oc_environs()

    list_main_jobhostsummary()

    delete_main_project()

    delete_job_templates()
    delete_inventories()
    delete_main_jobhostsummary()
    print('remaining jobs')
    print(list_main_jobhostsummary())

    print('\n\n')
    res = []

    projectId = create_main_project()

    res.append(create_inventory_and_template('mockA_test1', 2, projectId, {'variables': 'ansible_connection: local'}))
    res.append(create_inventory_and_template('mockA_test2', 3, projectId, {'variables': 'ansible_connection: local'}))

    # unreachable host
    res.append(create_inventory_and_template('mockA_test3', 1, projectId))

    # some shared host names
    res.append(create_inventory_and_template('mockA_test4', 2, projectId, {'host_names': ['mockA_test1_host_1', 'mockA_test2_host_1']}))
    res.append(create_inventory_and_template('mockA_test5', 2, projectId, {'host_names': ['mockA_test_localhost', 'mockA_test2_host_1']}))
    res.append(create_inventory_and_template('mockA_test6', 2, projectId, {'host_names': ['mockA_test_localhost', 'mockA_test3_host_1']}))

    jobs_count = 2

    hosts_count = []
    for r in res:
        hosts_count.append(r['hosts_count'])

    print(hosts_count)

    for r in res:
        for i in range(jobs_count):
            launch_job_template(r['job_template_id'])

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

    for i in range(0, int(total_job_hosts_summary_count / 2)):
        dates.append(basic_date)
        basic_date = (datetime.datetime.strptime(basic_date, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=13)).strftime('%Y-%m-%d %H:%M:%S')

    basic_date = '2024-01-05 01:00:00'

    # make division integer
    for i in range(0, int(total_job_hosts_summary_count / 2) + 1):
        dates.append(basic_date)
        basic_date = (datetime.datetime.strptime(basic_date, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(days=13)).strftime('%Y-%m-%d %H:%M:%S')

    set_different_modified_dates(dates)
    list_main_jobhostsummary()


if __name__ == '__main__':
    main()
