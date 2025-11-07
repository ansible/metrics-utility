import subprocess


# This script is used to generate sql scripts that will be used to insert data into the database.
# it should not be used in testathon, but rather for some performance testing.


def run(sql_script):
    command = ['docker', 'exec', '-i', 'postgres', 'psql', '-U', 'awx']

    process = subprocess.run(command, input=sql_script.encode(), capture_output=True)

    print(process.stdout.decode())
    print(process.stderr.decode())


def replace_date(script, date):
    return script.replace('2025-06-13', date)


def generate_ccsp():
    with open('tools/docker/main_jobhostsummary.sql', 'r') as file:
        original_script = file.read()

    def replace_host_count(script, host_count):
        return script.replace('host_count INTEGER := 2;', f'host_count INTEGER := {host_count};')

    def replace_job_count(script, job_count):
        return script.replace('job_count INTEGER := 3;', f'job_count INTEGER := {job_count};')

    def replace_and_run(date, host_count, job_count, task_count):
        sql_script = replace_date(original_script, date)
        sql_script = replace_host_count(sql_script, host_count)
        sql_script = replace_job_count(sql_script, job_count)

        sql_script = sql_script.replace('0,-- ok', f'{task_count},-- ok')

        print(sql_script)
        run(sql_script)

    # 2025
    replace_and_run('2025-06-03', 1, 2, 0)

    # 2022
    replace_and_run('2022-07-13', 2, 1, 3)
    replace_and_run('2022-05-13', 3, 3, 2)
    replace_and_run('2022-03-13', 4, 5, 4)

    run('select * from main_jobhostsummary;')


generate_ccsp()


def generate_renewal_guidance():
    with open('tools/docker/main_hostmetric.sql', 'r') as file:
        original_script = file.read()

    sql_script = replace_date(original_script, '2025-07-09')
    run(sql_script)


generate_renewal_guidance()
