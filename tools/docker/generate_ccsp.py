import subprocess


with open('tools/docker/main_jobhostsummary.sql', 'r') as file:
    original_script = file.read()


def run(sql_script):
    command = [
        'docker', 'exec', '-i', 'postgres',
        'psql', '-U', 'awx'
    ]

    process = subprocess.run(command, input=sql_script.encode(), capture_output=True)

    print(process.stdout.decode())
    print(process.stderr.decode())


def replace_date(date):
    return original_script.replace('2025-06-13', date)

def replace_and_run(date):
    sql_script = replace_date(date)
    print(sql_script)
    run(sql_script)

# 2025
replace_and_run('2025-06-13')
replace_and_run('2025-06-03')

# 2022
replace_and_run('2022-07-13')
replace_and_run('2022-05-13')
replace_and_run('2022-03-13')




