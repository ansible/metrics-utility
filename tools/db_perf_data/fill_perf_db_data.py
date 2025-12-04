from helpers import run, delete_all

def fill_perf_db_data():
    job_count = 100
    delete_all()

    for i in range(job_count):
        fill_job()

    return

def fill_job_data():
    return

def fill_jobhostsummary():
    return

def fill_jobevent():
    return

def fill_job():
    fill_job_data()
    fill_jobhostsummary()
    fill_jobevent()
    return

fill_perf_db_data()
