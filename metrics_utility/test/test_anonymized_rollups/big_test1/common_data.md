Ansible analysis

# Templates
## T1
scm type: git
job type: run
playbook: playbook1.yml

### list of tasks and their modules
- ansible.builtin.copy
- ansible.builtin.file
- ansible.builtin.yum

# Hosts
Host1
Host2
Host3
Host4

# Organizations
Organization1


# How to create jobs

Simulate playbook task runs on all hosts

if some task on host failed, it can be rerun 1-3 times, unless its
unreachable (dark), those are not retried

runs:
ok - task succeded on host
failed - task failed (can be rerun)
dark - task unreachable (can be reruned)

the final outcome - you can see it inside:

metrics-utility/metrics_utility/anonymized_rollups/jobhostsummary_anonymized_rollup.py

Test validation:
uv run pytest -s {name}

How to create new job data:

1) Get inspiration from existing jobs like job1.py

2) Pick job template from this document

3) Select hosts that you should run at from this document

4) Write first comment at the begining of the file, comment will have tasks runs on hosts with possible reruns

5) Than you create job data

6) Than you can create test for this job

7) Validate the test by:
uv run pytest -s {file name}


