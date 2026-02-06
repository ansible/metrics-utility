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

## T2
scm type: git
job type: run
playbook: playbook2.yml

### list of tasks and their modules
- ansible.builtin.copy
- community.general.git
- community.general.archive
- community.weird.git

## T3
scm type: manual
job type: workflow
playbook: playbook2.yml

### list of tasks and their modules
- ansible.builtin.copy
- community.general.git
- community.general.archive
- community.weird.git

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
ok - task succeeded on host
failed - task failed (can be rerun)
dark - task unreachable (not retried)

the final outcome - you can see it inside:

metrics-utility/metrics_utility/anonymized_rollups/jobhostsummary_anonymized_rollup.py

Test validation:
uv run pytest -s {name}

How to create new job data:

1) Get inspiration from existing jobs like job1.py

2) Pick random job template from this document, determine its launch type randomly, determine randomly its version - but everything must fit how ansible works

3) Select hosts that you should run at from this document

4) Write first comment at the begining of the file, comment will have tasks runs on hosts with possible reruns

5) Than you create job data

6) Than you can create test for this job

7) Validate the test by:
uv run pytest -s {file name}

8) Validate if tests input data and output report match by how ansible works - if jobs, job host summaries, events fit together - if reports match of whats in the input data

9) Let me know if you find any logical problem between real test run (the real output data) and how ansible should work


