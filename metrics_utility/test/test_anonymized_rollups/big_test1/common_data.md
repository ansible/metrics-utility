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


