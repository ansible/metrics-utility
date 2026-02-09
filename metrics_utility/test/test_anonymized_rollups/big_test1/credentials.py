# Credentials dataset for big_test1
# Format: {'credential_type': str, 'job_id': int, 'model': str}
# 
# Credential types:
# - Machine
# - Vault
# - Source Control
# - Network
# - Amazon Web Services
# - Container Registry
#
# Distribution across jobs 1-8:
# - Job 1: Machine, Amazon Web Services
# - Job 2: Machine, Vault
# - Job 3: Machine, Network
# - Job 4: Machine, Source Control
# - Job 5: Machine, Amazon Web Services
# - Job 6: Machine, Container Registry
# - Job 7: Machine, Amazon Web Services (workflowjob)
# - Job 8: Machine, Vault

credentials = [
    # Job 1 credentials
    {'credential_type': 'Machine', 'job_id': 1, 'model': 'job'},
    {'credential_type': 'Amazon Web Services', 'job_id': 1, 'model': 'job'},
    
    # Job 2 credentials
    {'credential_type': 'Machine', 'job_id': 2, 'model': 'job'},
    {'credential_type': 'Vault', 'job_id': 2, 'model': 'job'},
    
    # Job 3 credentials
    {'credential_type': 'Machine', 'job_id': 3, 'model': 'job'},
    {'credential_type': 'Network', 'job_id': 3, 'model': 'job'},
    
    # Job 4 credentials
    {'credential_type': 'Machine', 'job_id': 4, 'model': 'job'},
    {'credential_type': 'Source Control', 'job_id': 4, 'model': 'job'},
    
    # Job 5 credentials
    {'credential_type': 'Machine', 'job_id': 5, 'model': 'job'},
    {'credential_type': 'Amazon Web Services', 'job_id': 5, 'model': 'job'},
    
    # Job 6 credentials
    {'credential_type': 'Machine', 'job_id': 6, 'model': 'job'},
    {'credential_type': 'Container Registry', 'job_id': 6, 'model': 'job'},
    
    # Job 7 credentials (workflowjob)
    {'credential_type': 'Machine', 'job_id': 7, 'model': 'workflowjob'},
    {'credential_type': 'Amazon Web Services', 'job_id': 7, 'model': 'workflowjob'},
    
    # Job 8 credentials
    {'credential_type': 'Machine', 'job_id': 8, 'model': 'job'},
    {'credential_type': 'Vault', 'job_id': 8, 'model': 'job'},
]

# Expected unique credential types (all jobs combined):
# - Amazon Web Services (jobs 1, 5, 7)
# - Container Registry (job 6)
# - Machine (all jobs)
# - Network (job 3)
# - Source Control (job 4)
# - Vault (jobs 2, 8)
