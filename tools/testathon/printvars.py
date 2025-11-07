# print env vars used in testathon_data_prepare.py that are set in terminal

import os


env_vars = ['INV_PREFIX', 'API_URL', 'USERNAME', 'PASSWORD', 'SSH_URL', 'SSH_USER', 'ENVIRONMENT', 'POD_NAME', 'NAMESPACE', 'OC_LOGIN_COMMAND']

for var in env_vars:
    value = os.getenv(var)
    if value:
        print(f'{var} = {value!r}')
    else:
        print(f'{var} = None')
