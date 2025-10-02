Scripts to download a list of collections from Galaxy & Hub.

`./galaxy-download.sh` - downloads a list of community collections from galaxy.ansible.com
`./hub-download.sh` - downloads a list of certified & validated from Ansible Automation Hub
    needs `CLIENT_ID` & `CLIENT_SECRET` env vars, from a c.r.c service account

`./process.sh` - creates a `./collections.json` (FIXME: move to where `collections_types.py` expects it)

`./cleanup.sh` - cleans up
