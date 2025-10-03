Scripts to download a list of collections from Galaxy & Hub.

`./galaxy-download.sh` - downloads a list of community collections from galaxy.ansible.com
`./hub-download.sh` - downloads a list of certified & validated from Ansible Automation Hub
    needs `CLIENT_ID` & `CLIENT_SECRET` env vars, from a c.r.c service account

`./process.sh` - creates a `./collections.json`

`./cleanup.sh` - cleans up

---

Using together:

```
cd metrics-utility/

tools/collections/galaxy-download.sh
CLIENT_ID=123 CLIENT_SECRET=456 tools/collections/hub-download.sh

tools/collections/process.sh
mv tools/collections/collections.json metrics_utility/anonymized_rollups/collections.json

tools/collections/cleanup.sh
```
