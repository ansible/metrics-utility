# Running against awx dev env - within the awx docker compose virtualenv

### setup

```bash
git clone https://github.com/ansible/awx
cd awx

make docker-compose
```


### install deps

```bash
docker exec -it tools_awx_1 pip install pandas==2.2.3 openpyxl==3.1.2
```

and then *either* install metrics-utility from pip, or from source


### metrics-utility from source

checkout metrics-utility *inside* `awx/`:

```bash
cd awx
git clone https://github.com/ansible/metrics-utility
```

then run as `./manage.py` inside the container dir

```bash
docker exec -it tools_awx_1 /bin/bash
    # inside the container
    cd metrics-utility
    ./manage.py --help
```


### metrics-utility from pip

May fail with dependency conflicts. If so, the from source variant should still work.

```bash
docker exec -it tools_awx_1 pip install metrics-utility
```

then

```bash
docker exec -it tools_awx_1 metrics-utility --version
```


### access awx

```bash
open https://localhost:8043/api/v2/
open https://localhost:8043/api/docs/
```


### update

```bash
cd awx
git pull --ff-only origin devel
docker compose -f tools/docker-compose/_sources/docker-compose.yml down -v
```


### psql

```bash
docker exec tools_postgres_1 psql -n awx -c 'select app, max(name) from django_migrations group by app order by app;' --csv
```


### extract schema

```bash
cd metrics-utility/tools/docker
docker exec tools_postgres_1 pg_dump -s awx > latest.sql
```


### misc

* `/usr/bin/pip3.11 install --user ...` -> `/var/lib/awx/.local/bin`
* pytest might not run without `SETUPTOOLS_USE_DISTUTILS=true`
* root - `docker exec -u0`
* unnecessary - `. /var/lib/awx/venv/awx/bin/activate`
