# docker compose environment & mock awx data

This provides the docker compose environment,
and also the database schemas, used both by compose & github CI.

When loading the `.sql` files, `roles.sql` needs to come first, then `latest.sql` (schema),
then the rest of the files can go in any order.


## awx

`make compose-awx` runs a real AWX against the compose postgres, from an awx checkout in `../awx` (next to metrics-utility).

* image - [awx/Containerfile](./awx/Containerfile), only system packages (incl. podman & receptor)
* everything else is mounted - the awx checkout, [awx/bootstrap.sh](./awx/bootstrap.sh) (python deps, migrate, admin user, instance registration), [awx/supervisord.conf](./awx/supervisord.conf), and the awx config files in [awx/](./awx/)
* awx python dependencies live in the `awx_venv` volume, reinstalled on start whenever `../awx/requirements/` changed (first start takes a few minutes)
  * git-based requirements (`@devel` branches) don't get refreshed unless a requirements file changes too - `podman rm -f awx && podman volume rm docker_awx_venv` to force a reinstall
* https://localhost:8043/, `admin:admin` (reset on every start)
* jobs run in execution environments via podman inside the container, pulled images are kept in the `awx_containers` volume
* awx processes restart automatically when `.py` files in `../awx/awx/` change ([awx/autoreload.sh](./awx/autoreload.sh))
