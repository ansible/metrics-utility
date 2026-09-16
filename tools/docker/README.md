# docker compose environment & mock awx data

This provides the docker compose environment,
and also the database schemas, used both by compose & github CI.

When loading the `.sql` files, `roles.sql` needs to come first, then `latest.sql` (schema),
then the rest of the files can go in any order.


## awx

`make compose-awx` runs a real AWX against the compose postgres, from an awx checkout in `../awx` (next to metrics-utility).

* image - [awx/Containerfile](./awx/Containerfile), only system packages & awx python dependencies, built from `../awx/requirements/`
* everything else is mounted - the awx checkout, [awx/bootstrap.sh](./awx/bootstrap.sh) (migrate, admin user, instance registration), [awx/supervisord.conf](./awx/supervisord.conf), and the awx config files in [awx/](./awx/)
* https://localhost:8043/, `admin:admin` (reset on every start)
* jobs run in execution environments via podman inside the container, pulled images are kept in the `awx_containers` volume
* after updating the awx checkout, rebuild when requirements changed - `podman-compose -f tools/docker/docker-compose.yaml --profile awx build awx`
* awx processes restart automatically when `.py` files in `../awx/awx/` change ([awx/autoreload.sh](./awx/autoreload.sh))
