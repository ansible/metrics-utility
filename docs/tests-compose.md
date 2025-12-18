# Running tests inside the docker compose environment

You can also run pytest inside a container too - to run all tests once, you can `docker compose -f tools/docker/docker-compose.yaml --profile=pytest up`. You use also `podman`.

For more flexibility, use:

```
(host) $ docker compose -f tools/docker/docker-compose.yaml --profile=env up -d  # runs a metrics-utility-env container with python & uv set up
(host) $ docker exec -it metrics-utility-env /bin/sh # (wait for postgres & minio containers to start before running)
(container) $ uv run pytest -vv metrics_utility/test/ccspv_reports/test_complex_CCSP_with_scope.py # 1 test
(container) $ uv run pytest -vv metrics_utility/test/ccspv_reports # all ccsp tests
```

#### Using Docker (in CI mode to be able to run all tests)

```bash
# Ensure SQL data is loaded (only needed once after starting containers)
docker compose -f tools/docker/docker-compose.yaml exec postgres bash -c \
  'cat /docker-entrypoint-initdb.d/init-*.sql | psql -U awx -d postgres'

# Run all gather tests
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/'

# Run a specific gather test
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/test_jobhostsummary_gather.py::test_command'

# Run all tests (not just gather)
docker compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v'
```

#### Using Podman (in CI mode to be able to run all tests)

```bash
# Ensure SQL data is loaded (only needed once after starting containers)
podman compose -f tools/docker/docker-compose.yaml exec postgres bash -c \
  'cat /docker-entrypoint-initdb.d/init-*.sql | psql -U awx -d postgres'

# Run all gather tests
podman compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/'

# Run a specific gather test
podman compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v metrics_utility/test/gather/test_jobhostsummary_gather.py::test_command'

# Run all tests (not just gather)
podman compose -f tools/docker/docker-compose.yaml exec metrics-utility-env bash -c \
  'sed -i "/NAME/s/awx/postgres/" mock_awx/settings/__init__.py && \
   sed -i "/USER/s/myuser/awx/" mock_awx/settings/__init__.py && \
   sed -i "/PASSWORD/s/mypassword/awx/" mock_awx/settings/__init__.py && \
   sed -i "/HOST.*localhost/s/localhost/postgres/" mock_awx/settings/__init__.py && \
   uv run pytest -s -v'
```
