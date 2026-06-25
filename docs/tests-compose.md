# Running tests inside the docker compose environment

## One-shot (run all tests once)

```bash
make compose-pytest
```

## Interactive (run specific tests)

```bash
make compose-env  # starts a metrics-utility-env container with python & uv set up

# wait for postgres & minio containers to start, then:
docker exec -it metrics-utility-env /bin/sh
# or: podman exec -it metrics-utility-env /bin/sh

# inside the container:
uv run pytest -vv metrics_utility/test/ccspv_reports/test_complex_CCSP_with_scope.py  # 1 test
uv run pytest -vv metrics_utility/test/ccspv_reports  # all ccsp tests
uv run pytest -s -v metrics_utility/test/gather/  # all gather tests
uv run pytest -s -v  # everything
```

The container's environment variables (`METRICS_UTILITY_DB_HOST`, etc.) are already
configured to connect to the compose postgres -- no manual patching needed.


## metrics-service tests

```bash
make compose-pytest-svc
```

Runs the metrics-service test suite using the local metrics-utility checkout (via editable install).
Requires a `../metrics-service` checkout. This also runs in CI.
