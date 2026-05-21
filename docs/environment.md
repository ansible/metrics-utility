All known metrics-utility environment variables:

```
AWX_PATH
KUBERNETES_SERVICE_PORT
METRICS_UTILITY_BILLING_ACCOUNT_ID
METRICS_UTILITY_BILLING_PROVIDER
METRICS_UTILITY_BUCKET_ACCESS_KEY
METRICS_UTILITY_BUCKET_ENDPOINT
METRICS_UTILITY_BUCKET_NAME
METRICS_UTILITY_BUCKET_REGION
METRICS_UTILITY_BUCKET_SECRET_KEY
METRICS_UTILITY_CLUSTER_NAME
METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX
METRICS_UTILITY_CRC_INGRESS_URL
METRICS_UTILITY_CRC_SSO_URL
METRICS_UTILITY_DB_HOST
METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR
METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES
METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS
METRICS_UTILITY_OPTIONAL_COLLECTORS
METRICS_UTILITY_PROMETHEUS_URL
METRICS_UTILITY_PROXY_URL
METRICS_UTILITY_RED_HAT_ORG_ID
METRICS_UTILITY_SERVICE_ACCOUNT_ID
METRICS_UTILITY_SERVICE_ACCOUNT_SECRET
METRICS_UTILITY_SHIP_PATH
METRICS_UTILITY_SHIP_TARGET
METRICS_UTILITY_USAGE_BASED_METERING_ENABLED
container
```


### Specials:

* `KUBERNETES_SERVICE_PORT` - Used by collectors' `get_install_type` - for `k8s`
* `container` - Used by collectors' `get_install_type` - for `oci`


### Dev:

* `AWX_PATH` - used to find controller virtualenv, when *not* using `mock_awx`; defaults to `/awx_devel`; runs from `metrics_utility/__init__.py` `.prepare`/`.manage`
* `METRICS_UTILITY_DB_HOST` - host to talk to controller db when using `mock_awx`


### Stored in `config.json` - `billing_provider_params`

* `METRICS_UTILITY_BILLING_ACCOUNT_ID` - `billing_account_id`
* `METRICS_UTILITY_BILLING_PROVIDER` - `billing_provider`
* `METRICS_UTILITY_RED_HAT_ORG_ID` - `red_hat_org_id`


### Used by S3

* `METRICS_UTILITY_BUCKET_ACCESS_KEY` - `bucket_access_key`
* `METRICS_UTILITY_BUCKET_ENDPOINT` - `bucket_endpoint`
* `METRICS_UTILITY_BUCKET_NAME` - `bucket_name`
* `METRICS_UTILITY_BUCKET_REGION` - `bucket_region`
* `METRICS_UTILITY_BUCKET_SECRET_KEY` - `bucket_secret_key`


### Used by CRC

* `METRICS_UTILITY_CRC_INGRESS_URL` - upload url
* `METRICS_UTILITY_CRC_SSO_URL` - login url
* `METRICS_UTILITY_PROXY_URL` - upload proxy
* `METRICS_UTILITY_SERVICE_ACCOUNT_ID` - account id
* `METRICS_UTILITY_SERVICE_ACCOUNT_SECRET` - secret


### Gather configuration

* `METRICS_UTILITY_CLUSTER_NAME` - `total_workers_vcpu` collector payload `.cluster_name` (required when that collector is enabled)
* `METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX` - `total_workers_vcpu` collector custom lock name
* `METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR` - disable `job_host_summary` collector (use together with `METRICS_UTILITY_OPTIONAL_COLLECTORS`)
* `METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES` - skip updating last gather info from controller settings
* `METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS` - maximum length of collection interval in days, default 28; `get_max_gather_period_days`
* `METRICS_UTILITY_OPTIONAL_COLLECTORS` - optional collectors, comma-separated list
* `METRICS_UTILITY_PROMETHEUS_URL` - Prometheus base url
* `METRICS_UTILITY_SHIP_PATH` - directory in local path or s3
* `METRICS_UTILITY_SHIP_TARGET` - one of `directory`, `s3`, `crc`
* `METRICS_UTILITY_USAGE_BASED_METERING_ENABLED` - `total_workers_vcpu` collector toggle - skips kubernetes when disabled (=false, default)


---

## `billing_provider_params`

* `billing_account_id`
  - `GatherCommand` -> `Collector` -> `config.json`
* `billing_provider`
  - `GatherCommand` -> `Collector` -> `config.json`
* `bucket_access_key` (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
* `bucket_endpoint` (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
* `bucket_name` (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
* `bucket_region` (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
* `bucket_secret_key` (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
* `red_hat_org_id`
  - `GatherCommand` -> `Collector` -> `config.json`
* `ship_path`
  - `GatherCommand` -> `Collector` -> `PackageDirectory`
  - `GatherCommand` -> `Collector` -> `PackageS3`
