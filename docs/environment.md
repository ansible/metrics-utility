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
METRICS_UTILITY_DEDUPLICATOR
METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR
METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES
METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS
METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS
METRICS_UTILITY_OPTIONAL_COLLECTORS
METRICS_UTILITY_ORGANIZATION_FILTER
METRICS_UTILITY_PRICE_PER_NODE
METRICS_UTILITY_PROMETHEUS_URL
METRICS_UTILITY_PROXY_URL
METRICS_UTILITY_RED_HAT_ORG_ID
METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER
METRICS_UTILITY_REPORT_COMPANY_NAME
METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER
METRICS_UTILITY_REPORT_EMAIL
METRICS_UTILITY_REPORT_END_USER_CITY
METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME
METRICS_UTILITY_REPORT_END_USER_COUNTRY
METRICS_UTILITY_REPORT_END_USER_STATE
METRICS_UTILITY_REPORT_H1_HEADING
METRICS_UTILITY_REPORT_PO_NUMBER
METRICS_UTILITY_REPORT_RHN_LOGIN
METRICS_UTILITY_REPORT_SKU
METRICS_UTILITY_REPORT_SKU_DESCRIPTION
METRICS_UTILITY_REPORT_TYPE
METRICS_UTILITY_SERVICE_ACCOUNT_ID
METRICS_UTILITY_SERVICE_ACCOUNT_SECRET
METRICS_UTILITY_SHIP_PATH
METRICS_UTILITY_SHIP_TARGET
METRICS_UTILITY_USAGE_BASED_METERING_ENABLED
REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS
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


### Only used during gather

* `METRICS_UTILITY_CLUSTER_NAME` - `total_workers_vcpu` collector payload `.cluster_name` (required when that collector is enabled)
* `METRICS_UTILITY_COLLECTOR_LOCK_SUFFIX` - `total_workers_vcpu` collector custom lock name
* `METRICS_UTILITY_DISABLE_JOB_HOST_SUMMARY_COLLECTOR` - disable `job_host_summary` collector (use together with `METRICS_UTILITY_OPTIONAL_COLLECTORS`)
* `METRICS_UTILITY_DISABLE_SAVE_LAST_GATHERED_ENTRIES` - skip updating last gather info from controller settings
* `METRICS_UTILITY_MAX_GATHER_PERIOD_DAYS` - maximum lenght of collection interval in days, default 28; `get_max_gather_period_days`
* `METRICS_UTILITY_OPTIONAL_COLLECTORS` - optional collectors, comma-separated list
* `METRICS_UTILITY_PROMETHEUS_URL` - Prometheus base url
* `METRICS_UTILITY_USAGE_BASED_METERING_ENABLED` - `total_workers_vcpu` collector toggle - skips kubernetes when disabled (=false, default)


### Only used during build

#### deduplication

* `METRICS_UTILITY_DEDUPLICATOR` - choice of deduplication strategy
* `REPORT_RENEWAL_GUIDANCE_DEDUP_ITERATIONS` - number of max dedup iterations, specifically for `dedup-renewal`, with the `RENEWAL_GUIDANCE` report

#### logic

* `METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS` - enables optional report sheets, comma-separated list
* `METRICS_UTILITY_ORGANIZATION_FILTER` - CCSPv2 only, `report_organization_filter` - semicolon-separated list of org names to filter by
* `METRICS_UTILITY_REPORT_TYPE` - one of `CCSPv2`, `CCSP`, `RENEWAL_GUIDANCE`

#### ccsp data

* `METRICS_UTILITY_PRICE_PER_NODE` - {v1, v2} - `price_per_node` / `unit_price` - field value and multiplied by
* `METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER` - {v1} - `report_company_business_leader`
* `METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER` - {v1} - `report_company_procurement_leader`
* `METRICS_UTILITY_REPORT_COMPANY_NAME` - {v1, v2} - `report_company_name`
* `METRICS_UTILITY_REPORT_EMAIL` - {v1, v2} - `report_email`
* `METRICS_UTILITY_REPORT_END_USER_CITY` - {v2} - `report_end_user_company_city`
* `METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME` - {v2} - `report_end_user_company_name`
* `METRICS_UTILITY_REPORT_END_USER_COUNTRY` - {v2} - `report_end_user_company_country`
* `METRICS_UTILITY_REPORT_END_USER_STATE` - {v2} - `report_end_user_company_state`
* `METRICS_UTILITY_REPORT_H1_HEADING` - {v1, v2} - `report_h1_heading`
* `METRICS_UTILITY_REPORT_PO_NUMBER` - {v2} - `report_po_number`
* `METRICS_UTILITY_REPORT_RHN_LOGIN` - {v1, v2} - `report_rhn_login`
* `METRICS_UTILITY_REPORT_SKU` - {v1, v2} - `report_sku`
* `METRICS_UTILITY_REPORT_SKU_DESCRIPTION` - {v1, v2} - `report_sku_description`


### Both build & gather

* `METRICS_UTILITY_SHIP_PATH` - directory in local path or s3
* `METRICS_UTILITY_SHIP_TARGET` - one of `directory`, `s3`, `controller_db` (build), `crc` (gather)

---

## `extra_params` (build) / `billing_provider_params` (gather)

* `billing_account_id` - {gather}
  - `GatherCommand` -> `Collector` -> `config.json`
* `billing_provider` - {gather}
  - `GatherCommand` -> `Collector` -> `config.json`
* `bucket_access_key` - {gather, build}, (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
  - `BuildCommand` -> `ExtractorFactory` -> `ExtractorS3` -> `S3Handler`
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverS3` -> `S3Handler`
* `bucket_endpoint` - {gather, build}, (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
  - `BuildCommand` -> `ExtractorFactory` -> `ExtractorS3` -> `S3Handler`
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverS3` -> `S3Handler`
* `bucket_name` - {gather, build}, (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
  - `BuildCommand` -> `ExtractorFactory` -> `ExtractorS3` -> `S3Handler`
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverS3` -> `S3Handler`
* `bucket_region` - {gather, build}, (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
  - `BuildCommand` -> `ExtractorFactory` -> `ExtractorS3` -> `S3Handler`
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverS3` -> `S3Handler`
* `bucket_secret_key` - {gather, build}, (ship\_target=s3)
  - `GatherCommand` -> `Collector` -> `PackageS3` -> `S3Handler`
  - `BuildCommand` -> `ExtractorFactory` -> `ExtractorS3` -> `S3Handler`
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverS3` -> `S3Handler`
* `deduplicator` - {build}
  - `BuildCommand` -> `DedupFactory` -> `Dedup*`
* `ephemeral_days` - {build}, {renewal}
  - `BuildCommand` -> `ReportFactory` -> `ReportRenewalGuidance`
* `month_since` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `month_until` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `opt_since` - {build}, {v2, renewal}
  - `BuildCommand` -> `ExtractorFactory` -> `ExtractorControllerDB`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `opt_until` - {build}, {v2, renewal}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `optional_sheets` - {build}, {v1, v2, renewal}
  - `BuildCommand` -> `ExtractorFactory` -> `Extractor*` -> `Base` (extractor)
  - `BuildCommand` -> `ReportFactory` -> `Report*` -> `Base` (report)
* `price_per_node` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `red_hat_org_id` - {gather}
  - `GatherCommand` -> `Collector` -> `config.json`
* `report_company_business_leader` - {build}, {v1}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
* `report_company_name` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_company_procurement_leader` - {build}, {v1}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
* `report_email` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_end_user_company_city` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_end_user_company_country` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_end_user_company_name` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_end_user_company_state` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_h1_heading` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_organization_filter` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_period` - {build}, {v1, v2, renewal}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
  - `BuildCommand` -> `ReportFactory` -> `ReportRenewalGuidance`
* `report_po_number` - {build}, {v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_renewal_guidance_dedup_iterations` - {build, renewal}
  - `BuildCommand` -> `DedupFactory` -> `DedupRenewal`
* `report_rhn_login` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_sku` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_sku_description` - {build}, {v1, v2}
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSP`
  - `BuildCommand` -> `ReportFactory` -> `ReportCCSPv2`
* `report_spreadsheet_destination_path` - {build}
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverDirectory`
  - `BuildCommand` -> `ReportSaverFactory` -> `ReportSaverS3`
* `report_type` - {build}
  - `BuildCommand` -> `DataframeFactory`
  - `BuildCommand` -> `DedupFactory`
  - `BuildCommand` -> `ReportFactory`
* `ship_path` - {build, gather}
  - `GatherCommand` -> `Collector` -> `PackageDirectory`
  - `GatherCommand` -> `Collector` -> `PackageS3`
  - `BuildCommand` -> `ExtractorFactory` -> `Extractor*` -> `Base` (extractor)
* `since_date` - {build}, {v1, v2, renewal}
  - `BuildCommand` -> `DataframeFactory` -> `Dataframe*` -> `Base` (dataframe)
  - `BuildCommand` -> `ReportFactory` -> `ReportRenewalGuidance`
* `until_date` - {build}, {v1, v2, renewal}
  - `BuildCommand` -> `DataframeFactory` -> `Dataframe*` -> `Base` (dataframe)
  - `BuildCommand` -> `ReportFactory` -> `ReportRenewalGuidance`
