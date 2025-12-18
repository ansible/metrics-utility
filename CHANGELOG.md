# Changelog

## 0.7.0dev

- Removed direct AWX dependencies from collectors module
- Replaced AWX license functions with direct database queries
- added `metrics_utility.library`
- TODO


## 0.6.1

- vcpu collector - use prometheus
- gather: fix empty config.json when `job_host_summary` disabled
- dev: future optional collectors


## 0.6.0

- more validation for parameters / environment variables validation
- more tailored error messages for known exceptions, better --help
- deduplication: optional deduplication modes, adds `product_serial` & `machine_id` to hostnames-based dedup
- job host summary gather optimizations
- indirect node summary report tab
- vcpu workers collector
- dev: use mirror.gcr.io for dev containers; code cleanups; more MacOS X compatible scripts
- dev: tests set up awx environment by default; more tests
- dev: perf tests split up to make testing branches easier
- dev: remove pytz


## 0.4.1

- relax boto3 requirement so it builds with any version available


## 0.5.0

- add indirectly managed nodes support, sheets
- add scope (hosts across all inventories) support, sheets
- count only reachable hosts into ccsp
- add optional collector coverage sheet
- allow importing pglock from either awx or django-ansible-base (2.4/2.5 compatibility)
- perf: reduce need to load data only used by optional sheets
- handle missing `job_created` for older `job_host_summary` data
- basic param validation for --since, --until
- better error handling for missing data
- dev: added tests, mock data generator, scripts, docs, linters, workflows
- dev: standalone mode with just postgres; python 3.13
- dev: merge in insights-analytics-generator base lib


## 0.4.0

- Adding `RENEWAL_GUIDANCE` reports having host fact based dedup and detection of ephemeral hosts
- removing obsolete `host_metric` command
- adding organization usage details into CCSPv2 report
- adding job usage details into CCSPv2 report
- make CCSPv2 report generated sheets configurable, to be able to use it as a report for AAP historical usage
- allow arbitrary date range selection for the CCSPv2 report
- introducing S3 adapter for CCSP types of reports


## 0.3.0

- Adding CCSPv2 reports
- Both CCSP and CCSPv2 reports are getting extra sheets with additional usage stats
- Fix tar.extractall call on older python version missing a filter arg
- Fix return codes


## 0.2.0

- Take `ansible_host` and `ansible_connection` from host variables, use `ansible_host` instead
  of `host_name` if it's present for the CCSP(and other) reports


## 0.1.0

- change local storage schema from year={year}/month={month}/day={day} to /{year}/{month}/{day}


## 0.0.5

- adding proxy support for `gather_automation_controller_billing_data` command
- adding crc service account support for `gather_automation_controller_billing_data` command


## 0.0.4

- `gather_automation_controller_billing_data` command extension
  Adding ability to read AWS params required for provider billing.
- adding `METRICS_UTILITY_SHIP_TARGET="directory"`, so the billing data
  will be stored in a provided directory in daily partitions
- adding `build_report` command that can build .xlsx reports locally


## 0.0.3

- `gather_automation_controller_billing_data` command extension
  Adding ability to run without "since specified", collecting any
  gap automatically.


## 0.0.2

- `gather_automation_controller_billing_data` command


## 0.0.1

- `host_metric` command
