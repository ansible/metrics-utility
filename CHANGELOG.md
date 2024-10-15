# Changelog

## 0.0.1

- host_metric command

## 0.0.2

- gather_automation_controller_billing_data command

## 0.0.3

- gather_automation_controller_billing_data command extension
  Adding ability to run without "since specified", collecting any
  gap automatically.

## 0.0.4

- gather_automation_controller_billing_data command extension
  Adding ability to read AWS params required for provider billing.
- adding METRICS_UTILITY_SHIP_TARGET="directory", so the billing data
  will be stored in a provided directory in daily partitions
- adding build_report command that can build .xlsx reports locally

## 0.0.5

- adding proxy support for gather_automation_controller_billing_data command
- adding crc service account support for gather_automation_controller_billing_data command

## 0.1.0

- change local storage schema from year={year}/month={month}/day={day} to /{year}/{month}/{day}

## 0.2.0

- Take ansible_host and ansible_connection from host variables, use ansible_host instead
  of host_name if it's present for the CCSP(and other) reports

## 0.3.0

- Adding CCSPv2 reports
- Both CCSP and CCSPv2 reports are getting extra sheets with additional usage stats
- Fix tar.extractall call on older python version missing a filter arg
- Fix return codes

## 0.4.0
- Adding RENEWAL_GUIDANCE reports having host fact based dedup and detection of ephemeral hosts
- removing obsolete host_metric command
- adding organization usage details into CCSPv2 report