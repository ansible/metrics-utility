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
