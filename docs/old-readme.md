# AAP metrics-utility

The AAP metrics utility tool is a standalone CLI utility called `metrics-utility` which is intended to be installed to
the system containing instance of the [Automation Controller](https://www.ansible.com/products/controller).
It's an alternative command line tool for the Controller's CLI `awx-manage`.

## Installation

### Run from source

Run as awx user, to have the python virtual env available:

```shell
cd ~
git clone https://github.com/ansible/metrics-utility.git (or fetch the latest upstream commits)
cd metrics-utility

# Activate Automation Controller's Python virtual environment
. /var/lib/awx/venv/awx/bin/activate

# Install the dependecies
pip install .

# Set extra ENV VARs for report generation purposes
export METRICS_UTILITY_SHIP_TARGET=controller_db
export METRICS_UTILITY_REPORT_TYPE=RENEWAL_GUIDANCE
# add path for the report to go in
export METRICS_UTILITY_SHIP_PATH=/awx_devel/awx-dev/metrics-utility/shipped_data/billing

# Run some command, but rather instead of command using RPM like:
# metrics-utility build_report ...
# we run it as
# python manage.py build_report ...
python manage.py build_report --since=12months --ephemeral=1month
```


### From GitHub repository

Install as a root user (running this on older Controller envs can result in package conflicts):

```shell
# Download to any folder
cd ~
git clone https://github.com/ansible/metrics-utility.git

# Activate Automation Controller's Python virtual environment
. /var/lib/awx/venv/awx/bin/activate

# Install the utility
cd metrics-utility
pip install .
# Successfully installed metrics-utility

which metrics-utility
# /var/lib/awx/venv/awx/bin/metrics-utility

# Move to /usr/bin (like awx-manage)
mv /var/lib/awx/venv/awx/bin/metrics-utility /usr/bin/
```

## Available functionality

### Available storage adapters

#### Local directory

Storing datasets under a local directory. With using PVC on OpenShift deployments.

```
# Set needed ENV VARs for data gathering
export METRICS_UTILITY_SHIP_TARGET=directory
export METRICS_UTILITY_SHIP_PATH=/awx_devel/awx-dev/metrics-utility/shipped_data/billing
```

#### Object storage with S3 interface

Object storage with S3 like interface

```
# Set needed ENV VARs for data gathering
export METRICS_UTILITY_SHIP_TARGET=s3
export METRICS_UTILITY_SHIP_PATH=metrics-utility/shipped_data

# Define S3 config
export METRICS_UTILITY_BUCKET_NAME=metrics-utility
export METRICS_UTILITY_BUCKET_ENDPOINT=<endpoint to your S3>
# For AWS S3, define also a region
# export METRICS_UTILITY_BUCKET_REGION="us-east-1"

# Define S3 credentials
export METRICS_UTILITY_BUCKET_ACCESS_KEY=<access_key>
export METRICS_UTILITY_BUCKET_SECRET_KEY=<secret_key>
```


### Local data gathering and CCSP report generation

This set of commands will be periodically storing data and generating CCSP reports at the beginning of each month.
Run this command as a cronjob.


#### Example for CCSP type report

Set a storage adapter and path first, pick one from [Available storage adapters](#available-storage-adapters)

```
# Set extra ENV VARs for report generation purposes
export METRICS_UTILITY_REPORT_TYPE=CCSP
export METRICS_UTILITY_PRICE_PER_NODE=11.55 # in USD
export METRICS_UTILITY_REPORT_SKU=MCT3752MO
export METRICS_UTILITY_REPORT_SKU_DESCRIPTION="EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)"
export METRICS_UTILITY_REPORT_H1_HEADING="CCSP Reporting <Company>: ANSIBLE Consumption"
export METRICS_UTILITY_REPORT_COMPANY_NAME="Company Name"
export METRICS_UTILITY_REPORT_EMAIL="email@email.com"
export METRICS_UTILITY_REPORT_RHN_LOGIN="test_login"
export METRICS_UTILITY_REPORT_COMPANY_BUSINESS_LEADER="BUSINESS LEADER"
export METRICS_UTILITY_REPORT_COMPANY_PROCUREMENT_LEADER="PROCUREMENT LEADER"

# Gather and store the data in provided SHIP_PATH directory under ./report_data subdir
metrics-utility gather_automation_controller_billing_data --ship --until=10m

# Build report for previous month unless it already exists. Report will be created under ./reports dir under SHIP_PATH dir.
metrics-utility build_report
```


#### Example for CCSPv2 type report

Set a storage adapter and path first, pick one from [Available storage adapters](#available-storage-adapters)

```
# Set extra ENV VARs for report generation purposes
export METRICS_UTILITY_REPORT_TYPE=CCSPv2
export METRICS_UTILITY_PRICE_PER_NODE=11.55 # in USD
export METRICS_UTILITY_REPORT_SKU=MCT3752MO
export METRICS_UTILITY_REPORT_SKU_DESCRIPTION="EX: Red Hat Ansible Automation Platform, Full Support (1 Managed Node, Dedicated, Monthly)"
export METRICS_UTILITY_REPORT_H1_HEADING="CCSP NA Direct Reporting Template"
export METRICS_UTILITY_REPORT_COMPANY_NAME="Partner A"
export METRICS_UTILITY_REPORT_EMAIL="email@email.com"
export METRICS_UTILITY_REPORT_RHN_LOGIN="test_login"
export METRICS_UTILITY_REPORT_PO_NUMBER="123"
export METRICS_UTILITY_REPORT_END_USER_COMPANY_NAME="Customer A"
export METRICS_UTILITY_REPORT_END_USER_CITY="Springfield"
export METRICS_UTILITY_REPORT_END_USER_STATE="TX"
export METRICS_UTILITY_REPORT_END_USER_COUNTRY="US"

# Gather and store the data in provided SHIP_PATH directory under ./report_data subdir
python manage.py gather_automation_controller_billing_data --ship --until=10m
# or metrics-utility gather_automation_controller_billing_data --ship --until=10m

# Build report for previous month unless it already exists. Report will be created under ./reports dir under SHIP_PATH dir.
python manage.py build_report
# or metrics-utility build_report

# Build report for a specific month
python manage.py build_report --month=2024-06


```

#### Example for CCSPv2 type report for jobs, organizations and managed nodes usage history

This example reuses CCSPv2 type, but just provides a usage report outside of CCSP domain

Set a storage adapter and path first, pick one from [Available storage adapters](#available-storage-adapters)

```
# Set report type
export METRICS_UTILITY_REPORT_TYPE=CCSPv2

# Set subset of sheets to be built:
export METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS='jobs,managed_nodes,usage_by_organizations,managed_nodes_by_organizations'

# Optionally add a semicolon separated list of organizations, to limit the report only for certain orgs
# e.g. export METRICS_UTILITY_ORGANIZATION_FILTER="ACME;Test Org 1"

# Gather and store the data in provided SHIP_PATH directory under ./report_data subdir
python manage.py gather_automation_controller_billing_data --ship --until=10m
# or metrics-utility gather_automation_controller_billing_data --ship --until=10m

# Build report defining a custom range, report on last 5 months will be
python manage.py build_report --since=5months

# Build report defining a custom range with specific dates
python manage.py build_report --since=2024-05-01 --until=2024-09-30
```

### Example with Controller's database as a storage RENEWAL\_GUIDANCE type

```
# Set extra ENV VARs for report generation purposes
export METRICS_UTILITY_SHIP_TARGET=controller_db
export METRICS_UTILITY_REPORT_TYPE=RENEWAL_GUIDANCE
export METRICS_UTILITY_SHIP_PATH=/awx_devel/awx-dev/metrics-utility/shipped_data/billing

# Builds report covering 365days back by default
python manage.py build_report --since=12months --ephemeral=1month
# or metrics-utility build_report --since=12months --ephemeral=1month
```

### Pushing data periodically into console.redhat.com

This command will push new data into console.redhat.com, it automatically stores the last collected interval and will collect
up to 4 week long gap. The --until option collects the data until 10 minutes ago, to give time for fresh data to be inserted
into the Controller's database. Run this command as a cronjob.
```
export METRICS_UTILITY_SHIP_TARGET=crc
export METRICS_UTILITY_SERVICE_ACCOUNT_ID=<service account name>
export METRICS_UTILITY_SERVICE_ACCOUNT_SECRET=<service account secret>
export METRICS_UTILITY_OPTIONAL_COLLECTORS=""

# AWS specific params
export METRICS_UTILITY_BILLING_PROVIDER=aws
export METRICS_UTILITY_BILLING_ACCOUNT_ID="<AWS 12 digit customer id>"
export METRICS_UTILITY_RED_HAT_ORG_ID="<Red Had org id tied to the AWS billing account>"

metrics-utility gather_automation_controller_billing_data --ship --until=10m
```

You can inspect the data sent with --dry-run attribute and provide your own interval with --since and --until:
```
# This will collect whole day of 2023-12-21
metrics-utility gather_automation_controller_billing_data --dry-run --since=2023-12-21 --until=2023-12-22
```
