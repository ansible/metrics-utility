# AAP metrics-utility

The AAP metrics utility tool is a standalone CLI utility called `metrics-utility` which is intended to be installed to
the system containing instance of the [Automation Controller](https://www.ansible.com/products/controller).
It's an alternative command line tool for the Controller's CLI `awx-manage`

## Installation

### From GitHub repository

Install as a root user:

```shell
# Download to any folder
cd ~
git clone https://github.com/ansible/metrics-utility.git

# Activate Automation Controller's Python virtual environment
. /var/lib/awx/venv/awx/bin/activate

# Install the utility
cd metrics-utility
pip install .
# Successfully installed metrics-utility-0.0.1

which metrics-utility
# /var/lib/awx/venv/awx/bin/metrics-utility

# Move to /usr/bin (like awx-manage)
mv /var/lib/awx/venv/awx/bin/metrics-utility /usr/bin/
```

## Available functionality


### Local data gathering and CCSP report generation

This set of commands will be periodically storing data and generating CCSP reports at the beginning of each month.
Run this command as a cronjob.


#### Example with local directory storage CCSP type

```
# Set needed ENV VARs for data gathering
export METRICS_UTILITY_SHIP_TARGET=directory
export METRICS_UTILITY_SHIP_PATH=/awx_devel/awx-dev/metrics-utility/shipped_data/billing

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


#### Example with local directory storage CCSPv2 type

```
# Set needed ENV VARs for data gathering
export METRICS_UTILITY_SHIP_TARGET=directory
export METRICS_UTILITY_SHIP_PATH=/awx_devel/awx-dev/metrics-utility/shipped_data/billing

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
metrics-utility gather_automation_controller_billing_data --ship --until=10m

# Build report for previous month unless it already exists. Report will be created under ./reports dir under SHIP_PATH dir.
metrics-utility build_report
```

### Pushing data periodically into console.redhat.com

This command will push new data into console.redhat.com, it automatically stores the last collected interval and will collect
up to 4 week long gap. The --until option collects the data until 10 minutes ago, to give time for fresh data to be inserted
into the Controller's database. Run this command as a cronjob.
```
export METRICS_UTILITY_SHIP_TARGET=crc
export METRICS_UTILITY_SERVICE_ACCOUNT_ID=<service account name>
export METRICS_UTILITY_SERVICE_ACCOUNT_SECRET=<service account secret>

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