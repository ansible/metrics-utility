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

# Set ENV VARs
export METRICS_UTILITY_SHIP_TARGET=directory
export METRICS_UTILITY_SHIP_PATH=/awx_devel/awx-dev/metrics-utility/shipped_data/billing

# Run the gather command
python manage.py gather_automation_controller_billing_data --ship --until=10m
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


### Local data gathering

Set a storage adapter and path first, pick one from [Available storage adapters](#available-storage-adapters)

```
# Gather and store the data in provided SHIP_PATH directory under ./report_data subdir
metrics-utility gather_automation_controller_billing_data --ship --until=10m
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
