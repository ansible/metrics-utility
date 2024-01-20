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

## Available commands

### host_metric

Outputs the Host Metrics available in the Controller.

```shell
metrics-utility host_metric [--since=2023-06-01]
# since - optional - Start Date in ISO format YYYY-MM-DD
```


### gather_automation_controller_billing_data

Gather Controller billing data


Gather data for a specific datetime frame and just store them under /tmp for review:

```
# This will collect whole day of 2023-12-21
metrics-utility gather_automation_controller_billing_data --dry-run --since=2023-12-21 --until=2023-12-22
```

Gather and ship billing data to console.redhat.com for a specific datetime frame:
```
# You need to set 'Red Hat customer username/password' under Automation Controller 'Miscellaneous System' settings
# This will collect and ship whole day of 2023-12-21
metrics-utility gather_automation_controller_billing_data --ship --since=2023-12-21 --until=2023-12-22
```

Gather and ship billing data to console.redhat.com for a dynamic datetime range:
```
# You need to set 'Red Hat customer username/password' under Automation Controller 'Miscellaneous System' settings
# This will collect and ship data for yesterday, interval <2 days ago, 1 day ago>
metrics-utility gather_automation_controller_billing_data --ship --since=2d --until=1d
```

Gather and ship billing data to console.redhat.com with automatically collecting gap, by storing a last collected
timestamp and always collecting from that last succesfully collected timestamp. To be on the safe side, we can
collect interval <last_collected_timestamp_or_4_weeks_back, 10_minutes_ago> to give all records time to insert.
```
# You need to set 'Red Hat customer username/password' under Automation Controller 'Miscellaneous System' settings
# This will collect and ship data for interval <last_collected_timestamp_or_4_weeks_back, 10_minutes_ago>
metrics-utility gather_automation_controller_billing_data --ship --until=10m
```