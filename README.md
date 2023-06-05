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

# Activate Controller's Python virtual environment
. /var/lib/awx/venv/bin/activate

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
