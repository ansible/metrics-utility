# AAP Metrics-Utility

A standalone CLI utility for collecting and reporting metrics from [Ansible Automation Platform (AAP)](https://www.ansible.com/products/automation-platform) Controller instances. This tool allows users to:

- Collect and analyze Controller usage data
- Generate reports (CCSP, CCSPv2, RENEWAL_GUIDANCE)
- Support multiple storage adapters for data persistence
- Push metrics data to `console.redhat.com`

## Quick Start

### Prerequisites

- **Python 3.11 or later**
- **uv package manager** (Install with `pip install uv` if not already installed)
- **Dependencies managed via `pyproject.toml`** (Ensure `uv.lock` is used for consistency)

### Installation

```bash
# Clone the repository
git clone https://github.com/ansible/metrics-utility.git
cd metrics-utility

# Install dependencies using uv
uv pip install .
```

For development setup, see the [Developer Setup Guide](./docs/developer_setup.md).

### Basic Usage

#### Standalone Mode (CCSPv2 Reports)

The utility can generate CCSPv2 reports using test data without requiring a Controller environment:

```bash
# Generate a test CCSPv2 report using the convenience script
./run-ccsp2-build

# Or manually:
export METRICS_UTILITY_REPORT_TYPE=CCSPv2
python manage.py build_report
```

Reports will be saved in `metrics_utility/test/test_data/reports/YYYY/MM/`.

#### Controller Environment Mode

For data gathering and other report types, the utility needs to be run in a Controller environment. **If Automation Controller is not running, execution will fail with a missing module error.**

Recent changes in AWX have decoupled some dependencies, meaning certain components must now be mocked when running outside of a full AWX environment. Ensure Automation Controller is running before executing commands.

1. Install the utility in your Controller environment:

   ```bash
   cd metrics-utility
   uv pip install .
   ```

2. Configure storage location:

   ```bash
   export METRICS_UTILITY_SHIP_TARGET=directory
   export METRICS_UTILITY_SHIP_PATH=/path/to/store/data
   ```

3. Collect and analyze data:

   ```bash
   # Collect metrics data
   python manage.py gather_automation_controller_billing_data --ship --until=10m
   ```
   
   If the Automation Controller environment is missing, you will see an error like:
   
   ```
   Automation Controller modules not found in /awx_devel (AWX_PATH). Using mock and continuing.
   ModuleNotFoundError: No module named 'awx.main.utils.pglock'
   ```
   
   Ensure Automation Controller is correctly installed and running before proceeding.

4. Generate renewal guidance report:

   ```bash
   export METRICS_UTILITY_REPORT_TYPE=RENEWAL_GUIDANCE
   python manage.py build_report --since=12months --ephemeral=1month
   ```

## Documentation

Documentation is available in the [`/docs` directory](./docs).

> **Note:** The older README, which includes installation instructions, legacy AWX integration details, and additional information about available storage adapters and report types, has been moved to [`/docs/old-readme.md`](./docs/old-readme.md) for reference.

## Contributing

Please follow our [Contributor's Guide](./docs/contributing/CONTRIBUTING.md) for details on submitting changes and documentation standards.

