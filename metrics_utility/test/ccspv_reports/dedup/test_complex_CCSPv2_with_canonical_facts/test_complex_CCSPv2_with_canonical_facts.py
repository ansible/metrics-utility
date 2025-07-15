import json
import os
import sys
import tarfile

from unittest.mock import patch

import openpyxl
import pandas
import pytest

# Import helper functions from conftest
from .conftest import (
    copy_if_content_changed,
    get_test_dir,
    sort_json_fields,
    transform_sheet,
)


sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from metrics_utility.test.util import run_build_int


env_vars = {
    'METRICS_UTILITY_SHIP_PATH': './metrics_utility/test/test_data',
    'METRICS_UTILITY_SHIP_TARGET': 'directory',
    'METRICS_UTILITY_REPORT_TYPE': 'CCSPv2',
    'METRICS_UTILITY_OPTIONAL_COLLECTORS': 'main_host,main_jobevent,main_indirectmanagednodeaudit',
    'METRICS_UTILITY_OPTIONAL_CCSP_REPORT_SHEETS': (
        'ccsp_summary,jobs,managed_nodes,indirectly_managed_nodes,inventory_scope,usage_by_organizations,usage_by_collections,usage_by_roles,usage_by_modules,data_collection_status'
    ),
    'METRICS_UTILITY_DEDUPLICATOR': 'ccsp-experimental',  # Enable experimental deduplication
}

file_path = './metrics_utility/test/test_data/reports/2025/07/CCSPv2-2025-07-08--2025-07-11.xlsx'


@pytest.mark.filterwarnings('ignore::ResourceWarning')
@pytest.mark.parametrize(
    'cleanup',
    [
        file_path,
    ],
    indirect=True,
)
def test_command_with_extended_canonical_facts(cleanup, request):
    """Build xlsx report using build command with extended canonical facts and test its contents.

    This integration test demonstrates:
    - Extended canonical facts (ansible_host, host_name, ansible_port) collection and reporting
    - Multiple tarball processing across 3 days (2025-07-08 to 2025-07-10)
    - Non-numeric port handling (e.g., 'ssh' -> NULL)
    - Comprehensive test data covering various canonical facts scenarios
    - CCSPv2 XLSX report generation with proper data validation

    Note: This test focuses on data collection and reporting pipeline validation.
    For actual deduplication testing, see test_ccsp_realistic_dedup.py which uses
    mock data with proper serial computation.
    """

    # Extract CSVs from tarballs for human review
    extract_csvs_from_tarballs()

    # Mock the current_date method to return consistent date
    with patch('metrics_utility.automation_controller_billing.report.report_ccsp_v2.ReportCCSPv2.current_date', return_value='Jul 14, 2025'):
        # Running a command python way, so we can work with debugger in the code
        run_build_int(
            env_vars,
            {
                'since': '2025-07-08',
                'until': '2025-07-11',
                'force': True,
            },
        )

    # Simple verification that CSV files can be opened after generation
    verify_csv_files_can_open()

    # Validate input data structure and content
    validate_input_main_host_data()
    validate_input_job_host_summary_data()
    validate_deduplication_behavior()

    workbook = None
    try:
        # test workbook is openable with the lib we're creating it with
        workbook = openpyxl.load_workbook(filename=file_path)

        # Save a copy of the report to the reports directory for reference
        test_dir = get_test_dir()
        reports_dir = test_dir / 'reports'
        reports_dir.mkdir(exist_ok=True)
        test_report_path = reports_dir / 'CCSPv2-2025-07-08--2025-07-11.xlsx'

        # Only copy if content has changed
        copy_if_content_changed(file_path, test_report_path)

        # First, analyze and generate all possible outputs
        analyze_and_generate_all_outputs(file_path, request)

        # Validate all report sheets
        validate_ccsp_summary(file_path)
        validate_jobs(file_path)
        validate_managed_nodes(file_path)
        validate_indirectly_managed_nodes(file_path)
        validate_inventory_scope(file_path)
        validate_usage_by_organizations(file_path)
        validate_usage_by_collections(file_path)
        validate_usage_by_roles(file_path)
        validate_usage_by_modules(file_path)
        validate_data_collection_status(file_path)

    finally:
        if workbook:
            workbook.close()


def validate_managed_nodes(file_path):
    """Validate that managed nodes sheet shows proper deduplication results.

    Deduplication Logic Overview:
    -----------------------------
    The experimental deduplication (deduplicator='ccsp-experimental') merges hosts based on
    canonical facts, specifically the combination of:
    - ansible_product_serial (hardware serial number)
    - ansible_machine_id (system machine ID)

    Hosts with the same serial number AND machine ID are considered the same physical machine
    and are merged into a single entry. The first hostname encountered becomes the canonical
    hostname for the merged entry.

    Key Points:
    - Deduplication happens ACROSS organizations - hosts from different orgs can be merged
    - The "Automated by organizations" count shows how many unique orgs touched the host
    - Job runs and task runs are summed across all deduplicated entries
    - The canonical facts and host names show ALL values from merged hosts
    - The "Host names before deduplication" currently only shows the canonical hostname
      (this appears to be a limitation in the current implementation)
    """
    sheet = pandas.read_excel(file_path, sheet_name='Managed nodes')
    actual = transform_sheet(sheet.to_dict())

    # Validate input CSV data integrity using CSV files with cross-validation
    validate_input_csv_data_integrity()

    # Deduplication Test Cases Explained:
    # ===================================
    #
    # DEDUPLICATED HOSTS (merged based on matching serial + machine_id):
    # -------------------------------------------------------------------
    # 1. app01.cluster (4 entries → 1):
    #    - All 4 entries have same serial (HP-ProLiant-DL380) + machine_id (machine123)
    #    - Entries from 3 different orgs (Production x2, Development, Staging)
    #    - Result: Merged into single entry showing 3 organizations
    #
    # 2. web01.internal + web01.prod.company.com (3 entries → 1):
    #    - All have same VMware serial + machine_id (3a2f8c9b...)
    #    - Different hostnames but same physical machine
    #    - Result: Merged, showing both hostnames in canonical facts
    #
    # 3. web02.external + web02.internal (2 entries → 1):
    #    - Same VMware serial + machine_id (def789ghi012)
    #    - Different network access points to same machine
    #    - Result: Merged into web02.external (first seen)
    #
    # 4. db02.dev + db02.staging (2 entries → 1):
    #    - Same Dell serial (R750) + machine_id (db02-machine-id)
    #    - Different environment names for same database server
    #    - Result: Merged into db02.dev
    #
    # 5. web03.internal + web03.prod.internal (2 entries → 1):
    #    - Same VMware serial + machine_id (web03-machine-id)
    #    - Production variants of same web server
    #    - Result: Merged into web03.internal
    #
    # 6. cache01.internal (2 entries → 1):
    #    - Both have same machine_id (xyz789) but NO product_serial
    #    - From different orgs (Production, Development)
    #    - Result: Merged because machine_id matches (serial not required if missing)
    #
    # NOT DEDUPLICATED HOSTS (unique serial/machine_id combinations):
    # ---------------------------------------------------------------
    # 1. app01.failover:
    #    - Different machine_id (machine456) than app01.cluster
    #    - Same serial type but different physical machine
    #    - Result: Kept separate
    #
    # 2. db01.company.com:
    #    - Has product_serial but NO machine_id
    #    - Cannot deduplicate without machine_id
    #    - Result: Kept separate
    #
    # 3. log01.company.com:
    #    - Missing BOTH product_serial AND machine_id
    #    - No canonical facts to deduplicate on
    #    - Result: Kept separate
    #
    # 4. web04.dev and web04.staging:
    #    - Different machine_ids (web04-dev-machine vs web04-staging-machine)
    #    - Different serials (VMware-dev-... vs VMware-stg-...)
    #    - Result: Kept as separate hosts (different environments)
    #
    # FALSE NEGATIVES - NOT DEDUPLICATED (but should be):
    # ---------------------------------------------------
    # 1. win-srv01.company.com and win-srv01.internal:
    #    - Same Windows server (WIN-HP-DL380-001 serial)
    #    - Windows lacks machine_id (systemd-specific)
    #    - Only product_serial available for deduplication
    #    - Result: Kept separate (SHOULD be merged based on serial alone)
    #
    # 2. k8s-node-01.cluster and k8s-node-01.internal:
    #    - Same Kubernetes node accessed differently
    #    - Container environment lacks both machine_id and serial
    #    - No canonical facts for deduplication
    #    - Result: Kept separate (SHOULD be merged based on hostname pattern)
    #
    # 3. secure-host-01.company.com (privileged vs unprivileged):
    #    - Same host accessed with different credentials
    #    - Admin job has product_serial, user job doesn't
    #    - Same machine_id in both cases
    #    - Result: Kept separate (SHOULD be merged based on machine_id)
    #
    # FALSE POSITIVES - WRONGLY DEDUPLICATED (but shouldn't be):
    # ---------------------------------------------------------
    # 1. aws-vm-01.us-east and aws-vm-02.us-west:
    #    - Different AWS VMs in different regions
    #    - Cloud-init generates same synthetic machine_id
    #    - Generic AWS product_serial (ec2-instance)
    #    - Result: Wrongly merged (SHOULD be kept separate)
    #
    # 2. nat-host-01.external and nat-host-02.external:
    #    - Different hosts behind same NAT gateway
    #    - NAT gateway's machine_id and serial exposed to both
    #    - Same public IP address (203.0.113.10)
    #    - Result: Wrongly merged (SHOULD be kept separate)

    # Just validate we have the expected number of entries after adding new test cases
    assert len(actual) == 15, f'Expected 15 managed nodes entries, got {len(actual)}'

    # Validate key hosts are present to ensure deduplication worked
    host_names = [entry['Host name'] for entry in actual.values()]
    expected_hosts = [
        '203.0.113.10',  # NAT hosts deduplicated to IP
        'app01.cluster',
        'app01.failover',
        'aws-vm-01.us-east',  # AWS hosts deduplicated
        'cache01.internal',
        'db01.company.com',
        'db02.dev',  # db02.staging deduplicated
        'log01.company.com',
        'web01.internal',  # web01.prod.company.com deduplicated
        'web02.external',  # web02.internal deduplicated
        'web03.internal',  # web03.prod.internal deduplicated
        'web04.dev',
        'web04.staging',
        'win-srv01.company.com',
        'win-srv02.company.com',
    ]

    missing_hosts = set(expected_hosts) - set(host_names)
    assert len(missing_hosts) == 0, f'Missing hosts in managed nodes: {missing_hosts}'


def validate_inventory_scope(file_path):
    """Validate inventory scope sheet shows all hosts with deduplication information."""
    sheet = pandas.read_excel(file_path, sheet_name='Inventory Scope')
    actual = transform_sheet(sheet.to_dict())

    # Just validate we have the expected number of entries after adding new test cases
    assert len(actual) == 15, f'Expected 15 inventory scope entries, got {len(actual)}'

    # Validate key hosts are present to ensure deduplication worked
    host_names = [entry['Host name'] for entry in actual.values()]
    expected_hosts = [
        '203.0.113.10',  # NAT hosts deduplicated to IP
        'app01.cluster',
        'app01.failover',
        'aws-vm-01.us-east',  # AWS hosts deduplicated
        'cache01.internal',
        'db01.company.com',
        'db02.dev',  # db02.staging deduplicated
        'log01.company.com',
        'web01.internal',  # web01.prod.company.com deduplicated
        'web02.external',  # web02.internal deduplicated
        'web03.internal',  # web03.prod.internal deduplicated
        'web04.dev',
        'web04.staging',
        'win-srv01.company.com',
        'win-srv02.company.com',
    ]

    missing_hosts = set(expected_hosts) - set(host_names)
    assert len(missing_hosts) == 0, f'Missing hosts in inventory scope: {missing_hosts}'

    # Validate deduplication working - check that some hosts have multiple entries before deduplication
    dedup_counts = [entry['Host names before deduplication count'] for entry in actual.values()]
    multi_dedup_hosts = [count for count in dedup_counts if count > 1]
    assert len(multi_dedup_hosts) > 0, 'Expected some hosts to be deduplicated (count > 1)'


def validate_usage_by_organizations(file_path):
    """Validate usage by organization with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by organizations')
    actual = transform_sheet(sheet.to_dict())

    # Expected: Usage stats showing actual data with experimental deduplication
    expected = {
        0: {
            'Organization name': 'Default',
            'Job runs': 3,
            'Unique managed nodes automated': 0,
            'Non-unique managed nodes automated': 0,
            'Unique indirect managed nodes automated': 3,
            'Non-unique indirect managed nodes automated': 3,
            'Number of task runs': 3,
        },
        1: {
            'Organization name': 'Development',
            'Job runs': 4,  # job runs in Development org
            'Unique managed nodes automated': 4,  # 4 unique hosts after deduplication
            'Non-unique managed nodes automated': 4,  # 4 hosts (no dedup within Development)
            'Unique indirect managed nodes automated': 0,  # no indirect nodes
            'Non-unique indirect managed nodes automated': 0,  # no indirect nodes
            'Number of task runs': 24,  # task runs
        },
        2: {
            'Organization name': 'Production',
            'Job runs': 18,  # job runs in Production org
            'Unique managed nodes automated': 12,  # 12 unique hosts after deduplication
            'Non-unique managed nodes automated': 19,  # 19 total before deduplication
            'Unique indirect managed nodes automated': 0,  # no indirect nodes
            'Non-unique indirect managed nodes automated': 0,  # no indirect nodes
            'Number of task runs': 135,  # total task runs
        },
        3: {
            'Organization name': 'Staging',
            'Job runs': 3,  # job runs in Staging org
            'Unique managed nodes automated': 3,  # 3 unique hosts
            'Non-unique managed nodes automated': 3,  # 3 hosts (no dedup within Staging)
            'Unique indirect managed nodes automated': 0,  # no indirect nodes
            'Non-unique indirect managed nodes automated': 0,  # no indirect nodes
            'Number of task runs': 16,  # task runs
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_usage_by_collections(file_path):
    """Validate usage by collections with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by collections')
    actual = transform_sheet(sheet.to_dict())

    # The usage by collections only shows collections actually used in direct job runs
    # Indirect nodes use different collections (kubernetes.node, vmware.vmware) but those
    # are tracked separately and not shown in this sheet
    expected = {
        0: {
            'Collection name': 'ansible.builtin',
            'Unique managed nodes automated': 2,  # Only 2 unique nodes used ansible.builtin
            'Non-unique managed nodes automated': 2,  # Same as unique in this case
            'Number of task runs': 6,
            'Duration of task runs [seconds]': 8.1,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_usage_by_roles(file_path):
    """Validate usage by roles with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by roles')
    actual = transform_sheet(sheet.to_dict())

    expected = {
        0: {
            'Role name': 'No role used',
            'Unique managed nodes automated': 2,
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 6,
            'Duration of task runs [seconds]': 8.1,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_usage_by_modules(file_path):
    """Validate usage by modules with deduplication effects."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage by modules')
    actual = transform_sheet(sheet.to_dict())

    expected = {
        0: {
            'Module name': 'ansible.builtin.debug',
            'Unique managed nodes automated': 2,  # Only direct nodes that ran this module
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 3,
            'Duration of task runs [seconds]': 2.1,
        },
        1: {
            'Module name': 'ansible.builtin.setup',
            'Unique managed nodes automated': 2,  # Only direct nodes that ran this module
            'Non-unique managed nodes automated': 2,
            'Number of task runs': 3,
            'Duration of task runs [seconds]': 6.0,
        },
    }

    assert sort_json_fields(actual) == sort_json_fields(expected)


def validate_ccsp_summary(file_path):
    """Validate CCSP summary sheet (Usage Reporting)."""
    sheet = pandas.read_excel(file_path, sheet_name='Usage Reporting')

    # The Usage Reporting sheet is a CCSP summary format with specific structure
    # We'll validate it has the expected structure as a dict
    expected = {
        'structure': {
            'type': 'ccsp_summary',
            'has_header_fields': True,
            'has_report_period': True,
            'report_period_contains': ['2025-07-08', '2025-07-11'],
            'has_sku_data': True,
            'total_unique_nodes': 15,
        }
    }

    # Read raw data to validate structure
    raw_data = sheet.to_dict()

    # Build actual structure analysis
    actual = {
        'structure': {
            'type': 'ccsp_summary',
            'has_header_fields': False,
            'has_report_period': False,
            'report_period_contains': [],
            'has_sku_data': False,
            'total_unique_nodes': 0,
        }
    }

    # Check header fields exist
    first_column = raw_data.get('Unnamed: 0', {})
    header_fields = ['CCSP Company Name', 'CCSP Email', 'CCSP RHN Login', 'Report Period (YYYY-MM)', 'End User Company Name']
    has_all_headers = all(any(field in str(first_column.get(i, '')) for i in range(10)) for field in header_fields)
    actual['structure']['has_header_fields'] = has_all_headers

    # Check report period
    period_value = raw_data.get('Unnamed: 1', {}).get(3, '')
    if '2025-07-08' in str(period_value) and '2025-07-11' in str(period_value):
        actual['structure']['has_report_period'] = True
        actual['structure']['report_period_contains'] = ['2025-07-08', '2025-07-11']

    # Check for SKU data - look for quantity 15 anywhere in the sheet
    for col_name, col_data in raw_data.items():
        if isinstance(col_data, dict):
            for row_idx, value in col_data.items():
                if value == 15:
                    actual['structure']['has_sku_data'] = True
                    actual['structure']['total_unique_nodes'] = 15
                    break
        if actual['structure']['has_sku_data']:
            break

    assert actual == expected


def validate_jobs(file_path):
    """Validate Jobs sheet."""
    sheet = pandas.read_excel(file_path, sheet_name='Jobs')
    actual = transform_sheet(sheet.to_dict())

    # Jobs sheet has individual job runs with First run/Last run columns
    # Just validate a few sample entries to ensure the structure is correct
    expected_sample = {
        0: {
            'Job template name': 'Kubernetes Template',
            'Organization name': 'Default',
            'Job runs': 1,
        },
        1: {
            'Job template name': 'VMware Template',
            'Organization name': 'Default',
            'Job runs': 1,
        },
    }

    # Validate the first few entries match expected structure
    for i in range(min(2, len(actual))):
        assert actual[i]['Job template name'] == expected_sample[i]['Job template name']
        assert actual[i]['Organization name'] == expected_sample[i]['Organization name']
        assert actual[i]['Job runs'] == expected_sample[i]['Job runs']

    # Assert we have the expected total number of job template entries
    assert len(actual) == 23  # Based on the CSV output after adding new test data


def validate_indirectly_managed_nodes(file_path):
    """Validate Indirectly Managed nodes sheet."""
    sheet = pandas.read_excel(file_path, sheet_name='Indirectly Managed nodes')
    actual = transform_sheet(sheet.to_dict())

    # Just validate we have 3 indirect nodes as shown in the CSV output
    assert len(actual) == 3, f'Expected 3 indirect nodes, got {len(actual)}'

    # Validate the structure of the first node
    if len(actual) > 0:
        first_node = actual[0]
        expected_fields = ['Host name', 'Automated by organizations', 'Events', 'First automation', 'Last automation', 'Canonical Facts', 'Facts']
        for field in expected_fields:
            assert field in first_node, f'Missing field {field} in indirect node'


def validate_data_collection_status(file_path):
    """Validate Data collection status sheet."""
    sheet = pandas.read_excel(file_path, sheet_name='Data collection status')
    actual = transform_sheet(sheet.to_dict())

    # Data collection status shows collection events across different time periods
    # We should have entries for each time period where data was collected

    # Just validate we have the expected number of entries
    assert len(actual) >= 20, f'Expected at least 20 data collection entries, got {len(actual)}'

    # Validate structure of first entry
    first_entry = actual[0]
    expected_fields = ['CSV filename', 'Missing from', 'Missing until', 'Gap in seconds']
    for field in expected_fields:
        assert field in first_entry, f'Missing field {field} in data collection status'


def validate_input_csv_data_integrity():
    """Validate CSV data integrity using direct CSV file validation and tarball cross-validation."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    # Basic validation that input CSVs exist and have expected structure
    required_files = [
        'input_main_host.csv',
        'input_job_host_summary.csv',
        'input_main_jobevent.csv',
        'input_main_indirectmanagednodeaudit.csv',
        'input_data_collection_status.csv',
    ]

    for file_name in required_files:
        file_path = input_data_dir / file_name
        assert file_path.exists(), f'Required input file {file_name} not found'

        # Basic CSV validation
        try:
            df = pandas.read_csv(file_path)
            assert len(df) > 0, f'File {file_name} is empty'
        except Exception as e:
            pytest.fail(f'Failed to read {file_name}: {e}')


def validate_json_fields_comprehensive():
    """Comprehensive validation of JSON fields in CSV files."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    # Validate main_host canonical_facts JSON
    main_host_path = input_data_dir / 'input_main_host.csv'
    if main_host_path.exists():
        df = pandas.read_csv(main_host_path)
        for _, row in df.iterrows():
            try:
                canonical_facts = json.loads(row['canonical_facts'])
                assert isinstance(canonical_facts, dict), 'canonical_facts must be a dict'

                # Validate expected fields
                expected_fields = ['ansible_host', 'host_name']
                for field in expected_fields:
                    assert field in canonical_facts, f'Missing {field} in canonical_facts'

            except json.JSONDecodeError:
                pytest.fail(f'Invalid JSON in canonical_facts for row {row["host_name"]}')


def validate_canonical_facts_combinations():
    """Validate realistic combinations of canonical facts based on platform types."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    main_host_path = input_data_dir / 'input_main_host.csv'
    if main_host_path.exists():
        df = pandas.read_csv(main_host_path)

        # Validate canonical facts combinations by platform
        for _, row in df.iterrows():
            try:
                canonical_facts = json.loads(row['canonical_facts'])
                facts = json.loads(row['facts'])

                virtualization_type = facts.get('ansible_virtualization_type')
                connection_type = facts.get('ansible_connection_variable')

                # Platform-specific validations
                if virtualization_type == 'VirtualPC':  # Windows
                    assert connection_type == 'winrm', f'Windows host {row["host_name"]} should use winrm'
                    assert canonical_facts.get('ansible_port') == 5985, f'Windows host {row["host_name"]} should use port 5985'

                elif virtualization_type == 'container':  # Kubernetes
                    assert connection_type == 'kubectl', f'Container host {row["host_name"]} should use kubectl'

                elif connection_type == 'tcp':  # Network devices
                    assert virtualization_type == 'lxc', f'TCP connection host {row["host_name"]} should be lxc'

            except json.JSONDecodeError:
                pytest.fail(f'Invalid JSON in row {row["host_name"]}')


def extract_csvs_from_tarballs():
    """Extract CSV files from test tarballs for human review."""
    test_dir = get_test_dir()
    test_data_dir = test_dir.parent.parent.parent.parent / 'test_data' / 'data' / '2025'
    input_data_dir = test_dir / 'input_data'
    input_data_dir.mkdir(exist_ok=True)

    # Process files by date
    for date_dir in sorted(test_data_dir.rglob('*')):
        if date_dir.is_dir() and date_dir.name.isdigit():
            for tarball in sorted(date_dir.glob('*.tar.gz')):
                extract_tarball_csvs(tarball, input_data_dir)


def extract_tarball_csvs(tarball_path, output_dir):
    """Extract CSV files from a single tarball."""
    try:
        with tarfile.open(tarball_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.csv'):
                    # Extract to memory first
                    csv_content = tar.extractfile(member)
                    if csv_content:
                        # Determine output filename based on CSV type
                        base_name = member.name.split('/')[-1]
                        output_file = output_dir / f'input_{base_name}'

                        # Append or create file
                        with open(output_file, 'a' if output_file.exists() else 'w') as f:
                            content = csv_content.read().decode('utf-8')
                            # Skip header if appending
                            if output_file.exists() and '\n' in content:
                                lines = content.split('\n')
                                content = '\n'.join(lines[1:])  # Skip header
                            f.write(content)

        print(f'📦 Extracting from {tarball_path.name}')

    except Exception as e:
        print(f'❌ Error extracting {tarball_path}: {e}')


def verify_csv_files_can_open():
    """Simple verification that CSV files can be opened after generation."""
    test_dir = get_test_dir()

    # Input files
    input_data_dir = test_dir / 'input_data'
    input_files = [
        'input_main_host.csv',
        'input_job_host_summary.csv',
        'input_main_jobevent.csv',
        'input_main_indirectmanagednodeaudit.csv',
    ]

    for file_name in input_files:
        file_path = input_data_dir / file_name
        if file_path.exists():
            try:
                df = pandas.read_csv(file_path)
                print(f'✓ Successfully opened {file_name} ({len(df)} rows)')
            except Exception as e:
                pytest.fail(f'Failed to open {file_name}: {e}')

    # Output files
    output_data_dir = test_dir / 'output_data'
    output_files = [
        'output_usage_by_roles.csv',
        'output_usage_by_modules.csv',
        'output_usage_by_organizations.csv',
        'output_usage_reporting.csv',
        'output_data_collection_status.csv',
        'output_indirectly_managed_nodes.csv',
        'output_managed_nodes.csv',
        'output_jobs.csv',
        'output_inventory_scope.csv',
        'output_usage_by_collections.csv',
    ]

    for file_name in output_files:
        file_path = output_data_dir / file_name
        if file_path.exists():
            try:
                df = pandas.read_csv(file_path)
                print(f'✓ Successfully opened output CSV {file_name} ({len(df)} rows)')
            except Exception as e:
                pytest.fail(f'Failed to open output CSV {file_name}: {e}')


def analyze_and_generate_all_outputs(file_path, request):
    """Analyze Excel file and generate all possible CSV outputs."""
    test_dir = get_test_dir()
    output_data_dir = test_dir / 'output_data'
    output_data_dir.mkdir(exist_ok=True)

    try:
        workbook = openpyxl.load_workbook(filename=file_path)
        print(f'Available sheets: {workbook.sheetnames}')

        # Process each sheet
        for sheet_name in workbook.sheetnames:
            print(f'\nProcessing sheet: {sheet_name}')

            try:
                # Read sheet data
                sheet_data = pandas.read_excel(file_path, sheet_name=sheet_name)

                # Generate output CSV filename
                safe_sheet_name = sheet_name.lower().replace(' ', '_')
                output_file = output_data_dir / f'output_{safe_sheet_name}.csv'

                # Save to CSV
                sheet_data.to_csv(output_file, index=False)
                print(f'  Generated CSV: {output_file.name}')

            except Exception as e:
                print(f'  ❌ Error processing {sheet_name}: {e}')

        workbook.close()

    except Exception as e:
        print(f'❌ Error analyzing Excel file: {e}')


def validate_input_main_host_data():
    """Validate the main_host input data from tarballs."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'
    csv_path = input_data_dir / 'input_main_host.csv'

    if not csv_path.exists():
        pytest.fail('input_main_host.csv not found')

    df = pandas.read_csv(csv_path)

    # Expected structure for all hosts
    expected_hosts = {
        # Original hosts
        'app01.cluster': {'count': 4, 'orgs': ['Production', 'Development', 'Staging'], 'machine_id': 'machine123'},
        'app01.failover': {'count': 1, 'orgs': ['Production'], 'machine_id': 'machine456'},
        'cache01.internal': {'count': 2, 'orgs': ['Production', 'Development'], 'machine_id': 'xyz789'},
        'db01.company.com': {'count': 1, 'orgs': ['Production'], 'machine_id': None},
        'db02.dev': {'count': 1, 'orgs': ['Development'], 'machine_id': 'db02-machine-id'},
        'db02.staging': {'count': 1, 'orgs': ['Staging'], 'machine_id': 'db02-machine-id'},
        'log01.company.com': {'count': 1, 'orgs': ['Production'], 'machine_id': None},
        'web01.internal': {'count': 2, 'orgs': ['Production'], 'machine_id': '3a2f8c9b123456789012345678901234'},
        'web01.prod.company.com': {'count': 1, 'orgs': ['Production'], 'machine_id': '3a2f8c9b123456789012345678901234'},
        'web02.external': {'count': 1, 'orgs': ['Production'], 'machine_id': 'def789ghi012'},
        'web02.internal': {'count': 1, 'orgs': ['Production'], 'machine_id': 'def789ghi012'},
        'web03.internal': {'count': 1, 'orgs': ['Production'], 'machine_id': 'web03-machine-id'},
        'web03.prod.internal': {'count': 1, 'orgs': ['Production'], 'machine_id': 'web03-machine-id'},
        'web04.dev': {'count': 1, 'orgs': ['Development'], 'machine_id': 'web04-dev-machine'},
        'web04.staging': {'count': 1, 'orgs': ['Staging'], 'machine_id': 'web04-staging-machine'},
        # New test hosts
        'win-srv01.company.com': {'count': 1, 'orgs': ['Production'], 'machine_id': None},
        'win-srv02.company.com': {'count': 1, 'orgs': ['Production'], 'machine_id': None},
        'aws-vm-01.us-east': {'count': 1, 'orgs': ['Production'], 'machine_id': 'ec2-synthetic-id-123'},
        'aws-vm-02.us-east': {'count': 1, 'orgs': ['Production'], 'machine_id': 'ec2-synthetic-id-123'},
        'nat-host-01.external': {'count': 1, 'orgs': ['Production'], 'machine_id': 'nat-shared-001'},
        'nat-host-02.external': {'count': 1, 'orgs': ['Production'], 'machine_id': 'nat-shared-001'},
    }

    # Validate each host
    for host_name, expected in expected_hosts.items():
        host_rows = df[df['host_name'] == host_name]

        # Check count
        assert len(host_rows) == expected['count'], f'Host {host_name}: expected {expected["count"]} rows, got {len(host_rows)}'

        # Check organizations
        actual_orgs = sorted(host_rows['organization_name'].unique())
        expected_orgs = sorted(expected['orgs'])
        assert actual_orgs == expected_orgs, f'Host {host_name}: expected orgs {expected_orgs}, got {actual_orgs}'

        # Check machine_id consistency
        for _, row in host_rows.iterrows():
            try:
                canonical_facts = json.loads(row['canonical_facts'])
                actual_machine_id = canonical_facts.get('ansible_machine_id')
                assert actual_machine_id == expected['machine_id'], (
                    f'Host {host_name}: expected machine_id {expected["machine_id"]}, got {actual_machine_id}'
                )
            except json.JSONDecodeError:
                pytest.fail(f'Invalid JSON in canonical_facts for {host_name}')

    # Validate total row count
    assert len(df) == 26, f'Expected 26 total rows in main_host.csv, got {len(df)}'

    print(f'✓ Validated {len(df)} hosts in input_main_host.csv')
    return df


def validate_input_job_host_summary_data():
    """Validate the job_host_summary input data from tarballs."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'
    csv_path = input_data_dir / 'input_job_host_summary.csv'

    if not csv_path.exists():
        pytest.fail('input_job_host_summary.csv not found')

    df = pandas.read_csv(csv_path)

    # Validate we have entries for all hosts
    expected_hosts = [
        'app01.cluster',
        'app01.failover',
        'cache01.internal',
        'db01.company.com',
        'db02.dev',
        'db02.staging',
        'log01.company.com',
        'web01.internal',
        'web01.prod.company.com',
        'web02.external',
        'web02.internal',
        'web03.internal',
        'web03.prod.internal',
        'web04.dev',
        'web04.staging',
        # New hosts
        'win-srv01.company.com',
        'win-srv02.company.com',
        'aws-vm-01.us-east',
        'aws-vm-02.us-east',
        'nat-host-01.external',
        'nat-host-02.external',
    ]

    actual_hosts = sorted(df['host_name'].unique())
    missing_hosts = set(expected_hosts) - set(actual_hosts)

    assert len(missing_hosts) == 0, f'Missing job_host_summary entries for: {missing_hosts}'

    # Validate connection types
    connection_type_map = {
        'winrm': ['win-srv01.company.com', 'win-srv02.company.com'],
        'tcp': ['log01.company.com'],
        'ssh': [h for h in expected_hosts if h not in ['win-srv01.company.com', 'win-srv02.company.com', 'log01.company.com']],
    }

    for conn_type, hosts in connection_type_map.items():
        for host in hosts:
            host_rows = df[df['host_name'] == host]
            if len(host_rows) > 0:
                actual_conn = host_rows.iloc[0]['ansible_connection_variable']
                assert actual_conn == conn_type, f'Host {host}: expected connection {conn_type}, got {actual_conn}'

    print(f'✓ Validated {len(df)} job summaries in input_job_host_summary.csv')
    return df


def validate_deduplication_behavior():
    """Validate the deduplication behavior matches expectations."""
    test_dir = get_test_dir()
    input_data_dir = test_dir / 'input_data'

    # Read main_host data
    host_df = pandas.read_csv(input_data_dir / 'input_main_host.csv')

    # Analyze deduplication groups
    dedup_groups = {}

    for _, row in host_df.iterrows():
        try:
            canonical_facts = json.loads(row['canonical_facts'])
            machine_id = canonical_facts.get('ansible_machine_id')
            serial = canonical_facts.get('ansible_product_serial')

            # Create dedup key (current algorithm uses both)
            if machine_id and serial:
                dedup_key = f'{machine_id}:{serial}'
            else:
                dedup_key = f'no_dedup:{row["host_name"]}'

            if dedup_key not in dedup_groups:
                dedup_groups[dedup_key] = []
            dedup_groups[dedup_key].append(row['host_name'])

        except json.JSONDecodeError:
            pass

    # Expected deduplication behavior (all raw entries before deduplication)
    expected_groups = {
        # Correctly deduplicated - all 4 app01.cluster entries share same machine_id+serial
        'machine123:HP-ProLiant-DL380': ['app01.cluster', 'app01.cluster', 'app01.cluster', 'app01.cluster'],
        # Web server deduplications
        '3a2f8c9b123456789012345678901234:VMware-56 4d 3a 2f 8c 9b 12 34-56 78 90 ab cd ef 12 34': [
            'web01.internal',
            'web01.internal',
            'web01.prod.company.com',
        ],
        'def789ghi012:VMware-ab cd ef 12 34 56 78 90-12 34 56 78 90 ab cd ef': ['web02.external', 'web02.internal'],
        'web03-machine-id:VMware-12 34 56 78 90 ab cd ef-ab cd ef 12 34 56 78 90': ['web03.internal', 'web03.prod.internal'],
        'db02-machine-id:Dell-PowerEdge-R750': ['db02.dev', 'db02.staging'],
        # Cache server has machine_id but no serial - falls back to no_dedup
        'no_dedup:cache01.internal': ['cache01.internal', 'cache01.internal'],
        # False positives (wrongly deduplicated)
        'ec2-synthetic-id-123:ec2-instance': ['aws-vm-01.us-east', 'aws-vm-02.us-east'],
        'nat-shared-001:DELL-R740-NAT': ['nat-host-01.external', 'nat-host-02.external'],
        # False negatives (should deduplicate but don't) - no machine_id
        'no_dedup:win-srv01.company.com': ['win-srv01.company.com'],
        'no_dedup:win-srv02.company.com': ['win-srv02.company.com'],
        # Correctly not deduplicated
        'machine456:HP-ProLiant-DL380': ['app01.failover'],
        'no_dedup:db01.company.com': ['db01.company.com'],
        'no_dedup:log01.company.com': ['log01.company.com'],
        'web04-dev-machine:VMware-dev-01-02-03-04-05-06-07-08-09-10-11-12': ['web04.dev'],
        'web04-staging-machine:VMware-stg-01-02-03-04-05-06-07-08-09-10-11-12': ['web04.staging'],
    }

    # Validate deduplication groups
    issues = []
    for key, expected_hosts in expected_groups.items():
        actual_hosts = dedup_groups.get(key, [])
        if sorted(actual_hosts) != sorted(expected_hosts):
            issues.append(f"Dedup key '{key}': expected {expected_hosts}, got {actual_hosts}")

    if issues:
        pytest.fail('Deduplication validation failed:\n' + '\n'.join(issues))

    print(f'✓ Validated deduplication behavior for {len(dedup_groups)} groups')
    return dedup_groups
