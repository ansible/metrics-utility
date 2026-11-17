#!/usr/bin/env python3
"""
Run performance tests on all three dataset sizes (small, medium, large).

This script automates the complete testing workflow:
1. Small dataset (~100K events)
2. Medium dataset (~1M events)
3. Large dataset (~10M events)

For each dataset size:
- Cleans existing data
- Generates test data
- Runs individual task tests
- Runs full service test
- Saves results to separate output directories

Usage:
    # Run all dataset sizes
    python run_all_dataset_sizes.py

    # Run only specific sizes
    python run_all_dataset_sizes.py --only small
    python run_all_dataset_sizes.py --only medium
    python run_all_dataset_sizes.py --only large
    python run_all_dataset_sizes.py --only small,medium
"""

import argparse
import shutil
import subprocess
import sys
import time

from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent


# Dataset configurations
DATASETS = [
    {
        'name': 'small',
        'job_count': 20,
        'host_count': 100,
        'task_count': 50,
        'indirect_count': 1000,
        'target_events': '~100K',
        'test_since': '2024-01-01',
        'test_until': '2024-02-01',
    },
    {
        'name': 'medium',
        'job_count': 20,
        'host_count': 1000,
        'task_count': 50,
        'indirect_count': 100000,
        'target_events': '~1M',
        'test_since': '2024-01-01',
        'test_until': '2024-02-01',
    },
    {
        'name': 'large',
        'job_count': 6900,
        'host_count': 869,
        'task_count': 50,
        'indirect_count': 1000000,
        'target_events': '~300M total (~10M/day)',
        'test_since': '2024-01-15',
        'test_until': '2024-01-16',
    },
]


def get_python_executable():
    """Return the repo venv interpreter when available, otherwise the current interpreter."""
    venv_python = REPO_ROOT / '.venv' / 'bin' / 'python'
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def run_command(cmd, description):
    """Run a command (argv list) and return success status."""
    print(f'\n  → {description}...')
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f'    ✗ Failed with exit code {result.returncode}')
        return False
    print('    ✓ Done')
    return True


def copy_results(source_dir, dest_dir, description):
    """Copy a results directory and return success status."""
    print(f'\n  → {description}...')
    try:
        shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
    except OSError as e:
        print(f'    ✗ Failed: {e}')
        return False
    print('    ✓ Done')
    return True


def run_dataset(config, index, total):
    """Run tests for a single dataset size."""
    name = config['name']
    print(f'\n{"=" * 60}')
    print(f'Testing {name.upper()} dataset ({index}/{total})')
    print(f'Target: {config["target_events"]} events')
    print(f'{"=" * 60}')

    start_time = time.time()
    python_exe = get_python_executable()

    # Step 1: Clean database
    clean_cmd = [python_exe, str(SCRIPT_DIR / 'clean_all_data.py'), '--force']
    if not run_command(clean_cmd, 'Cleaning database'):
        return {'success': False, 'elapsed': time.time() - start_time}

    # Step 2: Generate test data
    fill_cmd = [
        python_exe,
        str(SCRIPT_DIR / 'fill_perf_db_data.py'),
        f'--job-count={config["job_count"]}',
        f'--host-count={config["host_count"]}',
        f'--task-count={config["task_count"]}',
        f'--indirect-count={config["indirect_count"]}',
        f'--since={config["test_since"]}',
        f'--until={config["test_until"]}',
    ]
    if not run_command(fill_cmd, f'Generating {name} dataset'):
        return {'success': False, 'elapsed': time.time() - start_time}

    # Step 3: Run individual task tests
    test_cmd = [
        python_exe,
        str(SCRIPT_DIR / 'rollup_performance_test.py'),
        f'--since={config["test_since"]}',
        f'--until={config["test_until"]}',
    ]
    if not run_command(test_cmd, 'Running individual task tests'):
        return {'success': False, 'elapsed': time.time() - start_time}

    # Step 4: Run full service test
    perf_cmd = [
        python_exe,
        str(SCRIPT_DIR / 'run_perf.py'),
        f'--since={config["test_since"]}',
        f'--until={config["test_until"]}',
    ]
    if not run_command(perf_cmd, 'Running full service test'):
        return {'success': False, 'elapsed': time.time() - start_time}

    # Step 5: Save results to dataset-specific directory
    output_dir = SCRIPT_DIR / f'out_{name}'
    if not copy_results(SCRIPT_DIR / 'out', output_dir, f'Saving results to out_{name}'):
        return {'success': False, 'elapsed': time.time() - start_time}

    elapsed = time.time() - start_time
    print(f'\n✓ {name.upper()} dataset tests completed in {elapsed / 60:.1f} minutes')
    return {'success': True, 'elapsed': elapsed}


def main():
    """Run all dataset tests."""
    parser = argparse.ArgumentParser(description='Run performance tests on anonymized rollups')
    parser.add_argument('--only', type=str, help='Run only specific dataset sizes (comma-separated). Options: small, medium, large')
    args = parser.parse_args()

    # Filter datasets if --only is specified. Select by index into the constant
    # DATASETS list so the CLI value is only used for filtering and never flows
    # into generated paths or commands.
    datasets_to_test = DATASETS
    if args.only:
        selected = {s.strip().lower() for s in args.only.split(',')}
        selected_indexes = [i for i, ds in enumerate(DATASETS) if ds['name'] in selected]
        datasets_to_test = [DATASETS[i] for i in selected_indexes]

        if not datasets_to_test:
            print('Error: No valid dataset sizes specified. Options: small, medium, large')
            return 1

        invalid = sorted(selected - {ds['name'] for ds in DATASETS})
        if invalid:
            print(f'Warning: Invalid dataset size(s) ignored: {", ".join(invalid)}')

    print('\n' + '=' * 60)
    print('ANONYMIZED ROLLUP PERFORMANCE TESTS')
    print('=' * 60)
    print(f'Testing {len(datasets_to_test)} dataset size(s):')
    for ds in datasets_to_test:
        print(f'  - {ds["name"].upper()}: {ds["target_events"]} events')

    time_estimates = {1: '1-5 minutes', 2: '5-10 minutes', 3: '10-15 minutes'}
    print(f'\nThis will take approximately {time_estimates.get(len(datasets_to_test), "10-15 minutes")}.')
    print('=' * 60)

    start_time = time.time()
    test_start_datetime = datetime.now()
    results = []

    # Run tests for each dataset
    for i, dataset in enumerate(datasets_to_test, 1):
        result = run_dataset(dataset, i, len(datasets_to_test))
        results.append({'name': dataset['name'], 'config': dataset, 'success': result['success'], 'elapsed': result['elapsed']})

        if not result['success']:
            print(f'\n✗ {dataset["name"].upper()} dataset tests failed!')
            print('Continuing with remaining datasets...\n')

    # Print summary
    total_time = time.time() - start_time
    print('\n' + '=' * 60)
    print('TEST SUMMARY')
    print('=' * 60)

    for result in results:
        status = '✓ PASS' if result['success'] else '✗ FAIL'
        print(f'{result["name"].upper():<10} {status}')

    print(f'\nTotal time: {total_time / 60:.1f} minutes')

    successful = sum(1 for r in results if r['success'])
    print(f'Results: {successful}/{len(datasets_to_test)} datasets tested successfully')

    # Generate consolidated markdown report
    test_date = test_start_datetime.strftime('%Y-%m-%d')
    md_file = Path(__file__).parent / f'perf_test_summary_{test_date}.md'
    with open(md_file, 'w') as f:
        f.write('# Performance Test Summary - All Dataset Sizes\n\n')
        f.write(f'**Test Date:** {test_start_datetime.strftime("%Y-%m-%d %H:%M:%S")}\n\n')

        f.write('## Test Overview\n\n')
        f.write(f'- **Total Datasets Tested:** {len(datasets_to_test)}\n')
        f.write(f'- **Successful:** {successful}\n')
        f.write(f'- **Failed:** {len(datasets_to_test) - successful}\n')
        f.write(f'- **Total Time:** {total_time / 60:.2f} minutes\n\n')

        f.write('## Dataset Results\n\n')
        f.write('| Dataset | Target Events | Jobs | Hosts | Tasks | Test Period | Duration | Status |\n')
        f.write('|---------|---------------|------|-------|-------|-------------|----------|--------|\n')

        for result in results:
            cfg = result['config']
            status_emoji = '✓' if result['success'] else '✗'
            duration_min = result['elapsed'] / 60
            f.write(
                f'| {cfg["name"].capitalize()} | {cfg["target_events"]} | '
                f'{cfg["job_count"]} | {cfg["host_count"]} | {cfg["task_count"]} | '
                f'{cfg["test_since"]} to {cfg["test_until"]} | '
                f'{duration_min:.2f} min | {status_emoji} |\n'
            )

        f.write('\n## Detailed Results\n\n')
        f.write('For detailed performance metrics, see the individual test reports:\n\n')
        for result in results:
            if result['success']:
                name = result['name']
                f.write(f'### {name.capitalize()} Dataset\n\n')
                f.write(f'- **Individual Tasks:** `out_{name}/perf_test_individual_tasks_{test_date}.md`\n')
                f.write(f'- **Full Service:** `out_{name}/perf_test_full_service_{test_date}.md`\n')
                f.write(f'- **Output Directory:** `out_{name}/`\n\n')

        if successful < len(datasets_to_test):
            f.write('\n## Failed Tests\n\n')
            for result in results:
                if not result['success']:
                    f.write(f'- **{result["name"].capitalize()}:** Test failed after {result["elapsed"] / 60:.2f} minutes\n')

    if successful == len(datasets_to_test):
        print('\n✓ All tests completed successfully!')
        print('\nResults saved to:')
        for r in results:
            print(f'  - out_{r["name"]}/')
        print(f'\nConsolidated report: {md_file}')
        return 0
    else:
        print('\n✗ Some tests failed. Check output above for details.')
        print(f'\nConsolidated report: {md_file}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
