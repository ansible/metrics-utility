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
import subprocess
import sys
import time

from pathlib import Path


# Dataset configurations
DATASETS = [
    {
        'name': 'small',
        'job_count': 20,
        'host_count': 100,
        'task_count': 50,
        'target_events': '~100K',
        'test_since': '2024-01-01',
        'test_until': '2024-02-01',
    },
    {
        'name': 'medium',
        'job_count': 20,
        'host_count': 1000,
        'task_count': 50,
        'target_events': '~1M',
        'test_since': '2024-01-01',
        'test_until': '2024-02-01',
    },
    {
        'name': 'large',
        'job_count': 6900,
        'host_count': 869,
        'task_count': 50,
        'target_events': '~300M total (~10M/day)',
        'test_since': '2024-01-15',
        'test_until': '2024-01-16',
    },
]


def run_command(cmd, description):
    """Run a command and return success status."""
    print(f'\n  → {description}...')
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f'    ✗ Failed with exit code {result.returncode}')
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

    # Step 1: Clean database
    if not run_command('source .venv/bin/activate && python tools/anonymized_db_perf_data/clean_all_data.py --force', 'Cleaning database'):
        return False

    # Step 2: Generate test data
    cmd = (
        f'source .venv/bin/activate && '
        f'python tools/anonymized_db_perf_data/fill_perf_db_data.py '
        f'--job-count={config["job_count"]} '
        f'--host-count={config["host_count"]} '
        f'--task-count={config["task_count"]}'
    )
    if not run_command(cmd, f'Generating {name} dataset'):
        return False

    # Step 3: Run individual task tests
    test_cmd = (
        f'source .venv/bin/activate && '
        f'python tools/anonymized_db_perf_data/rollup_performance_test.py '
        f'--since={config["test_since"]} --until={config["test_until"]}'
    )
    if not run_command(test_cmd, 'Running individual task tests'):
        return False

    # Step 4: Run full service test
    perf_cmd = (
        f'source .venv/bin/activate && python tools/anonymized_db_perf_data/run_perf.py --since={config["test_since"]} --until={config["test_until"]}'
    )
    if not run_command(perf_cmd, 'Running full service test'):
        return False

    # Step 5: Save results to dataset-specific directory
    output_dir = Path(__file__).parent / f'out_{name}'
    if not run_command(f'cp -r tools/anonymized_db_perf_data/out {output_dir}', f'Saving results to out_{name}'):
        return False

    elapsed = time.time() - start_time
    print(f'\n✓ {name.upper()} dataset tests completed in {elapsed / 60:.1f} minutes')
    return True


def main():
    """Run all dataset tests."""
    parser = argparse.ArgumentParser(description='Run performance tests on anonymized rollups')
    parser.add_argument('--only', type=str, help='Run only specific dataset sizes (comma-separated). Options: small, medium, large')
    args = parser.parse_args()

    # Filter datasets if --only is specified
    datasets_to_test = DATASETS
    if args.only:
        selected = [s.strip().lower() for s in args.only.split(',')]
        datasets_to_test = [ds for ds in DATASETS if ds['name'] in selected]

        if not datasets_to_test:
            print('Error: No valid dataset sizes specified. Options: small, medium, large')
            return 1

        invalid = [s for s in selected if s not in ['small', 'medium', 'large']]
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
    results = []

    # Run tests for each dataset
    for i, dataset in enumerate(datasets_to_test, 1):
        success = run_dataset(dataset, i, len(datasets_to_test))
        results.append((dataset['name'], success))

        if not success:
            print(f'\n✗ {dataset["name"].upper()} dataset tests failed!')
            print('Continuing with remaining datasets...\n')

    # Print summary
    total_time = time.time() - start_time
    print('\n' + '=' * 60)
    print('TEST SUMMARY')
    print('=' * 60)

    for name, success in results:
        status = '✓ PASS' if success else '✗ FAIL'
        print(f'{name.upper():<10} {status}')

    print(f'\nTotal time: {total_time / 60:.1f} minutes')

    successful = sum(1 for _, success in results if success)
    print(f'Results: {successful}/{len(datasets_to_test)} datasets tested successfully')

    if successful == len(datasets_to_test):
        print('\n✓ All tests completed successfully!')
        print('\nResults saved to:')
        for ds in datasets_to_test:
            print(f'  - out_{ds["name"]}/')
        return 0
    else:
        print('\n✗ Some tests failed. Check output above for details.')
        return 1


if __name__ == '__main__':
    sys.exit(main())
