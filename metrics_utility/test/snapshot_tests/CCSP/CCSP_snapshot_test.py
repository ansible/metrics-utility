import os

import pytest

from metrics_utility.logger import logger
from metrics_utility.test.snapshot_tests import snapshot_utils


@pytest.mark.filterwarnings('ignore::ResourceWarning')
def test_snapshot():
    snapshot_utils.run_and_test_snapshot_definitions('./metrics_utility/test/snapshot_tests/CCSP/data/')

    logger.info('\nNow comparing original CCSPv2 and CCSP reports pairs that should hold the same result:\n')

    # compare test with different params that should hold the same result (except ignored fields)
    for type in ['CCSP', 'CCSPv2']:
        prefix = f'./metrics_utility/test/snapshot_tests/CCSP/data/{type}/'

        path1 = prefix + 'snapshot_def_2024-02-01--2024-02-29.json'
        path2 = prefix + 'snapshot_def_2024-02.json'
        compare_different_reports(path1, path2, type)

        path1 = prefix + 'snapshot_def_2024-03-01--2024-03-31.json'
        path2 = prefix + 'snapshot_def_2024-03.json'
        compare_different_reports(path1, path2, type)


def compare_different_reports(path1, path2, type):
    logger.info(f'\nComparing different reports in {path1} and {path2}')

    json1 = snapshot_utils.parse_json_file(path1)
    json2 = snapshot_utils.parse_json_file(path2)

    report1 = snapshot_utils.run_snapshot_definition(json1)
    report2 = snapshot_utils.run_snapshot_definition(json2)

    if type == 'CCSP':
        snapshot_utils.compare_ccsp_reports(report1, report2)

    if type == 'CCSPv2':
        snapshot_utils.compare_ccspv2_reports(report1, report2)

    if os.path.exists(report1):
        os.remove(report1)

    if os.path.exists(report2):
        os.remove(report2)
