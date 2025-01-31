from .. import snapshot_utils
import pytest

@pytest.mark.filterwarnings("ignore::ResourceWarning")
def test_snapshot():    
    snapshot_utils.run_and_test_snapshot_definitions('./metrics_utility/test/snapshot_tests/CCSP/data/')

    return

    # compare test with different params that should hold the same result (except ignored fields)
    prefix = './metrics_utility/test/snapshot_tests/CCSP/data/CCSPv2/'
    report1 = prefix + 'snapshot_def_2024-02-01--2024-02-29/report.xlsx'
    report2 = prefix + 'snapshot_def_2024-02/report.xlsx'
    snapshot_utils.compare_CCSPv2_reports(report1, report2)

    report1 = prefix + 'snapshot_def_2024-03-01--2024-03-31/report.xlsx'
    report2 = prefix + 'snapshot_def_2024-03/report.xlsx'
    snapshot_utils.compare_CCSPv2_reports(report1, report2)

    snapshot_utils.compare_CCSPv2_reports(report1, report2)
    print('Test finished!')