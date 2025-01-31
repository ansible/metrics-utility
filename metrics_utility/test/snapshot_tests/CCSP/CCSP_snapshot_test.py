from .. import snapshot_utils
import pytest

@pytest.mark.filterwarnings("ignore::ResourceWarning")
def test_snapshot():    
    snapshot_utils.run_and_test_snapshot_definitions('./metrics_utility/test/snapshot_tests/CCSP/data/')

    # compare test with different params that should hold the same result (except ignored fields)
    
    # CCSPv2
    print('\nNow comparing original CCSPv2 reports pairs that should hold the same result:\n')
   
    prefix = './metrics_utility/test/snapshot_tests/CCSP/data/CCSPv2/'
    report1 = prefix + 'special_snapshot_def_2024-02-01--2024-02-29/report.xlsx'
    report2 = prefix + 'special_snapshot_def_2024-02/report.xlsx'
    snapshot_utils.compare_CCSPv2_reports(report1, report2)
    print('')

    report1 = prefix + 'special_snapshot_def_2024-03-01--2024-03-31/report.xlsx'
    report2 = prefix + 'special_snapshot_def_2024-03/report.xlsx'
    snapshot_utils.compare_CCSPv2_reports(report1, report2)

    # CCSP
    print('\nNow comparing original CCSP reports pairs that should hold the same result:\n')
   
    prefix = './metrics_utility/test/snapshot_tests/CCSP/data/CCSP/'
    report1 = prefix + 'special_snapshot_def_2024-02-01--2024-02-29/report.xlsx'
    report2 = prefix + 'special_snapshot_def_2024-02/report.xlsx'
    snapshot_utils.compare_CCSP_reports(report1, report2)
    print('')

    report1 = prefix + 'special_snapshot_def_2024-03-01--2024-03-31/report.xlsx'
    report2 = prefix + 'special_snapshot_def_2024-03/report.xlsx'
    snapshot_utils.compare_CCSP_reports(report1, report2)
    
    print('Test finished!')
