from .. import snapshot_utils

def test_snapshot():    
    snapshot_utils.run_and_test_snapshot_definitions('./metrics_utility/test/snapshot_tests/CCSP/data/')
    print('Test finished!')