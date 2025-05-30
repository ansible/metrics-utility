from metrics_utility import prepare


# this updates python paths to include mock_awx/ (and not fail to find awx imports)
# has to happen before any tests that import code that imports awx are imported
prepare()
