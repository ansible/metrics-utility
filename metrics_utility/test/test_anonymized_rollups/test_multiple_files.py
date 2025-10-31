# This test should test data split into multiple tarballs (each tarball contains one csv data file)
# I need to make sure that the logic for concatenating the dataframes is working correctly
# This will work very similar to test_from_gather_to_json.py, but without the gathering data from database
# as input data, use the data from other test files like
# test_jobs_anonymized_rollups.py, test_events_modules_anonymized_rollups.py, test_execution_environments_anonymized_rollups.py
# test_job_host_summary_anonymized_rollups.py

# those tests have their data in list of dict, so you can import them, split to 2-3 parts, store them into tarball packed csv


