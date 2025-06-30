Test data for `metrics_utility/test/ccspv_reports/test_empty_data_for_CCSP_and_CCSPv2.py`

Separated because the test is testing what happens for various input breakage situations:

* tarballs with no data
* no tarballs
* empty csv files

(Originally added in https://github.com/ansible/metrics-utility/pull/118/ , moved in https://github.com/ansible/metrics-utility/pull/158/ to unbreak the generator, and for when people try running build\_report over these date ranges by accident.)
