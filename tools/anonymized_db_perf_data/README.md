# Anonymized data perf test

3 main files:
`clean_all_data.py` Cleans the database
`fill_perf_db_data.py` Inserts data into database
`run_perf.py` Runs the final perf test using the generated data

Additionaly, added ability to save packed data in tarballs, or save only csv plain files.

Testing
`cd tools/anonymized_db_perf_data`

Your first step, call it for first time and thereafter whenever you want to clean data:
`./clean_all_data.py`

Filling the data (adjust params as needed):
`./fill_perf_db_data.py --host-count 5000 --job-count 100`

Running the perf test using prepared data:
`./run_perf.py`

You can see inside out folder anonymized.json with computed results. And also rollups with some unanonymized computed data.

The fill perf db data can be called multiple times, each time it adds new data.

You can easily run off the disk. For proper large data testing, we need around 50 GB of data (mine laptom dont have so much free space anymore...).
