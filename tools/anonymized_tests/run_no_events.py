# This script will accept parameters:
# since, until, output is written into ./out
# chunks - if specified, collectors will be split into chunks (since-until will be split into chunks - multiple collections)
# if chunks not specified, since-until will be split into hourly collections

# The algorithm is as follows:
# For each chunk (chunk has since-until):
#     For each collector:
#         run DB collector
#         pass result as dataframe into rollup (each collector has associated rollup)
#         result from rollup will go into merge function along with global state, which is by default empty
#         global state is updated with the result from merge
#         global state is per rollup type
#
# Then after all rollups are processed (as batches), call base function that
# then it goes into anonymized rollups where whole data are combined into one
# We are not sending data into segment.com, but rather saving final anonymized rollup into ./out directory directly

# It does not collect events data
# But it does not asserts anything, it only collects, compute rollups
# and store results in output dir - final json


