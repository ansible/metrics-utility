import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class JobHostSummaryAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - job_host_summary_service collector data
    """

    def __init__(self):
        super().__init__('job_host_summary')
        self.collector_names = ['job_host_summary_service']

    def merge(self, data_all, data_new):
        """
        Override merge to handle the new structure with jobhostsummary_total and aggregated data.
        Concatenates aggregated dataframes and sums jobhostsummary_total.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Concatenate aggregated dataframes and sum jobhostsummary_totals
        return {
            'jobhostsummary_total': data_all['jobhostsummary_total'] + data_new['jobhostsummary_total'],
            'aggregated': pd.concat([data_all['aggregated'], data_new['aggregated']], ignore_index=True),
        }

    # prepare is called for each batch of data
    # result of prepare is concatenated with other batches into one dataframe
    # each dataframe in prepare should reduce the number of rows as much as possible
    # dataframe has:
    # job_remote_id
    # job_template_name
    # host_name
    # dark
    # failures
    # ok
    # skipped
    # ignored
    # rescued

    def prepare(self, dataframe):
        # Count all records before processing
        jobhostsummary_total = len(dataframe)

        # Sum all task columns without grouping to reduce data volume early
        # This significantly improves performance when processing large batches
        if dataframe.empty:
            return {
                'jobhostsummary_total': jobhostsummary_total,
                'aggregated': dataframe,
            }

        # Sum all task columns across all records (no grouping)
        aggregated = pd.DataFrame([{
            'dark_total': dataframe['dark'].sum(),
            'failures_total': dataframe['failures'].sum(),
            'ok_total': dataframe['ok'].sum(),
            'skipped_total': dataframe['skipped'].sum(),
            'ignored_total': dataframe['ignored'].sum(),
            'rescued_total': dataframe['rescued'].sum(),
            # keep unique hosts as set
            'unique_hosts': set(dataframe['host_name']),
        }])

        return {
            'jobhostsummary_total': jobhostsummary_total,
            'aggregated': aggregated,
        }

    def base(self, data):
        """
        Number of tasks executed (sum of all tasks executed in dataframe)
        Success ratio of tasks executed (ratio between ok and failed tasks (and others))

        Success rate and average - this can compute SaaS team from the metrics

        data is a dict with 'jobhostsummary_total' and 'aggregated' dataframe
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {
                    'jobhostsummary_total': 0,
                    'aggregated': {
                        'dark_total': 0,
                        'failures_total': 0,
                        'ok_total': 0,
                        'skipped_total': 0,
                        'ignored_total': 0,
                        'rescued_total': 0,
                    },
                },
                'rollup': {'aggregated': pd.DataFrame(), 'jobhostsummary_total': 0},
            }

        # Extract jobhostsummary_total and aggregated dataframe from the data structure
        jobhostsummary_total = data.get('jobhostsummary_total', 0)
        dataframe = data.get('aggregated', pd.DataFrame())

        # Return empty result if dataframe is empty
        # TODO - ensure all columns are present in the dataframe, then let analysis run with empty data
        if dataframe.empty:
            return {
                'json': {
                    'jobhostsummary_total': jobhostsummary_total,
                    'aggregated': {
                        'dark_total': 0,
                        'failures_total': 0,
                        'ok_total': 0,
                        'skipped_total': 0,
                        'ignored_total': 0,
                        'rescued_total': 0,
                    },
                },
                'rollup': {'aggregated': dataframe, 'jobhostsummary_total': jobhostsummary_total},
            }

        # Union all unique_hosts sets from all batches
        unique_hosts_total = set()
        for hosts_set in dataframe['unique_hosts']:
            if isinstance(hosts_set, set):
                unique_hosts_total.update(hosts_set)
            elif hosts_set is not None:
                # Handle case where it might be stored differently
                unique_hosts_total.update(hosts_set)

        # Sum all totals across all batches (no grouping by template)
        aggregated = pd.DataFrame([{
            'dark_total': dataframe['dark_total'].sum(),
            'failures_total': dataframe['failures_total'].sum(),
            'ok_total': dataframe['ok_total'].sum(),
            'skipped_total': dataframe['skipped_total'].sum(),
            'ignored_total': dataframe['ignored_total'].sum(),
            'rescued_total': dataframe['rescued_total'].sum(),
        }])

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregated': aggregated,
            'jobhostsummary_total': jobhostsummary_total,
        }

        # Prepare JSON data (single dict, not a list)
        # Get the first (and only) row as a dict
        aggregated_dict = aggregated.iloc[0].to_dict()
        json_data = {
            'unique_hosts_total': len(unique_hosts_total),
            'aggregated': aggregated_dict,
            'jobhostsummary_total': jobhostsummary_total,
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
