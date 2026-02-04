import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class CredentialsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - credentials_service collector data
    """

    # List of credential types to count
    CREDENTIAL_TYPES = [
        'Machine',
        'Source Control',
        'Vault',
        'Network',
        'Amazon Web Services',
        'Microsoft Azure Resource Manager',
        'Google Compute Engine',
        'OpenStack',
        'VMware vCenter',
        'Container Registry',
        'Red Hat Ansible Automation Platform Subscription',
        'Insights',
        'GitHub Token',
        'GitLab Token',
        'CyberArk AIM',
        'HashiCorp Vault',
    ]

    def __init__(self):
        super().__init__('credentials')
        self.collector_names = ['credentials_service']

    # prepare is called for each batch of data
    # result of prepare is concatenated with other batches into one dataframe
    # each dataframe in prepare should reduce the number of rows as much as possible
    # dataframe has:
    # credential_type - name of the credential type
    # job_id - id of the job
    # model - job model (job_type)

    def _make_count_func(self, cred_type):
        """Create a function that counts occurrences of a specific credential type"""
        def count_func(x):
            return (x == cred_type).sum()
        return count_func

    def prepare(self, dataframe):
        """
        Batch processing that reduces each dataframe batch and computes number of distinct
        credential types for loaded jobs joined with credentials grouped by job model.
        Each job model should hold counts of distinct credential types and counts per credential type.
        """
        if dataframe.empty:
            # Return empty DataFrame with all required columns to ensure consistency
            columns = ['job_type', 'credential_types'] + [f'{cred_type}_count' for cred_type in self.CREDENTIAL_TYPES]
            return pd.DataFrame(columns=columns)

        # Check if model column exists (for backward compatibility)
        if 'model' not in dataframe.columns:
            # If model is missing, create a default 'unknown' value
            dataframe['model'] = 'unknown'

        # Group by model and aggregate:
        # 1. Collect distinct credential types as a set
        # 2. Count occurrences of each credential type
        agg_dict = {
            'credential_types': ('credential_type', lambda x: set(x.dropna())),
        }

        # Add count for each credential type
        for cred_type in self.CREDENTIAL_TYPES:
            # Count occurrences of this credential type
            agg_dict[f'{cred_type}_count'] = (
                'credential_type',
                self._make_count_func(cred_type)
            )

        aggregated = (
            dataframe.groupby('model')
            .agg(agg_dict)
            .reset_index()
            .rename(columns={'model': 'job_type'})
        )

        return aggregated

    def base(self, data):
        """
        Compute final count of distinct credential types per job model and counts per credential type.
        
        data is a dataframe with columns:
        - job_type (model)
        - credential_types (set of distinct credential types)
        - {credential_type}_count (count for each credential type)
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {
                    'by_job_type': [],
                },
                'rollup': {'aggregated': pd.DataFrame()},
            }

        # Return empty result if dataframe is empty
        if data.empty:
            return {
                'json': {
                    'by_job_type': [],
                },
                'rollup': {'aggregated': data},
            }

        # Group by job_type and union credential_types sets, and sum counts for each credential type
        def union_credential_types(series):
            """Union all sets in the series"""
            result = set()
            for cred_set in series:
                if isinstance(cred_set, set):
                    result.update(cred_set)
                elif cred_set is not None:
                    result.update(cred_set)
            return result

        # Ensure all credential type count columns exist in data (fill missing with 0)
        for cred_type in self.CREDENTIAL_TYPES:
            count_col = f'{cred_type}_count'
            if count_col not in data.columns:
                data[count_col] = 0

        agg_dict = {
            'credential_types': ('credential_types', union_credential_types),
        }

        # Sum counts for each credential type (all columns should exist now)
        for cred_type in self.CREDENTIAL_TYPES:
            count_col = f'{cred_type}_count'
            agg_dict[count_col] = (count_col, 'sum')

        aggregations_by_job_type = (
            data.groupby('job_type')
            .agg(agg_dict)
            .reset_index()
            .assign(distinct_credential_types_total=lambda x: x['credential_types'].apply(len))
            .drop(columns=['credential_types'])
        )

        # Rename count columns to have _total suffix for consistency
        rename_dict = {}
        for cred_type in self.CREDENTIAL_TYPES:
            count_col = f'{cred_type}_count'
            # Convert credential type name to a valid field name
            field_name = cred_type.lower().replace(' ', '_').replace('-', '_')
            rename_dict[count_col] = f'{field_name}_total'

        if rename_dict:
            aggregations_by_job_type = aggregations_by_job_type.rename(columns=rename_dict)

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            # pandas.DataFrame
            'aggregations_by_job_type': aggregations_by_job_type,
        }

        # Prepare JSON data (converted to list of dicts)
        json_data = {
            'by_job_type': aggregations_by_job_type.to_dict(orient='records'),
        }

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
