import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class CredentialsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - credentials_service collector data
    """

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

    def prepare(self, dataframe):
        """
        Batch processing that counts occurrences of each credential type in this batch.
        Returns a dataframe with one row per credential type and its count.
        """
        if dataframe.empty:
            # Return empty DataFrame with required columns
            return pd.DataFrame(columns=['credential_type', 'count'])

        # Check if credential_type column exists (required for processing)
        if 'credential_type' not in dataframe.columns:
            # If credential_type is missing, return empty DataFrame with required columns
            return pd.DataFrame(columns=['credential_type', 'count'])

        # Count occurrences of each credential type in this batch
        aggregated = (
            dataframe.groupby('credential_type', as_index=False)
            .agg(count=('credential_type', 'count'))
        )

        return aggregated

    def base(self, data):
        """
        Sum credential type counts across all batches.
        
        data is a dataframe with columns:
        - credential_type: name of the credential type
        - count: count of occurrences
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': {},
                'rollup': {'aggregated': pd.DataFrame()},
            }

        # Return empty result if dataframe is empty
        if data.empty:
            return {
                'json': {},
                'rollup': {'aggregated': pd.DataFrame(columns=['credential_type', 'count'])},
            }

        # Ensure required columns exist
        if 'credential_type' not in data.columns or 'count' not in data.columns:
            return {
                'json': {},
                'rollup': {'aggregated': pd.DataFrame(columns=['credential_type', 'count'])},
            }

        # Group by credential_type and sum the counts
        aggregated = (
            data.groupby('credential_type', as_index=False)
            .agg({'count': 'sum'})
        )

        # Convert to dictionary with credential_type_ prefix and _total suffix
        result_dict = {}
        for _, row in aggregated.iterrows():
            cred_type = row['credential_type']
            count = int(row['count'])
            # Convert credential type name to a valid field name
            field_name = str(cred_type).lower().replace(' ', '_').replace('-', '_')
            result_dict[f'credential_type_{field_name}_total'] = count

        # Prepare rollup data (dataframe before conversion)
        rollup_data = {
            'aggregated': aggregated,
        }

        # Prepare JSON data (dictionary of credential type counts)
        json_data = result_dict

        return {
            'json': json_data,
            'rollup': rollup_data,
        }
