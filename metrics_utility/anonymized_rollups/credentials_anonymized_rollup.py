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
        Batch processing that extracts unique credential types in this batch.
        Returns a dataframe with one row per unique credential type.
        """
        if dataframe.empty:
            # Return empty DataFrame with required columns
            return pd.DataFrame(columns=['credential_type'])

        # Check if credential_type column exists (required for processing)
        if 'credential_type' not in dataframe.columns:
            # If credential_type is missing, return empty DataFrame with required columns
            return pd.DataFrame(columns=['credential_type'])

        # Get unique credential types in this batch
        unique_credential_types = dataframe['credential_type'].dropna().unique()
        aggregated = pd.DataFrame({'credential_type': unique_credential_types})

        return aggregated

    def base(self, data):
        """
        Get unique credential types across all batches.

        data is a dataframe with columns:
        - credential_type: name of the credential type
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': [],
            }

        # Return empty result if dataframe is empty
        if data.empty:
            return {
                'json': [],
            }

        # Ensure required columns exist
        if 'credential_type' not in data.columns:
            return {
                'json': [],
            }

        # Get unique credential types across all batches
        unique_credential_types = data['credential_type'].dropna().unique()
        # Convert to sorted list
        credential_types_list = sorted([str(ct) for ct in unique_credential_types])

        return {
            'json': credential_types_list,
           
        }
