from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class CredentialsAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - credentials_service collector data
    """

    def __init__(self):
        super().__init__('credentials')
        self.collector_names = ['credentials_service']

    # prepare is called for each batch of data
    # result of prepare is merged with other batches using merge function
    # prepare should return JSON (dict) structure
    # dataframe has:
    # credential_type - name of the credential type
    # job_id - id of the job
    # model - job model (job_type)

    def prepare(self, dataframe):
        """
        Batch processing that extracts unique credential types in this batch.
        Returns a dictionary with a list of unique credential types.
        """
        # Convert ID columns to strings at the beginning
        dataframe = self._convert_id_columns_to_strings(dataframe)

        if dataframe.empty:
            return sanitize_json(
                {
                    'credential_types': [],
                }
            )

        # Check if credential_type column exists (required for processing)
        if 'credential_type' not in dataframe.columns:
            return sanitize_json(
                {
                    'credential_types': [],
                }
            )

        # Get unique credential types in this batch
        unique_credential_types = dataframe['credential_type'].dropna().unique()
        # Convert to sorted list of strings
        credential_types_list = sorted([str(ct) for ct in unique_credential_types])

        result = {
            'credential_types': credential_types_list,
        }

        # Sanitize to convert NumPy types to native Python types for JSON serialization
        return sanitize_json(result)

    def merge(self, data_all, data_new):
        """
        Merge two credential type dictionaries by unioning the credential_types lists.
        """
        # Handle initial None case (first iteration from load_anonymized_rollup_data)
        if data_all is None:
            return data_new

        # Get credential types from both dictionaries
        credential_types_all = set(data_all.get('credential_types', []))
        credential_types_new = set(data_new.get('credential_types', []))

        # Union the sets and convert back to sorted list
        credential_types_merged = sorted(list(credential_types_all.union(credential_types_new)))

        return {
            'credential_types': credential_types_merged,
        }

    def base(self, data):
        """
        Get unique credential types across all batches.

        data is a dictionary with structure:
        - credential_types: list of unique credential type names
        """

        # Handle None input (no data files)
        if data is None:
            return {
                'json': [],
            }

        # Extract credential types from the dictionary
        credential_types = data.get('credential_types', [])

        # Return empty result if no credential types
        if not credential_types:
            return {
                'json': [],
            }

        return {
            'json': credential_types,
        }
