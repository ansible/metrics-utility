import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json


class TableMetadataAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - table_metadata collector data
    """

    def __init__(self):
        super().__init__('table_metadata')
        self.collector_names = ['table_metadata']

    def prepare(self, dataframe):
        """
        Transform dataframe to JSON structure with dictionary of statistics prefixed by table name.
        """
        # Convert ID columns to strings at the beginning
        if dataframe is not None and not dataframe.empty:
            dataframe = self._convert_id_columns_to_strings(dataframe)

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            return sanitize_json({})

        # Transform dataframe into dict with table-name-prefixed keys
        tables_data = {}
        for _, row in dataframe.iterrows():
            table_name = row['tablename']
            tables_data[f'{table_name}_estimated_row_count'] = int(row['estimated_row_count'])
            tables_data[f'{table_name}_total_size_bytes'] = int(row['total_size_bytes'])
            tables_data[f'{table_name}_table_size_bytes'] = int(row['table_size_bytes'])
            tables_data[f'{table_name}_indexes_size_bytes'] = int(row['indexes_size_bytes'])

        # Sanitize to convert NumPy types to native Python types for JSON serialization
        return sanitize_json(tables_data)

    def merge(self, data_all, data_new):
        """
        For snapshot collectors, always pick new data (no merging needed).
        """
        return data_new

    def base(self, data):
        """
        Returns the data as-is (data is already computed by prepare).
        Safeguard: if data is a dataframe, call prepare on it first.
        """
        if data is None:
            return {'json': {}}
        if isinstance(data, pd.DataFrame):
            data = self.prepare(data)
        return {'json': data}
