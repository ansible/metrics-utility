from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup


class TableMetadataAnonymizedRollup(BaseAnonymizedRollup):
    """
    Collector - table_metadata collector data
    """

    def __init__(self):
        super().__init__('table_metadata')
        self.collector_names = ['table_metadata']

    # Prepare and merge just simply pick the latest value, its snapshot collector
    def prepare(self, dataframe):
        return dataframe

    def merge(self, dataframe_all, dataframe_new):
        return dataframe_new

    def base(self, dataframe):
        """
        Table metadata statistics:
        - Total estimated row count across all tables
        - Total size bytes across all tables
        - Per-table statistics (row count, sizes)
        """

        # Handle None or empty dataframe
        if dataframe is None or dataframe.empty:
            return {
                'json': {},
            }

        # Compute totals across all tables
        total_estimated_row_count = int(dataframe['estimated_row_count'].sum())
        total_size_bytes = int(dataframe['total_size_bytes'].sum())
        total_table_size_bytes = int(dataframe['table_size_bytes'].sum())
        total_indexes_size_bytes = int(dataframe['indexes_size_bytes'].sum())

        # Per-table statistics
        tables_data = []
        for _, row in dataframe.iterrows():
            tables_data.append({
                'tablename': row['tablename'],
                'estimated_row_count': int(row['estimated_row_count']),
                'total_size_bytes': int(row['total_size_bytes']),
                'table_size_bytes': int(row['table_size_bytes']),
                'indexes_size_bytes': int(row['indexes_size_bytes']),
            })

        # Prepare JSON data
        json_data = {
            'total_estimated_row_count': total_estimated_row_count,
            'total_size_bytes': total_size_bytes,
            'total_table_size_bytes': total_table_size_bytes,
            'total_indexes_size_bytes': total_indexes_size_bytes,
            'tables': tables_data,
        }

        return {
            'json': json_data,
        }
