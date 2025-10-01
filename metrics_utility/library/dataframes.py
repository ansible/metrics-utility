class DataframeHost:
    def __init__(self):
        print("library.dataframes DataframeHost.__init__")

    def add_csv(self, csv):
        print("library.dataframes DataframeHost.add_csv")

    def add_parquet(self, local):
        print("library.dataframes DataframeHost.add_parquet")

    def to_parquet(self):
        print("library.dataframes DataframeHost.to_parquet")
        return b"fake_parquet_data"

    def to_sql(self):
        print("library.dataframes DataframeHost.to_sql")


class DataframeJobHostSummary:
    def __init__(self):
        print("library.dataframes DataframeJobHostSummary.__init__")

    def add_csv(self, csv):
        print("library.dataframes DataframeJobHostSummary.add_csv")


class DataframeCollectionStatus:
    def __init__(self):
        print("library.dataframes DataframeCollectionStatus.__init__")

    def add_csv(self, csv):
        print("library.dataframes DataframeCollectionStatus.add_csv")


class DataframeHostMetric:
    def __init__(self):
        print("library.dataframes DataframeHostMetric.__init__")

    def add(self, data):
        print("library.dataframes DataframeHostMetric.add")