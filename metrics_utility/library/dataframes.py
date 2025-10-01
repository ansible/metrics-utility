class BaseDataframe:
    def __init__(self):
        print(f'library.dataframes {self.__class__.__name__}.__init__')

    def add_csv(self, csv):
        print(f'library.dataframes {self.__class__.__name__}.add_csv')
        self.regroup(csv, format='csv')

    def add_parquet(self, local):
        print(f'library.dataframes {self.__class__.__name__}.add_parquet')
        self.regroup(local, format='parquet')

    def add(self, data):
        print(f'library.dataframes {self.__class__.__name__}.add')
        self.regroup(data, format='data')

    def to_parquet(self):
        print(f'library.dataframes {self.__class__.__name__}.to_parquet')
        return b'fake_parquet_data'

    def to_sql(self):
        print(f'library.dataframes {self.__class__.__name__}.to_sql')

    def regroup(self, data, format):
        print(f'library.dataframes {self.__class__.__name__}.regroup')


class DataframeHost(BaseDataframe):
    def regroup(self, data, format):
        print('library.dataframes DataframeHost.regroup')


class DataframeJobHostSummary(BaseDataframe):
    def regroup(self, data, format):
        print('library.dataframes DataframeJobHostSummary.regroup')


class DataframeCollectionStatus(BaseDataframe):
    def regroup(self, data, format):
        print('library.dataframes DataframeCollectionStatus.regroup')


class DataframeHostMetric(BaseDataframe):
    def regroup(self, data, format):
        print('library.dataframes DataframeHostMetric.regroup')
