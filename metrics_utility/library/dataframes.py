from .debug import log


class BaseDataframe:
    def __init__(self):
        log(f'library.dataframes {self.__class__.__name__}.__init__')

    def add_csv(self, csv):
        log(f'library.dataframes {self.__class__.__name__}.add_csv')
        self.regroup(csv, format='csv')

    def add_parquet(self, local):
        log(f'library.dataframes {self.__class__.__name__}.add_parquet')
        self.regroup(local, format='parquet')

    def add(self, data):
        log(f'library.dataframes {self.__class__.__name__}.add')
        self.regroup(data, format='data')

    def to_parquet(self):
        log(f'library.dataframes {self.__class__.__name__}.to_parquet')
        return b'fake_parquet_data'

    def to_sql(self):
        log(f'library.dataframes {self.__class__.__name__}.to_sql')

    def regroup(self, data, format):
        log(f'library.dataframes {self.__class__.__name__}.regroup')


class DataframeHost(BaseDataframe):
    def regroup(self, data, format):
        log('library.dataframes DataframeHost.regroup')


class DataframeJobHostSummary(BaseDataframe):
    def regroup(self, data, format):
        log('library.dataframes DataframeJobHostSummary.regroup')


class DataframeCollectionStatus(BaseDataframe):
    def regroup(self, data, format):
        log('library.dataframes DataframeCollectionStatus.regroup')


class DataframeHostMetric(BaseDataframe):
    def regroup(self, data, format):
        log('library.dataframes DataframeHostMetric.regroup')
