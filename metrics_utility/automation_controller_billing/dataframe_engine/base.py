import datetime

from dateutil.relativedelta import relativedelta


def granularity_cast(date, granularity):
    if granularity == 'monthly':
        return date.replace(day=1)
    elif granularity == 'yearly':
        return date.replace(month=1, day=1)
    else:
        return date


def list_dates(start_date, end_date, granularity):
    # Given start date and end date, return list of dates in the given granularity
    # e.g. for daily it is a list of days withing the interval, for monthly it is a
    # list of months withing the interval, etc.
    start_date = granularity_cast(start_date, granularity)
    end_date = granularity_cast(end_date, granularity)

    dates_arr = []
    while start_date < end_date:
        dates_arr.append(start_date)

        if granularity == 'monthly':
            start_date += relativedelta(months=+1)
        elif granularity == 'yearly':
            start_date += relativedelta(years=+1)
        else:
            start_date += datetime.timedelta(days=1)

    dates_arr.append(end_date)

    return dates_arr


class Base:
    def __init__(self, extractor, month, extra_params, klass):
        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params
        self.klass = klass

    def build_dataframe(self):
        o = self.klass()
        o.from_tarballs(self.iter_batches(o.TARBALL_NAMES))
        if o.rollup is not None:
            return o.rollup
        return o.empty()

    def dedup(self, dataframe, hostname_mapping=None, scope_dataframe=None):
        return self.klass().dedup(dataframe, hostname_mapping=hostname_mapping, scope_dataframe=scope_dataframe, deduplicator=self.extra_params.get('deduplicator'))

    def iter_batches(self, names):
        collections = []
        optional = []
        datas = map(lambda x: x.replace('.csv', '').replace('.json', ''), names)
        names = [*names]

        if 'config.json' in names:
            optional.append('config')
            names.remove('config.json')
        if 'data_collection_status.csv' in names:
            optional.append('data_collection_status')
            names.remove('data_collection_status.csv')

        collections = list(map(lambda x: x.replace('.csv', ''), names))
        if len(collections) == 0:
            collections = None

        for date in self.dates():
            for data in self.extractor.iter_batches(date=date, collections=collections, optional=optional):
                tup = tuple()
                nonempty = 0

                for name in datas:
                    batch = data[name]
                    tup = (*tup, batch)

                    if name != 'config' and not batch.empty:
                        nonempty += 1

                if nonempty < 1:
                    continue

                if len(tup) == 1:
                    tup = tup[0]

                yield tup

    def dates(self):
        if self.extra_params.get('since_date') is not None:
            beginning_of_the_month = self.extra_params.get('since_date')
            end_of_the_month = self.extra_params.get('until_date')
        else:
            beginning_of_the_month = self.month.replace(day=1)
            end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)

        dates_list = list_dates(start_date=beginning_of_the_month, end_date=end_of_the_month, granularity='daily')
        return dates_list
