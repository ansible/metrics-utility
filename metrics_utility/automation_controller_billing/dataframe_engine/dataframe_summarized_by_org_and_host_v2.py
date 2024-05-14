import logging
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

#######################################
# Code for build the dataframe report and pushing it back to S3
######################################

def granularity_cast(date, granularity):
    if granularity == "monthly":
        return date.replace(day=1)
    elif granularity == "yearly":
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

        if granularity == "monthly":
            start_date += relativedelta(months=+1)
        elif granularity == "yearly":
            start_date += relativedelta(years=+1)
        else:
            start_date += datetime.timedelta(days=1)

    dates_arr.append(end_date)

    return dates_arr

class DataframeSummarizedByOrgAndHostv2():
    LOG_PREFIX = "[AAPBillingReport] "

    def __init__(self, extractor, month, extra_params):
        self.logger = logger

        self.extractor = extractor
        self.month = month
        self.extra_params = extra_params

        self.price_per_node = extra_params['price_per_node']

    @staticmethod
    def get_logger():
        return logging.getLogger(__name__)


    def build_dataframe(self):
        # A daily rollup dataframe
        billing_data_monthly_rollup = None

        # Get list of days of the specified month for the monthly report
        beginning_of_the_month = self.month.replace(day=1)
        end_of_the_month = beginning_of_the_month + relativedelta(months=1) - relativedelta(days=1)
        dates_list = list_dates(start_date=beginning_of_the_month,
                                end_date=end_of_the_month,
                                granularity="daily")

        for date in dates_list:
            ###############################
            # Generate the monthly dataset for report
            ###############################

            for data in self.extractor.iter_batches(date=date):
                # If the dataframe is empty, skip additional processing
                billing_data = data['job_host_summary']
                if billing_data.empty:
                    continue

                billing_data['organization_name'] = billing_data.organization_name.fillna("__ORGANIZATION NAME MISSING__")

                if 'ansible_host_variable' in billing_data.columns:
                    # Replace missing ansible_host_variable with host name
                    billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
                    # And use the new ansible_host_variable instead of host_name, since
                    # what is in ansible_host_variable should be the actual host we count
                    billing_data['host_name'] = billing_data['ansible_host_variable']
                ################################
                # Do the aggregation
                ################################
                billing_data_group = billing_data.groupby(
                    self.unique_index_columns(), dropna=False
                ).agg(
                    host_runs=('host_name', 'count'))

                # Tweak types to match the table
                billing_data_group = self.cast_dataframe(billing_data_group, self.cast_types())

                ################################
                # Merge aggregations of multiple batches
                ################################
                if billing_data_monthly_rollup is None:
                    billing_data_monthly_rollup = billing_data_group
                else:
                    # Multipart collection, merge the dataframes and sum counts
                    billing_data_monthly_rollup = pd.merge(
                        billing_data_monthly_rollup.loc[:, ],
                        billing_data_group.loc[:, ],
                        on=self.unique_index_columns(),
                        how='outer')

                    billing_data_monthly_rollup = self.summarize_merged_dataframes(
                        billing_data_monthly_rollup, self.data_columns())

                    # Tweak types to match the table
                    billing_data_monthly_rollup = self.cast_dataframe(
                        billing_data_monthly_rollup, self.cast_types())

        if billing_data_monthly_rollup is None:
            return None

        ccsp_report = billing_data_monthly_rollup.reset_index().groupby(
            'organization_name', dropna=False).agg(
                quantity_consumed=('host_name', 'nunique'))
        ccsp_report['mark_x'] = ''
        ccsp_report['sku_number'] = self.extra_params['report_sku']
        ccsp_report['sku_description'] = self.extra_params['report_sku_description']
        ccsp_report['notes'] = ''

        ccsp_report['unit_price'] = round(self.price_per_node, 2)
        ccsp_report['extended_unit_price'] = round((ccsp_report['quantity_consumed'] * ccsp_report['unit_price']), 2)

        # order the columns right
        ccsp_report = ccsp_report.reset_index()
        ccsp_report = ccsp_report.reindex(columns=['organization_name',
                                                   'mark_x',
                                                   'sku_number',
                                                   'quantity_consumed',
                                                   'sku_description',
                                                   'unit_price',
                                                   'extended_unit_price',
                                                   'notes'])
        return ccsp_report

    def cast_dataframe(self, df, types):
        levels = []
        for index, level in enumerate(df.index.levels):
            casted_level = df.index.levels[index].astype(object)
            levels.append(casted_level)

        df.index = df.index.set_levels(levels)

        return df.astype(types)

    def summarize_merged_dataframes(self, df, columns):
        for col in columns:
            df[col] = df[[f"{col}_x", f"{col}_y"]].sum(axis=1)
            del df[f"{col}_x"]
            del df[f"{col}_y"]
        return df

    @staticmethod
    def unique_index_columns():
        return ['organization_name', 'host_name']

    @staticmethod
    def data_columns():
        return ['host_runs']

    @staticmethod
    def cast_types():
        return {'host_runs': int}
